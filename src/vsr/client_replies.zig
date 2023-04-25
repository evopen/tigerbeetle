const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const log = std.log.scoped(.client_replies);

const stdx = @import("../stdx.zig");
const constants = @import("../constants.zig");
const RingBuffer = @import("../ring_buffer.zig").RingBuffer;
const IOPS = @import("../iops.zig").IOPS;
const vsr = @import("../vsr.zig");
const Message = @import("../message_pool.zig").MessagePool.Message;
const MessagePool = @import("../message_pool.zig").MessagePool;
const Slot = @import("superblock_client_sessions.zig").ReplySlot;
const ClientSessions = @import("superblock_client_sessions.zig").ClientSessions;

const client_replies_iops_max =
    constants.client_replies_iops_read_max + constants.client_replies_iops_write_max;

fn slot_offset(slot: Slot) usize {
    return slot.index * constants.message_size_max;
}

// TODO Optimization:
// Don't always immediately start writing a reply. Instead, hold onto it in the hopes that
// the same client will queue another request. If they do (within the same checkpoint),
// then we no longer need to persist the original reply.
pub fn ClientRepliesType(comptime Storage: type) type {
    return struct {
        const ClientReplies = @This();

        const Read = struct {
            client_replies: *ClientReplies,
            completion: Storage.Read,
            callback: fn (
                client_replies: *ClientReplies,
                reply_header: *const vsr.Header,
                reply: ?*Message,
                destination_replica: ?u8,
            ) void,
            slot: Slot,
            message: *Message,
            /// The header of the expected reply.
            header: vsr.Header,
            destination_replica: ?u8,
        };

        const Write = struct {
            client_replies: *ClientReplies,
            completion: Storage.Write,
            slot: Slot,
            message: *Message,
        };

        storage: *Storage,
        message_pool: *MessagePool,
        replica: u8,

        reads: IOPS(Read, constants.client_replies_iops_read_max) = .{},
        writes: IOPS(Write, constants.client_replies_iops_write_max) = .{},

        /// Track which slots have a write currently in progress.
        writing: std.StaticBitSet(constants.clients_max) =
            std.StaticBitSet(constants.clients_max).initEmpty(),
        /// Track which slots hold a corrupt reply, or are otherwise missing the reply
        /// that ClientSessions believes they should hold.
        faulty: std.StaticBitSet(constants.clients_max) =
            std.StaticBitSet(constants.clients_max).initEmpty(),

        /// Guard against multiple concurrent writes to the same slot.
        /// Pointers are into `writes`.
        write_queue: RingBuffer(*Write, constants.client_replies_iops_write_max, .array) = .{},

        ready_next_tick: Storage.NextTick = undefined,
        ready_callback: ?union(enum) {
            next_tick: fn (*ClientReplies) void,
            write: fn (*ClientReplies) void,
        } = null,

        checkpoint_next_tick: Storage.NextTick = undefined,
        checkpoint_callback: ?fn (*ClientReplies) void = null,

        pub fn init(options: struct {
            storage: *Storage,
            message_pool: *MessagePool,
            replica_index: u8,
        }) ClientReplies {
            return .{
                .storage = options.storage,
                .message_pool = options.message_pool,
                .replica = options.replica_index,
            };
        }

        pub fn deinit(client_replies: *ClientReplies) void {
            {
                var it = client_replies.reads.iterate();
                while (it.next()) |read| client_replies.message_pool.unref(read.message);
            }
            {
                var it = client_replies.writes.iterate();
                while (it.next()) |write| client_replies.message_pool.unref(write.message);
            }
            // Don't unref `write_queue`'s Writes — they are a subset of `writes`.
        }

        pub fn read_reply_sync(
            client_replies: *ClientReplies,
            slot: Slot,
            session: *const ClientSessions.Entry,
        ) ?*Message {
            const client = session.header.client;

            if (client_replies.writing.isSet(slot.index)) {
                var writes = client_replies.writes.iterate();
                var write_latest: ?*const Write = null;
                while (writes.next()) |write| {
                    if (write.message.header.client == client) {
                        if (write_latest == null or
                            write_latest.?.message.header.request < write.message.header.request)
                        {
                            write_latest = write;
                        }
                    }
                }
                assert(write_latest.?.message.header.checksum == session.header.checksum);

                return write_latest.?.message;
            }

            return null;
        }

        /// Caller must check read_reply_sync() first.
        /// (They are split up to avoid complicated NextTick bounds.)
        pub fn read_reply(
            client_replies: *ClientReplies,
            slot: Slot,
            session: *const ClientSessions.Entry,
            callback: fn (*ClientReplies, *const vsr.Header, ?*Message, ?u8) void,
            destination_replica: ?u8,
        ) error{Busy}!void {
            assert(client_replies.read_reply_sync(slot, session) == null);

            const read = client_replies.reads.acquire() orelse {
                log.debug("{}: read_reply: busy (client={} reply={})", .{
                    client_replies.replica,
                    session.header.client,
                    session.header.checksum,
                });

                return error.Busy;
            };

            log.debug("{}: read_reply: start (client={} reply={})", .{
                client_replies.replica,
                session.header.client,
                session.header.checksum,
            });

            const message = client_replies.message_pool.get_message();
            defer client_replies.message_pool.unref(message);

            read.* = .{
                .client_replies = client_replies,
                .completion = undefined,
                .slot = slot,
                .message = message.ref(),
                .callback = callback,
                .header = session.header,
                .destination_replica = destination_replica,
            };

            client_replies.storage.read_sectors(
                read_reply_callback,
                &read.completion,
                message.buffer[0..vsr.sector_ceil(session.header.size)],
                .client_replies,
                slot_offset(slot),
            );
        }

        fn read_reply_callback(completion: *Storage.Read) void {
            const read = @fieldParentPtr(ClientReplies.Read, "completion", completion);
            const client_replies = read.client_replies;
            const header = read.header;
            const message = read.message;
            const callback = read.callback;
            const destination_replica = read.destination_replica;

            client_replies.reads.release(read);

            defer {
                client_replies.message_pool.unref(message);
                client_replies.write_reply_next();
            }

            if (!message.header.valid_checksum() or
                !message.header.valid_checksum_body(message.body()))
            {
                log.warn("{}: read_reply: corrupt reply (client={} reply={})", .{
                    client_replies.replica,
                    header.client,
                    header.checksum,
                });

                callback(client_replies, &header, null, destination_replica);
                return;
            }

            // Possible causes:
            // - The read targets an older reply.
            // - The read targets a newer reply (that we haven't seen/written yet).
            // - The read targets a reply that we wrote, but was misdirected.
            if (message.header.checksum != header.checksum) {
                log.warn("{}: read_reply: unexpected header (client={} reply={} found={})", .{
                    client_replies.replica,
                    header.client,
                    header.checksum,
                    message.header.checksum,
                });

                callback(client_replies, &header, null, destination_replica);
                return;
            }

            assert(message.header.command == .reply);
            assert(message.header.cluster == header.cluster);

            log.debug("{}: read_reply: done (client={} reply={})", .{
                client_replies.replica,
                header.client,
                header.checksum,
            });

            callback(client_replies, &header, message, destination_replica);
        }

        /// Call `callback` when ClientReplies is able to start another write_reply().
        pub fn ready(
            client_replies: *ClientReplies,
            callback: fn (client_replies: *ClientReplies) void,
        ) void {
            assert(client_replies.ready_callback == null);

            if (client_replies.writes.available() > 0) {
                client_replies.ready_callback = .{ .next_tick = callback };
                client_replies.storage.on_next_tick(
                    ready_next_tick_callback,
                    &client_replies.ready_next_tick,
                );
            } else {
                // ready_callback will be called the next time a write completes.
                client_replies.ready_callback = .{ .write = callback };
            }
        }

        fn ready_next_tick_callback(next_tick: *Storage.NextTick) void {
            const client_replies = @fieldParentPtr(ClientReplies, "ready_next_tick", next_tick);
            assert(client_replies.writes.available() > 0);

            const callback = client_replies.ready_callback.?.next_tick;
            client_replies.ready_callback = null;
            callback(client_replies);
        }

        pub fn remove_reply(client_replies: *ClientReplies, slot: Slot) void {
            client_replies.faulty.unset(slot.index);
        }

        /// The caller is responsible for ensuring that the ClientReplies is able to write
        /// by calling `write_reply()` after `ready()` finishes.
        pub fn write_reply(client_replies: *ClientReplies, slot: Slot, message: *Message) void {
            assert(client_replies.ready_callback == null);
            assert(client_replies.writes.available() > 0);
            assert(message.header.command == .reply);
            // There is never any need to write a body-less message, since the header is
            // stored safely in the client sessions superblock trailer.
            assert(message.header.size != @sizeOf(vsr.Header));

            const write = client_replies.writes.acquire().?;
            write.* = .{
                .client_replies = client_replies,
                .completion = undefined,
                .message = message.ref(),
                .slot = slot,
            };

            // If there is already a write to the same slot queued (but not started), replace it.
            var write_queue = client_replies.write_queue.iterator_mutable();
            while (write_queue.next_ptr()) |queued| {
                if (queued.*.slot.index == slot.index) {
                    client_replies.message_pool.unref(queued.*.message);
                    client_replies.writes.release(queued.*);

                    queued.* = write;
                    break;
                }
            } else {
                client_replies.write_queue.push_assume_capacity(write);
                client_replies.write_reply_next();
            }
        }

        fn write_reply_next(client_replies: *ClientReplies) void {
            while (client_replies.write_queue.head()) |write| {
                if (client_replies.writing.isSet(write.slot.index)) return;

                var reads = client_replies.reads.iterate();
                while (reads.next()) |read| {
                    if (read.slot.index == write.slot.index) return;
                }

                const message = write.message;
                _ = client_replies.write_queue.pop();

                // Zero sector padding to ensure deterministic storage.
                const size = message.header.size;
                const size_ceil = vsr.sector_ceil(size);
                std.mem.set(u8, message.buffer[size..size_ceil], 0);

                client_replies.writing.set(write.slot.index);
                client_replies.storage.write_sectors(
                    write_reply_callback,
                    &write.completion,
                    message.buffer[0..size_ceil],
                    .client_replies,
                    slot_offset(write.slot),
                );
            }
        }

        fn write_reply_callback(completion: *Storage.Write) void {
            const write = @fieldParentPtr(ClientReplies.Write, "completion", completion);
            const client_replies = write.client_replies;
            const message = write.message;

            log.debug("{}: write_reply: wrote (client={} request={})", .{
                client_replies.replica,
                message.header.client,
                message.header.request,
            });

            // Release the write *before* invoking the callback, so that if the callback
            // checks .writes.available() we doesn't appear busy when we're not.
            client_replies.writing.unset(write.slot.index);
            client_replies.faulty.unset(write.slot.index);
            client_replies.writes.release(write);

            client_replies.message_pool.unref(message);
            client_replies.write_reply_next();

            if (client_replies.ready_callback) |ready_callback| {
                switch (ready_callback) {
                    .write => |callback| {
                        client_replies.ready_callback = null;
                        callback(client_replies);
                    },
                    .next_tick => {},
                }
            }

            if (client_replies.checkpoint_callback != null and
                client_replies.writes.executing() == 0)
            {
                client_replies.checkpoint_done();
            }
        }

        pub fn checkpoint(client_replies: *ClientReplies, callback: fn (*ClientReplies) void) void {
            assert(client_replies.checkpoint_callback == null);
            client_replies.checkpoint_callback = callback;

            if (client_replies.writes.executing() == 0) {
                assert(client_replies.writing.count() == 0);
                assert(client_replies.write_queue.count == 0);
                client_replies.storage.on_next_tick(
                    checkpoint_next_tick_callback,
                    &client_replies.checkpoint_next_tick,
                );
            }
        }

        fn checkpoint_next_tick_callback(next_tick: *Storage.NextTick) void {
            const client_replies = @fieldParentPtr(ClientReplies, "checkpoint_next_tick", next_tick);
            client_replies.checkpoint_done();
        }

        fn checkpoint_done(client_replies: *ClientReplies) void {
            assert(client_replies.writes.executing() == 0);
            assert(client_replies.writing.count() == 0);
            assert(client_replies.write_queue.count == 0);

            const callback = client_replies.checkpoint_callback.?;
            client_replies.checkpoint_callback = null;

            callback(client_replies);
        }
    };
}