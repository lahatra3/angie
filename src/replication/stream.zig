const std = @import("std");
const log = std.log;
const Io = std.Io;

const PgCopy = @import("../pg/copy.zig").PgCopy;
const Ring = @import("../iopoll/ring.zig").Ring;

pub const Stream = struct {
    io: Io,
    copy: *PgCopy,

    received_lsn: u64 = 0,
    committed_lsn: u64 = 0,
    durable_lsn: u64 = 0,
    server_wal_end: u64 = 0,

    pub fn init(
        io: Io,
        copy: *PgCopy,
    ) Stream {
        return .{
            .io = io,
            .copy = copy,
        };
    }

    pub fn read(
        self: *Stream,
        ring: *Ring,
    ) !void {
        while (try self.copy.read(ring)) |chunk| {
            defer chunk.deinit();

            const bytes = chunk.slice();

            if (bytes.len == 0) {
                return error.EmptyReplicationMessage;
            }

            switch (bytes[0]) {
                'w' => try self.handleXLogData(bytes),
                'k' => try self.handlePrimaryKeepalive(
                    ring,
                    bytes,
                ),
                else => {
                    log.warn(
                        "[stream] unhandle replication message: 0x{X:0>2}",
                        .{bytes[0]},
                    );
                },
            }
        }
    }

    fn handleXLogData(
        self: *Stream,
        bytes: []const u8,
    ) !void {
        const header_len: usize = 25;

        if (bytes.len < header_len) {
            return error.TruncatedXLogData;
        }

        const wal_start = std.mem.readInt(
            u64,
            bytes[1..9],
            .big,
        );

        const wal_end = std.mem.readInt(
            u64,
            bytes[9..17],
            .big,
        );

        const server_time = std.mem.readInt(
            i64,
            bytes[17..25],
            .big,
        );

        self.received_lsn = wal_end;
        self.server_wal_end = wal_end;

        const payload = bytes[header_len..];

        try self.decodeWalPayload(
            wal_start,
            server_time,
            payload,
        );
    }

    fn handlePrimaryKeepalive(
        self: *Stream,
        ring: *Ring,
        bytes: []const u8,
    ) !void {
        const message_len: usize = 18;

        if (bytes.len != message_len) {
            return error.InvalidPrimaryKeepaliveLength;
        }

        const server_wal_end = std.mem.readInt(
            u64,
            bytes[1..9],
            .big,
        );

        const reply_request = (bytes[17] != 0);

        self.server_wal_end = server_wal_end;

        if (reply_request) {
            try self.sendStandbyStatusUpdate(
                ring,
                self.durable_lsn,
            );
        }
    }

    fn sendStandbyStatusUpdate(
        self: *Stream,
        ring: *Ring,
        durable_lsn: u64,
    ) !void {
        var reply_buffer: [34]u8 = undefined;

        reply_buffer[0] = 'r';

        std.mem.writeInt(
            u64,
            reply_buffer[1..9],
            durable_lsn,
            .big,
        );

        std.mem.writeInt(
            u64,
            reply_buffer[9..17],
            durable_lsn,
            .big,
        );

        std.mem.writeInt(
            u64,
            reply_buffer[17..25],
            durable_lsn,
            .big,
        );

        const postgres_epoch_unix_us = 946_684_800_000_000;
        const unix_us = Io.Clock.real
            .now(self.io)
            .toMicroseconds();
        const postgres_us = unix_us - postgres_epoch_unix_us;
        std.mem.writeInt(
            u64,
            reply_buffer[25..33],
            postgres_us,
            .big,
        );

        reply_buffer[33] = 0;

        try self.copy.write(
            ring,
            reply_buffer[0..],
        );

        log.debug(
            "standby status update sent: durable_lsn={X}",
            .{durable_lsn},
        );
    }

    fn decodeWalPayload(
        self: *Stream,
        payload: []const u8,
    ) !void {
        switch (payload[0]) {
            'I' => try decodeInsert(payload),
            'B' => {},
            'C' => {},
            else => {},
        }
    }

    fn decodeInsert(payload: []const u8) !void {
        if (payload.len < 8) return;

        const payload_len = payload.len;
        var cursor: usize = 1;

        const relation_id: u32 = std.mem.readInt(
            u32,
            payload[cursor..][0..4],
            .big,
        );

        cursor += 4;

        if (payload[cursor] != 'N') return;

        cursor += 1;

        const col_count = std.mem.readInt(
            u16,
            payload[cursor..][0..2],
            .big,
        );

        cursor += 2;

        log.debug(
            "[INSERT] Table ID: {d} ({d} colonnes)",
            .{ relation_id, col_count },
        );

        var col_idx: u16 = 0;

        while (col_idx < col_count) : (col_idx += 1) {
            if (cursor >= payload_len) return;

            const col_type = payload[cursor];
            cursor += 1;

            switch (col_type) {
                'n' => {},
                't' => {
                    if (cursor + 4 > payload.len) return;

                    const text_len = std.mem.readInt(
                        u32,
                        payload[cursor..][0..4],
                        .big,
                    );

                    cursor += 4;

                    if (cursor + text_len > payload.len) return;

                    const val = payload[cursor .. cursor + text_len];
                    cursor += text_len;

                    log.debug(
                        "col {d} : {s}",
                        .{ col_idx, val },
                    );
                },
                else => {},
            }
        }
    }

    fn decodeCommit(
        self: *Stream,
        payload: []const u8,
    ) !void {
        const commit_message_len: usize = 26;

        if (payload.len != commit_message_len) {
            return error.InvalidCommitMessageLength;
        }

        const flags = payload[1];

        const commit_lsn = std.mem.readInt(
            u64,
            payload[2..10],
            .big,
        );

        const transaction_end_lsn = std.mem.readInt(
            u64,
            payload[10..18],
            .big,
        );

        const commit_time = std.mem.readInt(
            i64,
            payload[18..26],
            .big,
        );

        if (flags != 0) {
            return error.UnsupportedCommitFlags;
        }

        self.committed_lsn = transaction_end_lsn;
        
        // lahatra3, don't forget to see this 
        // after implementing sink
        self.durable_lsn = transaction_end_lsn;

        log.debug(
            "[COMMIT] commit_lsn={X}, end_lsn={X}, time={d}",
            .{ commit_lsn, transaction_end_lsn, commit_time },
        );
    }
};
