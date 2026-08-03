const std = @import("std");
const c = @import("c.zig").c;

pub const PgReplicationSlot = struct {
    conn_handle: *c.PGconn,
    last_processed_lsn: u64 = 0,

    pub fn init(conn_handle: *c.PGconn) PgReplicationSlot {
        return PgReplicationSlot{
            .conn_handle = conn_handle,
            .last_processed_lsn = 0,
        };
    }

    pub fn create(self: *PgReplicationSlot, slot_name: []const u8) !void {
        var buf: [256]u8 = undefined;
        const query = try std.fmt.bufPrintZ(
            &buf,
            "CREATE_REPLICATION_SLOT {s} LOGICAL pgoutput;",
            .{slot_name},
        );

        const res = c.PQexec(self.conn_handle, query);
        defer c.PQclear(res);

        if (c.PQresultStatus(res) != c.PGRES_TUPLES_OK) {
            const sqlstate = std.mem.span(c.PQresultErrorField(res, c.PG_DIAG_SQLSTATE));

            if (!std.mem.eql(u8, sqlstate, "42710")) {
                std.log.err(
                    \\ Creation slot command failed ...
                    \\ Error: {s}
                ,
                    .{std.mem.span(c.PQresultErrorMessage(res))},
                );
                return error.PostgresqlCreationSlotError;
            } else {
                std.log.warn("Replication slot already exists", .{});
            }
        }
    }

    pub fn start(
        self: *PgReplicationSlot,
        slot_name: []const u8,
        publication_name: []const u8,
    ) !void {
        var buf: [256]u8 = undefined;
        const query = try std.fmt.bufPrintZ(
            &buf,
            "START_REPLICATION SLOT {s} LOGICAL 0/0 (proto_version '1', publication_names '{s}');",
            .{ slot_name, publication_name },
        );

        const res = c.PQexec(self.conn_handle, query);
        defer c.PQclear(res);

        if (c.PQresultStatus(res) != c.PGRES_COPY_BOTH) {
            std.log.err(
                \\ START_REPLICATION failed ...
                \\ Error: {s}
            ,
                .{std.mem.span(c.PQresultErrorMessage(res))},
            );
            return error.PostgresqlStartReplicationFailed;
        }
    }

    fn poll(
        self: *PgReplicationSlot,
        io: std.Io,
        comptime Handler: type,
        handler: *Handler,
    ) !void {
        if (c.PQconsumeInput(self.conn_handle) == 0) {
            std.log.err(
                "Failed to consume input from server: {s}",
                .{std.mem.span(c.PQerrorMessage(self.conn_handle))},
            );
            return error.PostgresqlConnectionError;
        }

        var c_buf: [*c]u8 = null;
        const bytes_read = c.PQgetCopyData(
            self.conn_handle,
            &c_buf,
            1,
        );

        if (bytes_read > 0) {
            defer c.PQfreemem(c_buf);
            const data = c_buf[0..@intCast(bytes_read)];
            if (data.len == 0) return;

            switch (data[0]) {
                'w' => { // WalData
                    if (data.len >= 25) {
                        const lsn_end: u64 = std.mem.readInt(
                            u64,
                            data[9..17],
                            .big,
                        );
                        self.last_processed_lsn = lsn_end;
                        try handler.handlePayload(data[25..]);
                    }
                },
                'k' => { // Keepalive
                    if (data.len >= 18) {
                        const server_lsn = std.mem.readInt(
                            u64,
                            data[1..9],
                            .big,
                        );
                        const reply_requested = data[17] != 0;

                        if (self.last_processed_lsn == 0) {
                            self.last_processed_lsn = server_lsn;
                        }

                        if (reply_requested) {
                            try self.sendStandbyStatusUpdate(io);
                        }
                    }
                },
                else => |msg_type| {
                    std.log.warn(
                        "Other message type in COPY stream : {c}",
                        .{msg_type},
                    );
                },
            }
        } else if (bytes_read == 0) {
            try io.sleep(
                .fromMilliseconds(10),
                .real,
            );
        } else if (bytes_read == -1) {
            std.log.info(
                \\ End of COPY stream received from PostgreSQL...
                \\ {s}
            ,
                .{std.mem.span(c.PQerrorMessage(self.conn_handle))},
            );
            return error.CopyStreamEnded;
        } else {
            std.log.warn(
                "Error of COPY stream: {s} ...",
                .{std.mem.span(c.PQerrorMessage(self.conn_handle))},
            );
            return error.CopyStreamFailed;
        }
    }

    pub fn pollLoop(
        self: *PgReplicationSlot,
        io: std.Io,
        running: *std.atomic.Value(bool),
        comptime Handler: type,
        handler: *Handler,
    ) !void {
        while (running.load(.seq_cst)) {
            self.poll(io, Handler, handler) catch |err| switch (err) {
                error.CopyStreamEnded => {
                    std.log.info("Stopping poll loop...", .{});
                    break;
                },
                else => |e| return e,
            };
        }
    }

    pub fn sendStandbyStatusUpdate(
        self: *PgReplicationSlot,
        io: std.Io,
    ) !void {
        var reply_buf: [34]u8 = undefined;

        reply_buf[0] = 'r'; // Message type Stand by Status Update
        std.mem.writeInt(
            u64,
            reply_buf[1..9],
            self.last_processed_lsn,
            .big,
        );
        std.mem.writeInt(
            u64,
            reply_buf[9..17],
            self.last_processed_lsn,
            .big,
        );
        std.mem.writeInt(
            u64,
            reply_buf[17..25],
            self.last_processed_lsn,
            .big,
        );

        const pg_epoch_offset_us: i64 = 946_684_800 * std.time.us_per_s;
        const now_us = std.Io.Clock.real.now(io).toMicroseconds();
        const interval = now_us - pg_epoch_offset_us;
        const pg_ts: u64 = if (interval > 0) @intCast(interval) else 0;
        std.mem.writeInt(
            u64,
            reply_buf[25..33],
            pg_ts,
            .big,
        );

        reply_buf[33] = 0;

        const res = c.PQputCopyData(
            self.conn_handle,
            &reply_buf,
            reply_buf.len,
        );
        if (res != 1) {
            std.log.err(
                \\ ACK sending error ...
                \\ Error: {s}
            ,
                .{std.mem.span(c.PQerrorMessage(self.conn_handle))},
            );
            return error.PostgresqlSendingAckFailed;
        }

        _ = c.PQflush(self.conn_handle);
        std.log.info(
            "[ACK sent] LSN acknowledged : {X}",
            .{self.last_processed_lsn},
        );
    }
};
