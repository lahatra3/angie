const std = @import("std");
const c = @import("c.zig").c;

pub const PgClient = struct {
    conn_handle: *c.PGconn,

    pub fn connect(conn_info: [:0]const u8) !PgClient {
        const conn = c.PQconnectdb(conn_info) orelse {
            std.log.err("Connection allocation failed ...", .{});
            return error.PostgresqlAllocationError;
        };

        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            std.log.debug(
                \\ Connection failed ...
                \\ Error: {s}
            ,
                .{std.mem.span(c.PQerrorMessage(conn))},
            );
            c.PQfinish(conn);
            return error.PostgresqlConnectionFailed;
        }
        std.log.info("Connection successed ...", .{});

        return PgClient{ .conn_handle = conn };
    }

    pub fn deinit(self: *PgClient) void {
        std.log.info("Closing connection ...", .{});
        c.PQfinish(self.conn_handle);
    }
};

pub const PgReplicationSlot = struct {
    conn_handle: *c.PGconn,

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

    pub fn pollReplicationSlot(
        self: *PgReplicationSlot,
        io: std.Io,
        running: *std.atomic.Value(bool),
        comptime Handler: type,
        handler: *Handler,
    ) !void {
        var c_buf: [*c]u8 = null;
        while (running.load(.seq_cst)) {
            const bytes_read = c.PQgetCopyData(
                self.conn_handle,
                &c_buf,
                0,
            );

            if (bytes_read > 0) {
                defer c.PQfreemem(c_buf);
                try handler.handleWalPayload(c_buf[0..@intCast(bytes_read)]);
            } else if (bytes_read == 0) {
                try io.sleep(
                    .fromMilliseconds(10),
                    .real,
                );
            } else if (bytes_read == -1) {
                std.log.err(
                    \\ Error of COPY stream ...
                    \\ {s}
                ,
                    .{std.mem.span(c.PQerrorMessage(self.conn_handle))},
                );
                break;
            } else {
                std.log.warn(
                    "End of COPY stream ...",
                    .{},
                );
                break;
            }
        }
    }

    pub fn sendStandbyStatusUpdate(
        self: *PgReplicationSlot,
        lsn: u64,
        io: std.Io,
    ) !void {
        var reply_buf: [34]u8 = undefined;

        reply_buf[0] = 'r'; // Message type Stand by Status Update
        std.mem.writeInt(u64, reply_buf[1..9], lsn, .big);
        std.mem.writeInt(u64, reply_buf[9..17], lsn, .big);
        std.mem.writeInt(u64, reply_buf[17..25], lsn, .big);

        const pg_epoch_offset_us: i64 = 946_684_800 * std.time.us_per_s;
        const now_us = std.Io.Clock.real.now(io).toMicroseconds();
        const interval = now_us - pg_epoch_offset_us;
        const pg_ts: u64 = if (interval > 0) @intCast(interval) else 0;
        std.mem.writeInt(u64, reply_buf[25..33], pg_ts, .big);

        reply_buf[33] = 0;

        const res = c.PQputCopyData(self.conn_handle, &reply_buf, reply_buf.len);
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
            .{lsn},
        );
    }
};
