const std = @import("std");
const c = @cImport({
    @cInclude("postgresql/libpq-fe.h");
});

pub fn main(init: std.process.Init) !void {
    const conn_info = "host=172.17.0.1 port=5433 dbname=ldf user=postgres password=postgres31 replication=database";
    const conn = c.PQconnectdb(conn_info) orelse {
        std.log.err("Connection allocation failed ...", .{});
        return error.PostgresqlAllocationError;
    };
    defer c.PQfinish(conn);

    if (c.PQstatus(conn) != c.CONNECTION_OK) {
        std.log.debug(
            \\ Connection failed ...
            \\ Error: {s}
        ,
            .{std.mem.span(c.PQerrorMessage(conn))},
        );
        return error.PostgresqlConnectionFailed;
    }
    std.log.info("Connection successed ...", .{});

    //  Create slot
    const create_slot_query =
        \\ CREATE_REPLICATION_SLOT cdc_slot
        \\ LOGICAL pgoutput;
    ;
    const create_slot_res = c.PQexec(
        conn,
        create_slot_query,
    );
    defer c.PQclear(create_slot_res);

    if (c.PQresultStatus(create_slot_res) != c.PGRES_TUPLES_OK) {
        const sqlstate = std.mem.span(c.PQresultErrorField(create_slot_res, c.PG_DIAG_SQLSTATE));

        if (!std.mem.eql(u8, sqlstate, "42710")) {
            std.log.err(
                \\ Creation slot command failed ...
                \\ Error: {s}
            ,
                .{std.mem.span(c.PQresultErrorMessage(create_slot_res))},
            );
            return error.PostgresqlCreationSlotError;
        } else {
            std.log.warn("Replication slot already exists", .{});
        }
    }

    // Start replication
    const start_query =
        \\ START_REPLICATION SLOT cdc_slot 
        \\ LOGICAL 0/0 (proto_version '1', publication_names 'cdc_pub');
    ;
    const res = c.PQexec(
        conn,
        start_query,
    );
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

    std.log.info("Listening ...", .{});

    var last_processed_lsn: u64 = 0;

    var buffer: [*c]u8 = null;
    while (true) {
        const bytes_read = c.PQgetCopyData(
            conn,
            &buffer,
            0,
        );

        if (bytes_read > 0) {
            defer c.PQfreemem(buffer);
            const data = buffer[0..@intCast(bytes_read)];

            if (data.len == 0) continue;

            const msg_type = data[0];
            switch (msg_type) {
                'w' => {
                    if (data.len >= 25) {
                        const lsn_end: u64 = std.mem.readInt(
                            u64,
                            data[9..17],
                            .big,
                        );
                        last_processed_lsn = lsn_end;

                        const payload = data[25..];
                        try handleWalPayload(payload);
                    }
                },
                'k' => {
                    if (data.len >= 18) {
                        const server_lsn = std.mem.readInt(
                            u64,
                            data[1..9],
                            .big,
                        );
                        const reply_requested = data[17] != 0;

                        if (last_processed_lsn == 0) {
                            last_processed_lsn = server_lsn;
                        }

                        if (reply_requested) {
                            try sendStandbyStatusUpdate(
                                conn,
                                last_processed_lsn,
                                init.io,
                            );
                        }
                    }
                },
                else => {
                    std.log.warn(
                        "Other message type : {}",
                        .{msg_type},
                    );
                },
            }
        } else if (bytes_read == 0) {
            try init.io.sleep(
                .fromMilliseconds(10),
                .real,
            );
        } else if (bytes_read == -1) {
            std.log.warn(
                \\ Error of COPY stream ...
                \\ {s}
            ,
                .{std.mem.span(c.PQerrorMessage(conn))},
            );
            break;
        } else {
            std.log.warn(
                "End or error of COPY stream ...",
                .{},
            );
            break;
        }
    }
}

fn handleWalPayload(payload: []const u8) !void {
    if (payload.len == 0) return;
    switch (payload[0]) {
        'I' => {
            try handleInsert(payload);
        },
        'B' => {
            std.debug.print("Begin transaction ...\n", .{});
        },
        'C' => {
            std.debug.print("Commit transaction ...\n", .{});
        },
        else => {},
    }
}

fn handleInsert(payload: []const u8) !void {
    if (payload.len < 8) return;

    var cursor: usize = 1;

    const relation_id = std.mem.readInt(
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

    std.log.info(
        \\ [INSERT] Table ID : {d} ({d} colonnes)
    ,
        .{ relation_id, col_count },
    );

    var col_idx: u16 = 0;
    while (col_idx < col_count) : (col_idx += 1) {
        if (cursor >= payload.len) return;

        const col_type = payload[cursor];
        cursor += 1;

        switch (col_type) {
            'n' => {
                std.log.info("   ├─ Col {d} : NULL\n", .{col_idx});
            },
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

                std.log.info(
                    \\   ├─ Col {d} : {s}
                ,
                    .{ col_idx, val },
                );
            },
            else => {},
        }
    }
}

fn sendStandbyStatusUpdate(conn: *c.PGconn, lsn: u64, io: std.Io) !void {
    var reply_buf: [34]u8 = undefined;

    reply_buf[0] = 'r';
    std.mem.writeInt(u64, reply_buf[1..9], lsn, .big);

    std.mem.writeInt(u64, reply_buf[9..17], lsn, .big);

    std.mem.writeInt(u64, reply_buf[17..25], lsn, .big);

    const pg_epoch_offset_ms: i64 = 946_684_800 * std.time.us_per_s;
    const now_ms = std.Io.Clock.real.now(io).toMicroseconds();
    const interval: i64 = now_ms - pg_epoch_offset_ms;
    const pg_ts: u64 = if (interval > 0) @intCast(interval) else 0;
    std.mem.writeInt(u64, reply_buf[25..33], pg_ts, .big);

    reply_buf[33] = 0;

    const res = c.PQputCopyData(
        conn,
        &reply_buf,
        @intCast(reply_buf.len),
    );
    if (res != 1) {
        std.log.err(
            \\ ACK sending error ...
            \\ Error: {s}
        ,
            .{std.mem.span(c.PQerrorMessage(conn))},
        );
        return error.PostgresqlSendingAckFailed;
    }

    _ = c.PQflush(conn);
    std.log.info(
        "[ACK sent] LSN acknowledged : {X}",
        .{lsn},
    );
}
