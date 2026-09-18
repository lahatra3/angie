const std = @import("std");
const log = std.log;

const span = std.mem.span;

const c = @import("c.zig").c;
const PgClient = @import("client.zig").PgClient;
const Ring = @import("../iopoll/ring.zig").Ring;

pub const PgCopy = struct {
    client: *PgClient,
    started: bool = false,
    finished: bool = false,

    pub const Chunk = struct {
        ptr: [*c]u8,
        len: usize,

        pub fn slice(self: Chunk) []const u8 {
            return self.ptr[0..self.len];
        }

        pub fn deinit(self: Chunk) void {
            if (self.ptr != null) {
                c.PQfreemem(self.ptr);
            }
        }
    };

    pub fn init(client: *PgClient) PgCopy {
        return .{
            .client = client,
        };
    }

    pub fn start(
        self: *PgCopy,
        ring: *Ring,
        query: [:0]const u8,
    ) !void {
        if (self.started) {
            return error.QueryAlreadyStarted;
        }

        if (query.len == 0) {
            return error.EmptyQueryStart;
        }

        if (!self.client.healthy) {
            return error.UnhealthyPostgresConnection;
        }

        if (c.PQsendQuery(self.client.conn, query.ptr) != 1) {
            self.client.markBroken();

            log.err(
                "[PostgreSQL] send query failed: {s}",
                .{span(c.PQerrorMessage(self.client.conn))},
            );

            return error.SendQueryFailed;
        }

        try self.client.flush(ring);

        while (c.PQisBusy(self.client.conn) != 0) {
            try self.client.consume(ring);
        }

        const result = c.PQgetResult(self.client.conn) orelse {
            self.client.markBroken();

            return error.MissingQueryStartResult;
        };
        defer c.PQclear(result);

        const status = c.PQresultStatus(result);

        if (status != c.PGRES_COPY_BOTH) {
            self.client.markBroken();

            log.err(
                "[PostgreSQL] starting query failed, status={d}, error={s}",
                .{ status, span(c.PQresultErrorMessage(result)) },
            );

            return error.QueryResponseFailed;
        }

        self.started = true;
    }

    pub fn read(
        self: *PgCopy,
        ring: *Ring,
    ) !?Chunk {
        if (!self.started) {
            return error.QueryNotStarted;
        }

        if (self.finished) {
            return null;
        }

        while (true) {
            var buffer: [*c]u8 = null;

            const bytes_read = c.PQgetCopyData(
                self.client.conn,
                &buffer,
                1,
            );

            if (bytes_read > 0) {
                return Chunk{
                    .ptr = buffer,
                    .len = @intCast(bytes_read),
                };
            }

            switch (bytes_read) {
                0 => {
                    try self.client.consume(ring);
                },
                -1 => {
                    self.finished = true;
                    return null;
                },
                -2 => {
                    self.client.markBroken();

                    log.err(
                        "[PostgreSQL] copy stream failed: {s}",
                        .{span(c.PQerrorMessage(self.client.conn))},
                    );

                    return error.CopyStreamFailed;
                },
                else => {
                    self.client.markBroken();

                    return error.UnexpectedReadResult;
                },
            }
        }
    }

    pub fn write(
        self: *PgCopy,
        ring: *Ring,
        data: []const u8,
    ) !void {
        if (data.len == 0) {
            return error.EmptyData;
        }

        if (!self.started) {
            return error.QueryNotStarted;
        }

        if (self.finished) {
            return error.CopyStreamFinished;
        }

        if (!self.client.healthy) {
            return error.UnhealthyPostgresConnection;
        }

        if (data.len > std.math.maxInt(c_int)) {
            return error.CopyDataTooLarge;
        }

        while (true) {
            const result = c.PQputCopyData(
                self.client.conn,
                @ptrCast(data.ptr),
                @intCast(data.len),
            );

            switch (result) {
                1 => {
                    try self.client.flush(ring);
                    return;
                },
                0 => {
                    try self.client.flush(ring);
                },
                -1 => {
                    self.client.markBroken();

                    log.err(
                        "[PostgreSQL] write data failed: {s}",
                        .{span(c.PQerrorMessage(self.client.conn))},
                    );

                    return error.WriteDataFailed;
                },
                else => {
                    self.client.markBroken();

                    return error.UnexpectedWriteResult;
                },
            }
        }
    }

    pub fn reset(self: *PgCopy) void {
        self.started = false;
        self.finished = false;
    }

    fn finish(
        self: *PgCopy,
        ring: *Ring,
    ) !void {
        var received_command_ok = false;

        while (true) {
            while (c.PQisBusy(self.client.conn) != 0) {
                try self.client.consume(ring);
            }

            const result = c.PQgetResult(self.client.conn) orelse break;
            defer c.PQclear(result);

            const status = c.PQresultStatus(result);

            switch (status) {
                c.PGRES_COMMAND_OK => {
                    received_command_ok = true;
                },
                c.PGRES_FATAL_ERROR, c.PGRES_BAD_RESPONSE => {
                    self.client.markBroken();

                    log.err(
                        "[PostgreSQL] copy completion failed, status={d}, error={s}",
                        .{ status, span(c.PQresultErrorMessage(result)) },
                    );

                    return error.CopyCompletionFailed;
                },
                else => {
                    self.client.markBroken();

                    log.err(
                        "[PostgreSQL] unexpected copy completion status={d}",
                        .{status},
                    );

                    return error.UnexpectedCopyCompletion;
                },
            }
        }

        if (!received_command_ok) {
            self.client.markBroken();

            return error.MissingCopyCompletionResult;
        }
    }
};
