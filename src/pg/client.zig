const std = @import("std");
const posix = std.posix;
const log = std.log;

const span = std.mem.span;

const c = @import("c.zig").c;
const Ring = @import("../iopoll/ring.zig").Ring;

pub const PgClient = struct {
    conn: *c.PGconn,
    healthy: bool = true,
    generation: u64 = 0,

    pub fn init(
        conn_info: [:0]const u8,
        ring: *Ring,
    ) !PgClient {
        if (conn_info.len == 0) {
            return error.EmptyConnectionInfo;
        }

        const conn = c.PQconnectStart(conn_info.ptr) orelse {
            return error.ConnectionMemoryAllocationFailed;
        };
        errdefer c.PQfinish(conn);

        if (c.PQstatus(conn) == c.CONNECTION_BAD) {
            log.err(
                "[PostgreSQL] initial connection status is invalid: {s}",
                .{span(c.PQerrorMessage(conn))},
            );

            return error.ConnectionFailed;
        }

        if (c.PQsetnonblocking(conn, 1) != 0) {
            log.err(
                "[PostgreSQL] set non-blocking mode failed: {s}",
                .{span(c.PQerrorMessage(conn))},
            );

            return error.SetNonBlockingConnectionFailed;
        }

        try pollConnection(
            conn,
            ring,
            .connect,
        );

        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            log.err(
                "[PostgreSQL] connection completed with invalid status: {s}",
                .{span(c.PQerrorMessage(conn))},
            );

            return error.ConnectionFailed;
        }

        return .{
            .conn = conn,
            .healthy = true,
            .generation = 1,
        };
    }

    pub fn deinit(self: *PgClient) void {
        c.PQfinish(self.conn);
    }

    pub fn socket(self: *PgClient) !posix.fd_t {
        const fd = c.PQsocket(self.conn);
        if (fd < 0) {
            return error.InvalidPostgresSocket;
        }

        return @intCast(fd);
    }

    pub fn flush(
        self: *PgClient,
        ring: *Ring,
    ) !void {
        while (true) {
            const result = c.PQflush(self.conn);

            switch (result) {
                0 => return,
                1 => {
                    const fd = try self.socket();
                    try ring.waitWritable(fd);
                },
                else => {
                    self.healthy = false;

                    log.err(
                        "[PostgreSQL] flushing output failed: {s}",
                        .{span(c.PQerrorMessage(self.conn))},
                    );

                    return error.PostgresFlushingFailed;
                },
            }
        }
    }

    pub fn consume(
        self: *PgClient,
        ring: *Ring,
    ) !void {
        const fd = try self.socket();
        try ring.waitReadable(fd);

        if (c.PQconsumeInput(self.conn) != 1) {
            log.err(
                "[PostgreSQL] consuming input failed: {s}",
                .{span(c.PQerrorMessage(self.conn))},
            );

            return error.ConsumeInputFailed;
        }
    }

    pub fn reconnect(
        self: *PgClient,
        ring: *Ring,
    ) !void {
        self.healthy = false;
        self.generation +%= 1;

        if (c.PQresetPoll(self.conn) != 1) {
            log.err(
                "[PostgreSQL] reset asynchronous connection failed: {s}",
                .{span(c.PQerrorMessage(self.conn))},
            );

            return error.ConnectionResetFailed;
        }

        try pollConnection(
            self.conn,
            ring,
            .reset,
        );

        if (c.PQstatus(self.conn) != c.CONNECTION_OK) {
            log.err(
                "[PostgreSQL] connection reset completed with invalid status: {s}",
                .{span(c.PQerrorMessage(self.conn))},
            );

            return error.ConnectionResetFailed;
        }

        if (c.PQsetnonblocking(self.conn, 1) != 0) {
            log.err(
                "[PostgreSQL] restoring non-blocking mode failed: {s}",
                .{span(c.PQerrorMessage(self.conn))},
            );

            return error.RestoringNonBlockingConnectionFailed;
        }

        self.healthy = true;
    }

    pub fn markBroken(self: *PgClient) void {
        self.healthy = false;
    }

    pub const PollMode = enum {
        connect,
        reset,
    };

    fn pollConnection(
        conn: *c.PGconn,
        ring: *Ring,
        mode: PollMode,
    ) !void {
        while (true) {
            const poll_status = switch (mode) {
                .connect => c.PQconnectPoll(conn),
                .reset => c.PQresetPoll(conn),
            };

            switch (poll_status) {
                c.PGRES_POLLING_OK => {
                    return;
                },
                c.PGRES_POLLING_READING => {
                    const raw_fd = c.PQsocket(conn);
                    if (raw_fd < 0) {
                        return error.InvalidPostgresSocket;
                    }

                    try ring.waitReadable(raw_fd);
                },
                c.PGRES_POLLING_WRITING => {
                    const raw_fd = c.PQsocket(conn);
                    if (raw_fd < 0) {
                        return error.InvalidPostgresSocket;
                    }

                    try ring.waitWritable(raw_fd);
                },
                c.PGRES_POLLING_ACTIVE => {
                    continue;
                },
                c.PGRES_POLLING_FAILED => {
                    log.err(
                        "[PostgreSQL] asynchronous connection failed: {s}",
                        .{span(c.PQerrorMessage(conn))},
                    );

                    return switch (mode) {
                        .connect => error.ConnectionFailed,
                        .reset => error.ConnectionResetFailed,
                    };
                },
                else => {
                    return error.UnexpectedPollingStatus;
                },
            }
        }
    }
};
