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
            std.log.err(
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

    pub fn exec(self: *PgClient, query: [:0]const u8) !void {
        const res = c.PQexec(
            self.conn_handle,
            query,
        );
        defer c.PQclear(res);

        if (c.PQresultStatus(res) != c.PGRES_COMMAND_OK) {
            std.log.err(
                "SQL Execution failed: {s}",
                .{std.mem.span(c.PQerrorMessage(self.conn_handle))},
            );
            return error.PostgresqlExecFailed;
        }
    }
};
