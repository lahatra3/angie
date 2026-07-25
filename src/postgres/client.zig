const std = @import("std");
const c = @import("c.zig").c;

pub const PgClient = struct {
    conn_handler: *c.PGconn,

    pub fn init(conn_info: [:0]const u8) !PgClient {
        const conn = c.PQconnectdb(conn_info) orelse {
            std.log.err("PGconn allocation failed ...", .{});
            return error.PostgresqlAllocationError;
        };

        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            std.log.err(
                \\ Connection failed ... 
                \\ Error: {s}
            ,
                .{std.mem.span(c.PQerrorMessage(conn))},
            );
            return error.PostgresqlConnectionFailed;
        }
        std.log.info("Connection ready ...", .{});

        return PgClient{ .conn_handler = conn };
    }

    pub fn deinit(self: *PgClient) void {
        std.log.info("Closing connection ...", .{});
        c.PQfinish(self.conn_handler);
    }
};
