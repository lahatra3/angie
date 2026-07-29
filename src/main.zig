const std = @import("std");
const PgClient = @import("postgres/client.zig").PgClient;

pub fn main(init: std.process.Init) !void {
    _ = init;

    var pg_client = try PgClient.connect("host=172.17.0.1 port=5432 dbname=ldf user=postgres password=postgres31");
    defer pg_client.deinit();
}
