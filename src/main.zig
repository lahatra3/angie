const std = @import("std");

const c = @import("pg/c.zig").c;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const allocator = init.arena.allocator();

    _ = io;
    _ = allocator;
}
