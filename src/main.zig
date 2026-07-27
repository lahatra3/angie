const std = @import("std");
const c = @cImport({
    @cInclude("postgresql/libpq-fe.h");
});

pub fn main(init: std.process.Init) !void {
    _ = init;
}