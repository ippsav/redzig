const std = @import("std");
const RespData = @import("./encoding.zig").RespData;
const Connection = std.net.Server.Connection;

pub const Command = enum { ping, echo, set, get };
