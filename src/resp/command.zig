const std = @import("std");
const RespData = @import("./encoding.zig").RespData;
const Connection = std.net.Server.Connection;

pub const Command = enum { ping, echo, set, get };

pub fn getCommandEnum(command: []const u8) ?Command {
    const fields = std.meta.fields(Command);

    inline for (fields) |field| {
        if (std.ascii.eqlIgnoreCase(field.name, command)) {
            return @field(Command, field.name);
        }
    }
    return null;
}
