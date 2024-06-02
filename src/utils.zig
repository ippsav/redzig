const std = @import("std");

pub fn getEnumIgnoreCase(comptime T: type, str: []const u8) ?T {
    const fields = std.meta.fields(T);

    inline for (fields) |field| {
        if (std.ascii.eqlIgnoreCase(field.name, str)) {
            return @field(T, field.name);
        }
    }
    return null;
}
