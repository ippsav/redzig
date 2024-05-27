const std = @import("std");
const TokenIterator = std.mem.TokenIterator;

const U8SequenceIterator = TokenIterator(u8, .sequence);

// Types
// Simple strings   +
// Simple Errors    -
// Simple Integers  :
// Bulk strings     $
// Arrays           *
// Nulls            _
// Booleans         #
// Doubles          ,
// Big numbers      (
// Bulk errors      !
// Verbatim strings =
// Maps             %
// Sets             ~
// Pushes           >

pub const RespData = union(enum) {
    string: []const u8,
    @"error": []const u8,
    integer: i64,
    bulk_string: []const u8,
    array: []RespData,
    null,
    boolean: bool,
    double: f64,
    bigint: i128,
    errors: [][]const u8,
};

pub const RespParser = struct {
    const Self = @This();

    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
        };
    }

    // connection accepted
    // reading...
    // received data: *1
    // $4
    // PING
    //
    // received data: *2
    // $4
    // ECHO
    // $4
    // word
    pub fn parseArray(self: *Self, lines: *U8SequenceIterator) error{ Overflow, InvalidCharacter, OutOfMemory }!RespData {
        const curr = lines.next().?;

        const length = try std.fmt.parseInt(usize, curr[1..2], 10);

        var resp_array = try std.ArrayList(RespData).initCapacity(self.allocator, length);
        defer resp_array.deinit();

        for (0..length) |_| {
            const resp_data = try self.parse(lines);
            try resp_array.append(resp_data);
        }

        return .{
            .array = try resp_array.toOwnedSlice(),
        };
    }

    pub fn parseBulkString(self: *Self, lines: *U8SequenceIterator) !RespData {
        const length_line = lines.next().?;
        const length = try std.fmt.parseInt(usize, length_line[1..], 10);

        const resp_string = try self.allocator.alloc(u8, length);

        const string_line = lines.next().?;
        @memcpy(resp_string, string_line);

        return .{
            .bulk_string = resp_string,
        };
    }

    pub fn parse(self: *Self, lines: *U8SequenceIterator) error{ Overflow, InvalidCharacter, OutOfMemory }!RespData {
        const peeked_line = lines.peek().?;
        switch (peeked_line[0]) {
            '*' => {
                return try self.parseArray(lines);
            },
            '$' => {
                return try self.parseBulkString(lines);
            },
            else => {
                return .null;
            },
        }
    }

    pub fn readStream(self: *Self, reader: anytype) !?RespData {
        // var header_bytes = std.ArrayList(u8).init(self.allocator);
        // defer header_bytes.deinit();
        //
        // try reader.streamUntilDelimiter(header_bytes.writer(), '\n', null);
        //
        // std.debug.assert(header_bytes.items.len > 0);
        // std.debug.assert(header_bytes.items[0] == '*');
        //
        // const header = std.mem.trimRight(u8, header_bytes.items, "\r");
        // std.debug.print("header: {s}\n", .{header[1..]});
        // std.debug.print("header length: {d}\n", .{header.len});
        //
        // const number_of_elements = try std.fmt.parseInt(usize, header[1..], 10);
        //
        var bytes = std.ArrayList(u8).init(self.allocator);
        defer bytes.deinit();

        var buffer: [1024]u8 = undefined;

        while (true) {
            const bytes_read = try reader.read(&buffer);
            if (bytes_read == 0) break;

            try bytes.appendSlice(buffer[0..bytes_read]);
            buffer = std.mem.zeroes([1024]u8);
            if (bytes_read < buffer.len) break;
        }
        if (bytes.items.len == 0) return null;

        var lines = std.mem.tokenizeSequence(u8, bytes.items, "\r\n");

        return try self.parse(&lines);
    }
};

test "RespParser" {
    const data = "1234567890";
    const reader = std.io.fixedBufferStream(data).reader();

    var parser = RespParser{};
    parser.readStream(reader);
}
