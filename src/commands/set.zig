const std = @import("std");
const RespData = @import("../resp/encoding.zig").RespData;
const utils = @import("../utils.zig");

const ParsingSetCommandStep = enum { key, value, expiry };

pub const SetOptionalParams = enum { ex, px };

const SetCommandValues = struct {
    key: []const u8,
    value: RespData,
    expiration_ms: ?i64,
};

pub fn parseSetCommand(parsed_data: RespData) !SetCommandValues {
    var step: ParsingSetCommandStep = .key;

    var key: []const u8 = undefined;
    var value: RespData = undefined;

    var opt_param_enum: ?SetOptionalParams = null;

    var expiration_ms: ?i64 = null;

    if (parsed_data.array.len < 3) return error.InvalidCommand;

    var i: usize = 1;
    while (i < parsed_data.array.len) : (i += 1) {
        const arg = parsed_data.array[i];

        if (i >= 3 and i % 2 != 0) {
            opt_param_enum = utils.getEnumIgnoreCase(SetOptionalParams, arg.bulk_string) orelse return error.InvalidOptionalParam;
            switch (opt_param_enum.?) {
                .ex, .px => {
                    std.debug.assert(expiration_ms == null);
                    step = .expiry;
                },
            }
        }
        switch (step) {
            .key => {
                key = arg.bulk_string;
                step = .value;
            },
            .value => {
                value = arg;
                if (parsed_data.array.len == 3) break;
            },
            .expiry => {
                i += 1;
                if (i >= parsed_data.array.len) return error.MissingExpiration;
                const expiration_arg = parsed_data.array[i];
                expiration_ms = try std.fmt.parseInt(i64, expiration_arg.bulk_string, 10);
                if (opt_param_enum == .ex) {
                    expiration_ms = expiration_ms.? * 1000;
                }
                step = .value; // Reset to value for further parsing if needed
            },
        }
    }
    return SetCommandValues{
        .key = key,
        .value = value,
        .expiration_ms = expiration_ms,
    };
}
