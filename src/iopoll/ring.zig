const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const log = std.log;

pub const Ring = struct {
    ring: linux.IoUring,
    next_operation_id: u64 = first_operation_id,

    const first_operation_id: u64 = 1024;

    pub const RingConfig = struct {
        entries: u16 = 1024,
        sqpoll: bool = true,
        cpu_core: u32 = 0,
        idle_ms: u32 = 100,
        single_issuer: bool = true,
    };

    pub fn init(config: RingConfig) !Ring {
        if (config.entries < 4 or
            config.entries > 4096 or
            !std.math.isPowerOfTwo(config.entries))
        {
            return error.InvalidRingEntries;
        }

        const cpu_count = try std.Thread.getCpuCount();

        if (config.sqpoll and config.cpu_core >= cpu_count) {
            return error.SqpollCpuCoreOutOfBounds;
        }

        var params = std.mem.zeroes(linux.io_uring_params);

        if (config.sqpoll) {
            params.flags |= linux.IORING_SETUP_SQPOLL;
            params.flags |= linux.IORING_SETUP_SQ_AFF;

            params.sq_thread_idle = config.idle_ms;
            params.sq_thread_cpu = config.cpu_core;
        }

        if (config.single_issuer) {
            params.flags |= linux.IORING_SETUP_SINGLE_ISSUER;
        }

        const ring = try linux.IoUring.init_params(
            config.entries,
            &params,
        );

        return .{
            .ring = ring,
        };
    }

    pub fn deinit(self: *Ring) void {
        self.ring.deinit();
    }

    pub fn waitReadable(
        self: *Ring,
        fd: posix.fd_t,
    ) !void {
        const result = try self.waitPoll(
            fd,
            linux.POLL.IN,
        );

        try validePollResult(
            result,
            linux.POLL.IN,
        );
    }

    pub fn waitWritable(
        self: *Ring,
        fd: posix.fd_t,
    ) !void {
        const result = try self.waitPoll(
            fd,
            linux.POLL.OUT,
        );

        try validePollResult(
            result,
            linux.POLL.OUT,
        );
    }

    pub fn waitPoll(
        self: *Ring,
        fd: posix.fd_t,
        events: u32,
    ) !u32 {
        if (events != linux.POLL.IN and
            events != linux.POLL.OUT)
        {
            return error.InvalidPollEvents;
        }

        const operation_id = self.nextOperationId();

        const sqe = self.ring.get_sqe() catch {
            return error.SubmissionQueueFull;
        };

        sqe.prep_poll_add(
            fd,
            events,
        );
        sqe.user_data = operation_id;

        const result = try self.submitAndWait(operation_id);

        return @intCast(result);
    }

    fn submitAndWait(
        self: *Ring,
        operation_id: u64,
    ) !i32 {
        _ = self.ring.submit_and_wait(1) catch |err| {
            log.err(
                "[io_uring] submit and wait failed: {s}",
                .{@errorName(err)},
            );

            return error.IoUringSubmitAndWaitFailed;
        };

        while (true) {
            const cqe = self.ring.copy_cqe() catch {
                _ = self.ring.submit_and_wait(1) catch {
                    return error.IoUringWaitFailed;
                };

                continue;
            };

            const received_operation_id = cqe.user_data;
            const result = cqe.res;
            const flags = cqe.flags;

            if (received_operation_id != operation_id) {
                log.err(
                    "[io_uring] unexpected completion: expected={d}, received={d}, flags={d}",
                    .{ operation_id, received_operation_id, flags },
                );

                return error.UnexpectedCompletion;
            }

            if (result < 0) {
                return posix.unexpectedErrno(
                    @enumFromInt(-result),
                );
            }

            return result;
        }
    }

    fn nextOperationId(self: *Ring) u64 {
        const operation_id = self.next_operation_id;
        self.next_operation_id +%= 1;

        if (self.next_operation_id < first_operation_id) {
            self.next_operation_id = first_operation_id;
        }

        return operation_id;
    }

    fn validePollResult(
        result: u32,
        expected: u32,
    ) !void {
        const terminal = linux.POLL.ERR | linux.POLL.HUP | linux.POLL.NVAL;

        if ((result & linux.POLL.NVAL) != 0) {
            return error.InvalidPolledFileDescriptor;
        }

        if ((result & linux.POLL.ERR) != 0) {
            return error.PollSocketError;
        }

        if ((result & linux.POLL.HUP) != 0 and
            (result & expected) == 0)
        {
            return error.PolledSocketClosed;
        }

        if ((result & expected) == 0 and
            (result & terminal) == 0)
        {
            return error.UnexpectedPollResult;
        }
    }
};
