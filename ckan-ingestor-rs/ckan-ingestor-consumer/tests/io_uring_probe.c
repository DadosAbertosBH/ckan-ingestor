#define _GNU_SOURCE

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

#ifndef SYS_io_uring_setup
#define SYS_io_uring_setup 425
#endif

#define IORING_SETUP_COOP_TASKRUN (1U << 8)
#define IORING_SETUP_TASKRUN_FLAG (1U << 9)

/*
 * The kernel ABI for struct io_uring_params is 120 bytes. The third u32 is
 * `flags`; the probe needs no other field, so an opaque ABI-sized buffer keeps
 * this independent from distribution-specific linux/io_uring.h headers.
 */
struct io_uring_params {
    uint32_t fields[30];
};

_Static_assert(sizeof(struct io_uring_params) == 120, "io_uring_params ABI size");

static void probe(const char *label, uint32_t flags) {
    struct io_uring_params params = {0};
    params.fields[2] = flags;

    const int fd = syscall(SYS_io_uring_setup, 4096, &params);
    if (fd < 0) {
        printf(
            "io_uring_setup label=%s flags=0x%x result=-1 errno=%d (%s)\\n",
            label,
            flags,
            errno,
            strerror(errno)
        );
        return;
    }

    printf(
        "io_uring_setup label=%s flags=0x%x result=%d errno=0\\n",
        label,
        flags,
        fd
    );
    close(fd);
}

int main(void) {
    probe("default", 0);
    probe("coop_taskrun", IORING_SETUP_COOP_TASKRUN);
    probe(
        "coop_taskrun_and_taskrun_flag",
        IORING_SETUP_COOP_TASKRUN | IORING_SETUP_TASKRUN_FLAG
    );
    return 0;
}
