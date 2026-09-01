#define _GNU_SOURCE

#include <errno.h>
#include <linux/io_uring.h>
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

static void probe(const char *label, unsigned int flags) {
    struct io_uring_params params = {0};
    params.flags = flags;

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
