#ifndef CLOCKCYCLE_H
#define CLOCKCYCLE_H

#include <stdint.h>
#include <time.h>

static inline uint64_t clock_now(void)
{
#if defined(__powerpc__) || defined(__powerpc64__) || defined(__ppc__) || defined(__PPC__)
    unsigned int tbl;
    unsigned int tbu0;
    unsigned int tbu1;

    do {
        __asm__ __volatile__("mftbu %0" : "=r"(tbu0));
        __asm__ __volatile__("mftb %0" : "=r"(tbl));
        __asm__ __volatile__("mftbu %0" : "=r"(tbu1));
    } while (tbu0 != tbu1);

    return (((uint64_t)tbu0) << 32) | tbl;
#else
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
#endif
}

static inline double clock_ticks_to_seconds(uint64_t delta)
{
#if defined(__powerpc__) || defined(__powerpc64__) || defined(__ppc__) || defined(__PPC__)
    return (double)delta / 512000000.0;
#else
    return (double)delta / 1000000000.0;
#endif
}

#endif
