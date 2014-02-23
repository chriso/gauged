/*!
 * Gauged - https://github.com/chriso/gauged
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#ifndef CTEST_H_
#define CTEST_H_

#include <stdio.h>
#include <sys/time.h>
#include <assert.h>

static double ctest_elapsed;
static int ctest_fails = 0, ctest_last = 0;
static char *ctest_msg = NULL;
static struct timeval ctest_start, ctest_end;

#define CTEST_SUITE(name) \
    do { \
        printf("\n  \x1B[1m\x1B[37m" name "\x1B[0m\n\n"); \
        (void)ctest_msg; (void)ctest_elapsed; \
        (void)ctest_fails; (void)ctest_last; \
        (void)ctest_start; (void)ctest_end; \
    } while (0)

#define CTEST_END \
    do { \
        CTEST_SUITE("Summary"); \
        if (ctest_fails) { \
            printf("    [  \x1B[31mFAIL\x1B[0m  ] %u failed C test%s\n\n", \
                ctest_fails, ctest_fails != 1 ? "s" : ""); \
        } else { \
            printf("    [  \x1B[32mPASS\x1B[0m  ] All C tests passed\n\n"); \
        } \
        exit(ctest_fails); \
    } while (0)

#define CTEST_EXPECT(msg, condition) \
    do { \
        if (ctest_last) puts(""); \
        if (!(condition)) { \
            printf("    [  \x1B[31mFAIL\x1B[0m  ] %s\n\n" \
                "        Failed assertion \x1B[31m%s\x1B[0m at \x1B[36m%s:%u\x1B[0m\n", \
                msg, #condition, __FILE__, __LINE__); \
            ctest_fails++; ctest_last = 1; \
        } else { \
            printf("    [  \x1B[32mPASS\x1B[0m  ] %s\n", msg); \
            ctest_last = 0; \
        } \
    } while (0)

#define CTEST_ASSERT(msg, condition) \
    do { \
        CTEST_EXPECT(msg, condition); \
        if (!(condition)) { \
            CTEST_END; \
        } \
    } while (0)

#define CTEST_BENCH_START(msg) \
    do { \
        gettimeofday(&ctest_start, NULL); \
        printf("    [  ....s  ....MB/s  ] %s", msg); \
        fflush(stdout); \
        ctest_msg = msg; \
    } while (0)

#define CTEST_BENCH_END(size) \
    do { \
        gettimeofday(&ctest_end, NULL); \
        ctest_elapsed = (double)(ctest_end.tv_sec - ctest_start.tv_sec) \
            + (double)(ctest_end.tv_usec - ctest_start.tv_usec) / 1000000; \
        if (size / ctest_elapsed / 1024 / 1024 >= 10000) { \
            printf("\r    [  \x1B[32m%1.2fs\x1B[0m  \x1B[33minf MB/s\x1B[0m  ] %s", \
                ctest_elapsed, ctest_msg); \
        } else { \
            printf("\r    [  \x1B[32m%1.2fs\x1B[0m  \x1B[33m%4.0fMB/s\x1B[0m  ] %s", \
                ctest_elapsed, (float)(size) / 1024 / 1024 / ctest_elapsed, ctest_msg); \
        } \
        printf("                        \n"); \
    } while(0)

#endif
