/*!
 * Gauged - https://github.com/chriso/gauged
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#ifndef GAUGED_TEST_H_
#define GAUGED_TEST_H_

#include "ctest.h"
#include "writer.h"

#define GAUGED_SUITE(s)        CTEST_SUITE(s)
#define GAUGED_END             CTEST_END
#define GAUGED_EXPECT(m,c)     CTEST_EXPECT(m,c)
#define GAUGED_ASSERT(m,c)     CTEST_ASSERT(m,c)
#define GAUGED_BENCH_START(m)  CTEST_BENCH_START(m)
#define GAUGED_BENCH_END(s)    CTEST_BENCH_END(s)

#define GAUGED_TMP_A(var, line) GAUGED_TMP_B(var, line)
#define GAUGED_TMP_B(var, line) var##line

#define GAUGED_FLOAT_DIFF 0.00001f

#define GAUGED_EXPECT_FLOAT_EQUALS(msg, value, expected) \
    CTEST_EXPECT(msg, fabs((float)value - (float)expected < GAUGED_FLOAT_DIFF))

#define GAUGED_EXPECT_EQUALS(msg, array, ...) \
    do { \
        float GAUGED_TMP_A(arr, __LINE__)[] = { __VA_ARGS__ }; \
        size_t GAUGED_TMP_A(exp, __LINE__) = sizeof(GAUGED_TMP_A(arr, __LINE__)) / sizeof(float); \
        bool mismatch = false; \
        if (!array || array->length != GAUGED_TMP_A(exp, __LINE__)) { \
            mismatch = true; \
        } else { \
            for (size_t i = 0; i < GAUGED_TMP_A(exp, __LINE__); i++) { \
                if (fabs(array->buffer[i] - GAUGED_TMP_A(arr, __LINE__)[i]) >= GAUGED_FLOAT_DIFF) { \
                    mismatch = true; \
                    break; \
                } \
            } \
        } \
        CTEST_EXPECT(msg, !mismatch); \
    } while (0)

#define GAUGED_EXPECT_SORTED(msg, array) \
    do { \
        bool sorted = true; \
        for (size_t i = 1; i < array->length; i++) { \
            if (array->buffer[i] < array->buffer[i-1]) { \
                sorted = false; \
                break; \
            } \
        } \
        CTEST_EXPECT(msg, sorted); \
    } while (0)

#define GAUGED_MAP_COPY(map) \
    gauged_map_import(gauged_map_export(map), gauged_map_length(map))

#endif
