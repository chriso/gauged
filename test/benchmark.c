/*!
 * Gauged - https://github.com/chriso/gauged
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#include "test.h"

#define ARRAY_COUNT 10000000
#define ARRAY_COUNT_HUMAN "10M"
#define ARRAY_SIZE 4
#define ARRAY_SIZE_HUMAN "4"
#define ARRAY_FLOATS_TOTAL "40M"

gauged_map_t *gauged_map_random(size_t array_count, size_t array_size) {
    gauged_map_t *map = gauged_map_new();
    gauged_array_t *array = gauged_array_new();
    if (!map || !array) {
        goto error;
    }
    for (size_t i = 1; i <= array_count; i++) {
        array->length = 0;
        for (size_t j = 0; j <= array_size; j++) {
            if (!gauged_array_append(array, drand48())) {
                goto error;
            }
        }
        if (!gauged_map_append(map, i, array)) {
            goto error;
        }
    }
    gauged_array_free(array);
    return map;
error:
    if (array) gauged_array_free(array);
    if (map) gauged_map_free(map);
    return NULL;
}

int main() {
    gauged_map_t *map, *copy;
    volatile float result;
    size_t size = ARRAY_COUNT * ARRAY_SIZE * sizeof(float);

    srand48(time(NULL));

    GAUGED_SUITE("Map creation");

    GAUGED_BENCH_START("Creating a map of " ARRAY_COUNT_HUMAN " arrays each with "
        ARRAY_SIZE_HUMAN " floats (" ARRAY_FLOATS_TOTAL " total)");
    map = gauged_map_random(ARRAY_COUNT, ARRAY_SIZE);
    assert(map);
    GAUGED_BENCH_END(map->length);

    GAUGED_SUITE("Aggregates");

    GAUGED_BENCH_START("First");
    result = gauged_map_first(map);
    GAUGED_BENCH_END(size);
    GAUGED_BENCH_START("Last");
    result = gauged_map_last(map);
    GAUGED_BENCH_END(size);
    GAUGED_BENCH_START("Sum");
    result = gauged_map_sum(map);
    GAUGED_BENCH_END(size);
    GAUGED_BENCH_START("Min");
    result = gauged_map_min(map);
    GAUGED_BENCH_END(size);
    GAUGED_BENCH_START("Max");
    result = gauged_map_max(map);
    GAUGED_BENCH_END(size);
    GAUGED_BENCH_START("Mean");
    result = gauged_map_mean(map);
    GAUGED_BENCH_END(size);
    GAUGED_BENCH_START("Stddev");
    result = gauged_map_stddev(map);
    GAUGED_BENCH_END(size);
    GAUGED_BENCH_START("Count");
    result = gauged_map_count(map);
    GAUGED_BENCH_END(size);
    copy = GAUGED_MAP_COPY(map);
    GAUGED_BENCH_START("Percentile (5th)");
    gauged_map_percentile(map, 5, (float *)&result);
    GAUGED_BENCH_END(size);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    GAUGED_BENCH_START("Percentile (25th)");
    gauged_map_percentile(map, 25, (float *)&result);
    GAUGED_BENCH_END(size);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    GAUGED_BENCH_START("Percentile (50th)");
    gauged_map_percentile(map, 50, (float *)&result);
    GAUGED_BENCH_END(size);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    GAUGED_BENCH_START("Percentile (75th)");
    gauged_map_percentile(map, 75, (float *)&result);
    GAUGED_BENCH_END(size);
    gauged_map_free(map);
    map = copy;
    GAUGED_BENCH_START("Percentile (95th)");
    gauged_map_percentile(map, 95, (float *)&result);
    GAUGED_BENCH_END(size);

    gauged_map_free(map);

    puts("");

    return 0;
}
