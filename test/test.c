/*!
 * Gauged - https://github.com/chriso/gauged
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#include "test.h"

void gauged_map_debug(const gauged_map_t *map) {
    gauged_array_t *array;
    uint32_t position;
    GAUGED_MAP_FOREACH(map, position, array) {
        printf("[ %u ] = [", position);
        if (array->length) {
            printf("%.2f", array->buffer[0]);
            for (size_t i = 1; i < array->length; i++) {
                printf(", %.2f", array->buffer[i]);
            }
        }
        printf("]\n");
    }
}

int main() {
    gauged_array_t *array;
    gauged_map_t *map, *copy;
    float percentile;

    GAUGED_SUITE("Writer");

    gauged_writer_t *writer = gauged_writer_new(4);
    assert(writer);

    gauged_writer_emit(writer, 0, "foo", 10);
    gauged_writer_emit(writer, 0, "foo", 20);
    gauged_writer_emit(writer, 1, "baz", 30);
    gauged_writer_emit(writer, 1, "baz", 40);

    GAUGED_EXPECT("Writer ignores large keys",
        GAUGED_KEY_OVERFLOW == gauged_writer_emit(writer, 0, "foooo", 1));

    gauged_writer_flush_arrays(writer, 10);

    gauged_writer_emit(writer, 0, "baz", 50);
    gauged_writer_emit(writer, 1, "baz", 60);
    gauged_writer_flush_arrays(writer, 11);

    uint32_t expected_maps = 0;
    gauged_writer_hash_node_t *node;
    for (size_t i = 0; i < writer->pending->size; i++) {
        if (writer->pending->nodes[i]) {
            node = writer->pending->nodes[i];
            map = node->map;
            if (node->namespace_ == 0) {
                if (!strcmp("foo", node->key)) {
                    GAUGED_EXPECT("Pending map stores the namespace A", 0 == node->namespace_);
                    GAUGED_EXPECT("Pending map stores the key A", 0 == strcmp("foo", node->key));
                    GAUGED_EXPECT("Pending map stores the map A", gauged_map_sum(map) == 30);
                } else {
                    GAUGED_EXPECT("Pending map stores the namespace B", 0 == node->namespace_);
                    GAUGED_EXPECT("Pending map stores the key B", 0 == strcmp("baz", node->key));
                    GAUGED_EXPECT("Pending map stores the map B", gauged_map_sum(map) == 50);
                }
            } else if (node->namespace_ == 1) {
                GAUGED_EXPECT("Pending map stores the namespace C", 1 == node->namespace_);
                GAUGED_EXPECT("Pending map stores the key C", 0 == strcmp("baz", node->key));
                GAUGED_EXPECT("Pending map stores the map C", gauged_map_sum(map) == 130);
            }
            expected_maps++;
        }
    }

    GAUGED_EXPECT("Pending map count", 3 == expected_maps);

    GAUGED_EXPECT("Pending map size before flush", 3 == writer->pending->count);
    gauged_writer_flush_maps(writer, false);
    GAUGED_EXPECT("Pending map size after flush", 0 == writer->pending->count);

    gauged_writer_parse_query(writer, "foo=bar&baz&bah=&%3Ckey%3E=%3D%3Dvalue%3D%3D%3");

    GAUGED_EXPECT("Parsed key/value pairs from query", 6 == writer->buffer_size);

    char *expected[] = { "foo", "bar", "bah", "", "<key>", "==value==%3" };
    for (size_t i = 0; i < writer->buffer_size; i++) {
        GAUGED_EXPECT("Parsed key/value from string", !strcmp(expected[i], writer->buffer[i]));
    }

    gauged_writer_free(writer);

    GAUGED_SUITE("Arrays");

    array = gauged_array_new_values(1, 1.);
    assert(array);
    GAUGED_EXPECT_EQUALS("Array creation", array, 1);

    gauged_array_sort(array);
    GAUGED_EXPECT_SORTED("Array sorting", array);

    gauged_array_append(array, 10);
    gauged_array_append(array, 8);
    gauged_array_append(array, 6);
    gauged_array_sort(array);
    GAUGED_EXPECT_SORTED("Array sorting (small)", array);

    gauged_array_t *large = gauged_array_import(NULL, 2000000);
    assert(large);
    for (size_t i = 2000000; i; i--) {
        gauged_array_append(large, i);
    }
    gauged_array_sort(large);
    GAUGED_EXPECT_SORTED("Array sorting (1M+)", large);
    gauged_array_free(large);

    gauged_array_t *array_copy = gauged_array_import(gauged_array_export(array),
        gauged_array_length(array));
    assert(array_copy);
    GAUGED_EXPECT_EQUALS("Array import/export", array, 1, 6, 8, 10);
    gauged_array_free(array_copy);

    GAUGED_SUITE("Maps");

    map = gauged_map_new();
    assert(map);
    gauged_map_append(map, 10, array);

    gauged_array_clear(array);
    gauged_map_append(map, 12, array);
    gauged_array_append(array, 100);
    gauged_map_append(map, 15, array);
    gauged_map_append(map, 20, array);

    GAUGED_EXPECT("Map append A", gauged_map_sum(map) == 225);
    GAUGED_EXPECT("Map append B", gauged_map_length(map) == 36);

    gauged_map_t *map_copy = gauged_map_import(gauged_map_export(map),
        gauged_map_length(map));
    assert(map_copy);
    GAUGED_EXPECT("Map copy", gauged_map_sum(map_copy) == 225);
    gauged_map_free(map_copy);

    GAUGED_SUITE("Aggregates");

    gauged_map_clear(map);
    gauged_array_clear(array);
    gauged_array_append(array, 0.0);
    gauged_array_append(array, 10.0);
    gauged_array_append(array, 20.0);
    gauged_map_append(map, 10, array);

    gauged_array_clear(array);
    gauged_array_append(array, 5.5);
    gauged_array_append(array, -8.0);
    gauged_array_append(array, 14.5);
    gauged_map_append(map, 13, array);

    GAUGED_EXPECT("Map first", gauged_map_first(map) == 0);
    GAUGED_EXPECT("Map last", gauged_map_last(map) == 14.5);
    GAUGED_EXPECT("Map sum", gauged_map_sum(map) == 42.0);
    GAUGED_EXPECT("Map min", gauged_map_min(map) == -8.0);
    GAUGED_EXPECT("Map max", gauged_map_max(map) == 20.0);
    GAUGED_EXPECT("Map mean", gauged_map_mean(map) == 7.0);
    GAUGED_EXPECT("Map stddev", abs(gauged_map_stddev(map) - 9.224062735) < 0.000001);
    GAUGED_EXPECT("Map count", gauged_map_count(map) == 6);

    copy = GAUGED_MAP_COPY(map);
    gauged_map_percentile(map, 0, &percentile);
    GAUGED_EXPECT("Map percentile 0", percentile == -8);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    gauged_map_percentile(map, 40, &percentile);
    GAUGED_EXPECT("Map percentile 40", percentile == 5.5);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    gauged_map_percentile(map, 50, &percentile);
    GAUGED_EXPECT("Map percentile 50", percentile == 7.75);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    gauged_map_percentile(map, 75, &percentile);
    GAUGED_EXPECT("Map percentile 75", percentile == 13.375);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    gauged_map_percentile(map, 90, &percentile);
    GAUGED_EXPECT("Map percentile 90", percentile == 17.25);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    gauged_map_percentile(map, 100, &percentile);
    GAUGED_EXPECT("Map percentile 100", percentile == 20);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    gauged_map_percentile(map, -10, &percentile);
    GAUGED_EXPECT("Map percentile invalid", isnan(percentile));
    gauged_map_free(map);
    map = copy;

    gauged_array_clear(array);
    gauged_map_clear(map);

    for (float i = 1000; i; i--) {
        gauged_array_append(array, i);
    }
    gauged_map_append(map, 10, array);
    gauged_map_percentile(map, 99, &percentile);
    GAUGED_EXPECT("Map percentile large", abs(percentile - 990.01) < 0.00001);

    gauged_map_free(map);
    gauged_array_free(array);

    GAUGED_END;
}
