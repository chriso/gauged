/*!
 * Gauged - https://github.com/chriso/gauged
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>

#include "test.h"

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

    uint32_t data_points = 0;
    gauged_writer_emit_pairs(writer, 0, "baz=50", &data_points);
    GAUGED_EXPECT("Writer emit pairs tracks data points A", data_points == 1);
    data_points = 0;
    gauged_writer_emit_pairs(writer, 1, "baz=60&ignore=me", &data_points);
    GAUGED_EXPECT("Writer emit pairs tracks data points B", data_points == 1);
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
                    GAUGED_EXPECT_FLOAT_EQUALS("Pending map stores the map A", gauged_map_sum(map), 30);
                } else {
                    GAUGED_EXPECT("Pending map stores the namespace B", 0 == node->namespace_);
                    GAUGED_EXPECT("Pending map stores the key B", 0 == strcmp("baz", node->key));
                    GAUGED_EXPECT_FLOAT_EQUALS("Pending map stores the map B", gauged_map_sum(map), 50);
                }
            } else if (node->namespace_ == 1) {
                GAUGED_EXPECT("Pending map stores the namespace C", 1 == node->namespace_);
                GAUGED_EXPECT("Pending map stores the key C", 0 == strcmp("baz", node->key));
                GAUGED_EXPECT_FLOAT_EQUALS("Pending map stores the map C", gauged_map_sum(map), 130);
            }
            expected_maps++;
        }
    }

    GAUGED_EXPECT("Pending map count", 3 == expected_maps);

    GAUGED_EXPECT("Pending map size before flush", 3 == writer->pending->count);
    gauged_writer_flush_maps(writer, true);
    GAUGED_EXPECT("Pending map size after soft flush", 3 == writer->pending->count);
    gauged_writer_flush_maps(writer, false);
    GAUGED_EXPECT("Pending map size after flush", 0 == writer->pending->count);

    gauged_writer_parse_query(writer, "foo=bar&baz&bah=&%3Ckey%3E=%3D%3Dvalue%3D%3D%3");

    GAUGED_EXPECT("Parsed key/value pairs from query", 6 == writer->buffer_size);

    char *expected[] = { "foo", "bar", "bah", "", "<key>", "==value==%3" };
    assert(sizeof(expected) / sizeof(expected[0]) == writer->buffer_size);
    for (size_t i = 0; i < writer->buffer_size; i++) {
        GAUGED_EXPECT("Parsed key/value from string", !strcmp(expected[i], writer->buffer[i]));
    }

    gauged_writer_parse_query(writer, "foo+bar=baz\n");

    char *expected_b[] = { "foo bar", "baz" };
    GAUGED_EXPECT("Parsed key/value pairs from query", 2 == writer->buffer_size);
    assert(sizeof(expected_b) / sizeof(expected_b[0]) == writer->buffer_size);
    for (size_t i = 0; i < writer->buffer_size; i++) {
        GAUGED_EXPECT("Parsed key/value from string", !strcmp(expected_b[i], writer->buffer[i]));
    }

    gauged_writer_free(writer);

    writer = gauged_writer_new(4);

    char key[2] = { 'A', '\0' };
    for (char c = 'A'; c <= 'Z'; c++) {
        key[0] = c;
        gauged_writer_emit(writer, 0, key, 10);
    }

    expected_maps = 0;
    float writer_sum = 0;
    for (size_t i = 0; i < writer->pending->size; i++) {
        if (writer->pending->nodes[i]) {
            node = writer->pending->nodes[i];
            writer_sum += gauged_map_sum(node->map);
            expected_maps++;
        }
    }

    GAUGED_EXPECT("Count of all maps", expected_maps == 26);
    GAUGED_EXPECT_FLOAT_EQUALS("Sum of all maps", writer_sum, 260);

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

    gauged_array_t *medium = gauged_array_import(NULL, 2000000);
    assert(medium);
    for (size_t i = 1000000; i; i--) {
        gauged_array_append(medium, i);
    }
    gauged_array_sort(medium);
    GAUGED_EXPECT_SORTED("Array sorting (medium)", medium);
    gauged_array_free(medium);

    gauged_array_t *large = gauged_array_import(NULL, 2000000);
    assert(large);
    for (size_t i = 2000000; i; i--) {
        gauged_array_append(large, i);
    }
    gauged_array_sort(large);
    GAUGED_EXPECT_SORTED("Array sorting (large)", large);
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

    GAUGED_EXPECT_FLOAT_EQUALS("Map append A", gauged_map_sum(map), 225);
    GAUGED_EXPECT("Map append B", gauged_map_length(map) == 36);

    gauged_map_t *map_copy = gauged_map_import(gauged_map_export(map),
        gauged_map_length(map));
    assert(map_copy);
    GAUGED_EXPECT_FLOAT_EQUALS("Map copy", gauged_map_sum(map), 225);
    gauged_map_free(map_copy);

    map_copy = gauged_map_new();
    assert(map_copy);
    gauged_map_concat(map_copy, map, 12, 20, 0);
    GAUGED_EXPECT_FLOAT_EQUALS("Map concat A", gauged_map_sum(map_copy), 100);
    gauged_map_clear(map_copy);
    gauged_map_concat(map_copy, map, 12, 21, 0);
    GAUGED_EXPECT_FLOAT_EQUALS("Map concat B", gauged_map_sum(map_copy), 200);
    gauged_map_free(map_copy);

    GAUGED_SUITE("Aggregates");

    gauged_map_clear(map);

    GAUGED_EXPECT("Empty map first", isnan(gauged_map_first(map)));
    GAUGED_EXPECT("Empty map last", isnan(gauged_map_last(map)));
    GAUGED_EXPECT("Empty map sum", isnan(gauged_map_min(map)));
    GAUGED_EXPECT("Empty map min", isnan(gauged_map_min(map)));
    GAUGED_EXPECT("Empty map max", isnan(gauged_map_max(map)));
    GAUGED_EXPECT("Empty map mean", isnan(gauged_map_mean(map)));
    GAUGED_EXPECT("Empty map stddev", isnan(gauged_map_stddev(map)));
    GAUGED_EXPECT_FLOAT_EQUALS("Empty map count", gauged_map_count(map), 0);

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

    GAUGED_EXPECT_FLOAT_EQUALS("Map first", gauged_map_first(map), 0);
    GAUGED_EXPECT_FLOAT_EQUALS("Map last", gauged_map_last(map), 14.5);
    GAUGED_EXPECT_FLOAT_EQUALS("Map sum", gauged_map_min(map), 42);
    GAUGED_EXPECT_FLOAT_EQUALS("Map min", gauged_map_min(map), -8);
    GAUGED_EXPECT_FLOAT_EQUALS("Map max", gauged_map_max(map), 20);
    GAUGED_EXPECT_FLOAT_EQUALS("Map mean", gauged_map_mean(map), 7);
    GAUGED_EXPECT_FLOAT_EQUALS("Map stddev", gauged_map_stddev(map), 9.224062735);
    GAUGED_EXPECT_FLOAT_EQUALS("Map count", gauged_map_count(map), 6);

    copy = GAUGED_MAP_COPY(map);
    gauged_map_percentile(map, 0, &percentile);
    GAUGED_EXPECT_FLOAT_EQUALS("Map percentile 0", percentile, -8);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    gauged_map_percentile(map, 40, &percentile);
    GAUGED_EXPECT_FLOAT_EQUALS("Map percentile 40", percentile, 5.5);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    gauged_map_percentile(map, 50, &percentile);
    GAUGED_EXPECT_FLOAT_EQUALS("Map percentile 50", percentile, 7.75);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    gauged_map_percentile(map, 75, &percentile);
    GAUGED_EXPECT_FLOAT_EQUALS("Map percentile 75", percentile, 13.375);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    gauged_map_percentile(map, 90, &percentile);
    GAUGED_EXPECT_FLOAT_EQUALS("Map percentile 90", percentile, 17.25);
    gauged_map_free(map);
    map = copy;
    copy = GAUGED_MAP_COPY(map);
    gauged_map_percentile(map, 100, &percentile);
    GAUGED_EXPECT_FLOAT_EQUALS("Map percentile 100", percentile, 20);
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
    GAUGED_EXPECT_FLOAT_EQUALS("Map percentile large", percentile, 990.01);

    gauged_map_free(map);
    gauged_array_free(array);

    GAUGED_END;
}
