/*!
 * Gauged
 * https://github.com/chriso/gauged (MIT Licensed)
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#ifndef GAUGED_MAP_H_
#define GAUGED_MAP_H_

#include <stdint.h>

#include "array.h"
#include "common.h"

/**
 * A sparse map which encodes one or more float arrays with an offset.
 *
 * Arrays are packed together using the following encoding:
 *
 * <header1><array1><header2><array2>...<headerN><arrayN>
 *
 * The header encodes the array's length and position in one of two ways:
 *
 * Short encoding: 1LLLLLLL LLPPPPPP PPPPPPPP PPPPPPPP
 *  Long encoding: 0LLLLLLL LLLLLLLL LLLLLLLL LLLLLLLL
 *                 PPPPPPPP PPPPPPPP PPPPPPPP PPPPPPPP
 */

typedef struct gauged_map_s {
    uint32_t *buffer;
    size_t size;
    size_t length;
} gauged_map_t;

/**
 * Create a new map.
 */

gauged_map_t *gauged_map_new(void);

#define GAUGED_MAP_INITIAL_SIZE 32

/**
 * Free the specified map.
 */

void gauged_map_free(gauged_map_t *);

/**
 * Access the map buffer.
 */

uint32_t *gauged_map_export(const gauged_map_t *);

/**
 * Get the length of the map in bytes.
 */

size_t gauged_map_length(const gauged_map_t *);

/**
 * Clear the map.
 */

void gauged_map_clear(gauged_map_t *);

/**
 * Create a new map using the specified buffer and length in bytes.
 */

gauged_map_t *gauged_map_import(const uint32_t *, size_t);

/**
 * Append an array to end of the map.
 */

int gauged_map_append(gauged_map_t *, uint32_t, const gauged_array_t *);

/**
 * Slice and concatenate a map on to another.
 */

#define GAUGED_MAP_START 0
#define GAUGED_MAP_END 0

int gauged_map_concat(gauged_map_t *a, const gauged_map_t *b,
    uint32_t start, uint32_t end, uint32_t offset);

/**
 * Get the first float in the map.
 */

float gauged_map_first(const gauged_map_t *);

/**
 * Get the last float in the map.
 */

float gauged_map_last(const gauged_map_t *);

/**
 * Get the sum of all floats in the map.
 */

float gauged_map_sum(const gauged_map_t *);

/**
 * Get the minimum of all floats in the map.
 */

float gauged_map_min(const gauged_map_t *);

/**
 * Get the maximum of all floats in the map.
 */

float gauged_map_max(const gauged_map_t *);

/**
 * Get the mean of all floats in the map.
 */

float gauged_map_mean(const gauged_map_t *);

/**
 * Get the standard deviation of all floats in the map.
 */

float gauged_map_stddev(const gauged_map_t *);

/**
 * Get the sum of squared differences.
 */

float gauged_map_sum_of_squares(const gauged_map_t *, float mean);

/**
 * Count the number of floats in the map.
 */

float gauged_map_count(const gauged_map_t *);

/**
 * Get a percentile of all floats in the map. Note that this function
 * uses the buffer to sort the floats in-place. You'll need to create
 * a copy of the map prior to calling this if you want to re-use it
 * afterwards.
 */

int gauged_map_percentile(gauged_map_t *, float percentile, float *result);

/**
 * Provide a way to iterate over all positions/arrays in a map.
 */

#define GAUGED_MAP_FOREACH(map, position, array) \
    (void)position; \
    (void)array; \
    position = 0; \
    size_t ZTMP(header, __LINE__) = 0; \
    gauged_array_t ZTMP(s, __LINE__); \
    array = &ZTMP(s, __LINE__); \
    uint32_t *ZTMP(buffer, __LINE__) = map->buffer; \
    uint32_t *ZTMP(end, __LINE__) = map->buffer + map->length; \
    while (ZTMP(buffer, __LINE__) < ZTMP(end, __LINE__) \
        ? (((*ZTMP(buffer, __LINE__) & 0x80000000) \
            ? ((ZTMP(s, __LINE__).length = (*ZTMP(buffer, __LINE__) >> 22) & 0x1FF), \
              (position = *ZTMP(buffer, __LINE__) & 0x3FFFFF), \
              (ZTMP(header, __LINE__) = 1)) \
            : ((ZTMP(s, __LINE__).length = (*ZTMP(buffer, __LINE__)) & 0x3FFFFFFF), \
              (position = ZTMP(buffer, __LINE__)[1]), \
              (ZTMP(header, __LINE__) = 2))), \
          (ZTMP(s, __LINE__).buffer = (float *) ZTMP(buffer, __LINE__) + ZTMP(header, __LINE__)), \
          (ZTMP(buffer, __LINE__) += ZTMP(header, __LINE__) + ZTMP(s, __LINE__).length), \
          1) : 0)

#define GAUGED_MAP_FOREACH_ARRAY(map, array) \
    (void)array; \
    size_t ZTMP(header, __LINE__) = 0; \
    gauged_array_t ZTMP(s, __LINE__); \
    array = &ZTMP(s, __LINE__); \
    uint32_t *ZTMP(buffer, __LINE__) = map->buffer; \
    uint32_t *ZTMP(end, __LINE__) = map->buffer + map->length; \
    while (ZTMP(buffer, __LINE__) < ZTMP(end, __LINE__) \
        ? (((*ZTMP(buffer, __LINE__) & 0x80000000) \
            ? ((ZTMP(s, __LINE__).length = (*ZTMP(buffer, __LINE__) >> 22) & 0x1FF), \
              (ZTMP(header, __LINE__) = 1)) \
            : ((ZTMP(s, __LINE__).length = (*ZTMP(buffer, __LINE__)) & 0x3FFFFFFF), \
              (ZTMP(header, __LINE__) = 2))), \
          (ZTMP(s, __LINE__).buffer = (float *) ZTMP(buffer, __LINE__) + ZTMP(header, __LINE__)), \
          (ZTMP(buffer, __LINE__) += ZTMP(header, __LINE__) + ZTMP(s, __LINE__).length), \
          1) : 0)

uint32_t *gauged_map_advance(uint32_t *, size_t *, uint32_t *, size_t *, float **);

#endif
