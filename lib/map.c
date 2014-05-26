/*!
 * Gauged - https://github.com/chriso/gauged
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#include <stdlib.h>
#include <string.h>
#include <float.h>
#include <math.h>
#include <assert.h>

#include "map.h"
#include "hash.h"

GAUGED_EXPORT gauged_map_t *gauged_map_import(const uint32_t *buffer, size_t size) {
    gauged_map_t *map = malloc(sizeof(gauged_map_t));
    if (!map) {
        return NULL;
    }
    map->length = 0;
    map->size = size ? size / sizeof(uint32_t) : GAUGED_MAP_INITIAL_SIZE;
    map->buffer = malloc(map->size * sizeof(uint32_t));
    if (!map->buffer) {
        goto error;
    }
    if (buffer) {
        memcpy(map->buffer, buffer, size);
        map->length = map->size;
    }
    return map;
error:
    free(map);
    return NULL;
}

GAUGED_EXPORT gauged_map_t *gauged_map_new() {
    return gauged_map_import(NULL, 0);
}

GAUGED_EXPORT void gauged_map_free(gauged_map_t *map) {
    free(map->buffer);
    free(map);
}

static inline int gauged_map_resize(gauged_map_t *map, size_t size) {
    size_t new_size = map->size;
    while (new_size < size) {
        new_size *= 2;
    }
    if (new_size > map->size) {
        uint32_t *buffer = realloc(map->buffer, new_size * sizeof(uint32_t));
        if (!buffer) {
            return GAUGED_ERROR;
        }
        map->buffer = buffer;
        map->size = new_size;
    }
    return GAUGED_OK;
}

GAUGED_EXPORT size_t gauged_map_length(const gauged_map_t *map) {
    return map->length * sizeof(uint32_t);
}

GAUGED_EXPORT uint32_t *gauged_map_export(const gauged_map_t *map) {
    return map->buffer;
}

GAUGED_EXPORT void gauged_map_clear(gauged_map_t *map) {
    map->length = 0;
}

GAUGED_EXPORT uint32_t *gauged_map_advance(uint32_t *buffer, size_t *header, uint32_t *position,
        size_t *length, float **array) {
    if (*buffer & 0x80000000) {
        *length = (*buffer >> 22) & 0x1FF;
        *position = *buffer & 0x3FFFFF;
        *header = 1;
    } else {
        *length = *buffer & 0x3FFFFFFF;
        *position = buffer[1];
        *header = 2;
    }
    if (array) {
        *array = (float *)(buffer + *header);
    }
    return *header + *length + buffer;
}

static inline size_t gauged_map_header_size(uint32_t position, size_t length) {
    assert(length < 0x80000000);
    return (position > ((1 << 22) - 1) || length > ((1 << 9) - 1)) + 1;
}

static inline void gauged_map_encode(uint32_t *buffer, uint32_t position, size_t length,
        const gauged_array_t *array) {
    size_t header = gauged_map_header_size(position, length);
    if (header == 1) {
        *buffer = 0x80000000 | (uint32_t)(length << 22) | position;
    } else {
        buffer[0] = (uint32_t)length & 0x7FFFFFFF;
        buffer[1] = position;
    }
    if (array) {
        memcpy(buffer + header, array->buffer, length * sizeof(uint32_t));
    }
}

GAUGED_EXPORT int gauged_map_append(gauged_map_t *map, uint32_t position,
        const gauged_array_t *array) {
    if (!array->length) {
        return GAUGED_OK;
    }
    size_t required_size = gauged_map_header_size(position, array->length) + array->length;
    if (!gauged_map_resize(map, map->length + required_size)) {
        return GAUGED_ERROR;
    }
    gauged_map_encode(map->buffer + map->length, position, array->length, array);
    map->length += required_size;
    return GAUGED_OK;
}

GAUGED_EXPORT int gauged_map_concat(gauged_map_t *a, const gauged_map_t *b, uint32_t start,
        uint32_t end, uint32_t offset) {
    size_t initial_length = a->length;
    gauged_array_t *array;
    uint32_t position;
    GAUGED_MAP_FOREACH(b, position, array) {
        if (position < start) {
            continue;
        }
        if (end && position >= end) {
            break;
        }
        if (!gauged_map_append(a, position + offset, array)) {
            a->length = initial_length;
            return GAUGED_ERROR;
        }
    }
    return GAUGED_OK;
}

static inline gauged_array_t *gauged_map_merge(gauged_map_t *map) {
    gauged_array_t *merged = gauged_array_new();
    gauged_map_t *replacement = gauged_map_new();
    if (!merged || !replacement) {
        goto error;
    }
    free(merged->buffer);
    merged->buffer = (float *)map->buffer;
    merged->size = map->size;
    gauged_array_t array;
    uint32_t position, *buffer = map->buffer, *end = map->buffer + map->length;
    size_t header;
    while (buffer < end) {
        buffer = gauged_map_advance(buffer, &header, &position, &array.length, &array.buffer);
        for (size_t i = 0; i < array.length; i++) {
            (merged->buffer + merged->length)[i] = array.buffer[i];
        }
        merged->length += array.length;
    }
    (void)position;
    (void)header;
    map->buffer = replacement->buffer;
    map->size = replacement->size;
    map->length = 0;
    free(replacement);
    return merged;
error:
    if (merged) gauged_array_free(merged);
    if (replacement) gauged_map_free(replacement);
    return NULL;
}

GAUGED_EXPORT float gauged_map_first(const gauged_map_t *map) {
    gauged_array_t *array;
    float result = NAN;
    GAUGED_MAP_FOREACH_ARRAY(map, array) {
        if (array->length) {
            result = array->buffer[0];
            break;
        }
    }
    return result;
}

GAUGED_EXPORT float gauged_map_last(const gauged_map_t *map) {
    gauged_array_t *array;
    float result = NAN;
    GAUGED_MAP_FOREACH_ARRAY(map, array) {
        if (array->length) {
            result = array->buffer[array->length - 1];
        }
    }
    return result;
}

GAUGED_EXPORT float gauged_map_sum(const gauged_map_t *map) {
    gauged_array_t *array;
    double result = 0;
    float element = 0;
    GAUGED_MAP_FOREACH_ARRAY(map, array) {
        GAUGED_ARRAY_FOREACH(array, element) {
            result += element;
        }
    }
    return (float)result;
}

GAUGED_EXPORT float gauged_map_min(const gauged_map_t *map) {
    gauged_array_t *array;
    float element = 0, result = INFINITY;
    GAUGED_MAP_FOREACH_ARRAY(map, array) {
        GAUGED_ARRAY_FOREACH(array, element) {
            if (element < result) {
                result = element;
            }
        }
    }
    return isinf(result) ? NAN : result;
}

GAUGED_EXPORT float gauged_map_max(const gauged_map_t *map) {
    gauged_array_t *array;
    float element = 0, result = -INFINITY;
    GAUGED_MAP_FOREACH_ARRAY(map, array) {
        GAUGED_ARRAY_FOREACH(array, element) {
            if (element > result) {
                result = element;
            }
        }
    }
    return isinf(result) ? NAN : result;
}

GAUGED_EXPORT float gauged_map_mean(const gauged_map_t *map) {
    gauged_array_t *array;
    float element = 0;
    double result = 0, total = 0;
    GAUGED_MAP_FOREACH_ARRAY(map, array) {
        total += array->length;
        GAUGED_ARRAY_FOREACH(array, element) {
            result += element;
        }
    }
    return total ? (float)result / (float)total : NAN;
}

GAUGED_EXPORT float gauged_map_sum_of_squares(const gauged_map_t *map, float mean) {
    gauged_array_t *array;
    double sum = 0;
    float element = 0;
    GAUGED_MAP_FOREACH_ARRAY(map, array) {
        GAUGED_ARRAY_FOREACH(array, element) {
            sum += (element - mean) * (element - mean);
        }
    }
    return (float)sum;
}

GAUGED_EXPORT float gauged_map_stddev(const gauged_map_t *map) {
    gauged_array_t *array;
    float element = 0;
    double sum = 0;
    size_t total = 0;
    GAUGED_MAP_FOREACH_ARRAY(map, array) {
        total += array->length;
        GAUGED_ARRAY_FOREACH(array, element) {
            sum += element;
        }
    }
    if (!total) {
        return NAN;
    }
    float mean = (float)sum / (float)total;
    return (float)sqrt(gauged_map_sum_of_squares(map, mean) / (float)total);
}

GAUGED_EXPORT float gauged_map_count(const gauged_map_t *map) {
    gauged_array_t *array;
    float result = 0;
    GAUGED_MAP_FOREACH_ARRAY(map, array) {
        result += array->length;
    }
    return result;
}

GAUGED_EXPORT int gauged_map_percentile(gauged_map_t *map, float percentile,
        float *result_) {
    if (!map->length || percentile < 0 || percentile > 100 || isnan(percentile)) {
        *result_ = NAN;
        return GAUGED_OK;
    }
    // TODO: Use the two-heap algorithm instead of sorting all floats
    float rank, nearest_rank, result;
    gauged_array_t *values = gauged_map_merge(map);
    if (!values) {
        return GAUGED_ERROR;
    }
    gauged_array_sort(values);
    rank = (float)(values->length - 1) * percentile / 100;
    nearest_rank = (float)floor(rank);
    if (ceil(rank) == nearest_rank) {
        result = values->buffer[(size_t)rank];
    } else {
        result = values->buffer[(size_t)nearest_rank];
        result += (rank - nearest_rank) * (values->buffer[(size_t)nearest_rank+1] - result);
    }
    gauged_array_free(values);
    *result_ = result;
    return GAUGED_OK;
}
