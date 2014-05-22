/*!
 * Gauged - https://github.com/chriso/gauged
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include "common.h"
#include "array.h"
#include "sort.h"

GAUGED_EXPORT gauged_array_t *gauged_array_import(const float *buffer, size_t size) {
    gauged_array_t *array = malloc(sizeof(gauged_array_t));
    if (!array) {
        return NULL;
    }
    array->length = 0;
    array->size = size ? size / sizeof(float) : GAUGED_ARRAY_INITIAL_SIZE;
    array->buffer = malloc(array->size * sizeof(float));
    if (!array->buffer) {
        goto error;
    }
    if (buffer) {
        memcpy(array->buffer, buffer, size);
        array->length = array->size;
    }
    return array;
error:
    free(array);
    return NULL;
}

GAUGED_EXPORT gauged_array_t *gauged_array_new() {
    return gauged_array_import(NULL, 0);
}

GAUGED_EXPORT gauged_array_t *gauged_array_new_values(size_t length, ...) {
    gauged_array_t *array = gauged_array_new();
    if (!array) {
        return NULL;
    }
    va_list va;
    va_start(va, length);
    for (size_t i = 0; i < length; i++) {
        gauged_array_append(array, (float)va_arg(va, double));
    }
    va_end(va);
    return array;
}

GAUGED_EXPORT void gauged_array_free(gauged_array_t *array) {
    free(array->buffer);
    free(array);
}

static inline int gauged_array_resize(gauged_array_t *array, size_t size) {
    size_t new_size = array->size;
    while (new_size < size) {
        new_size *= 2;
    }
    if (new_size > array->size) {
        float *buffer = realloc(array->buffer, new_size * sizeof(float));
        if (!buffer) {
            return GAUGED_ERROR;
        }
        array->buffer = buffer;
        array->size = new_size;
    }
    return GAUGED_OK;
}

GAUGED_EXPORT size_t gauged_array_length(const gauged_array_t *array) {
    return array->length * sizeof(float);
}

GAUGED_EXPORT float *gauged_array_export(const gauged_array_t *array) {
    return array->buffer;
}

GAUGED_EXPORT void gauged_array_clear(gauged_array_t *array) {
    array->length = 0;
}

GAUGED_EXPORT int gauged_array_append(gauged_array_t *array, float value) {
    if (!gauged_array_resize(array, array->length + 1)) {
        return GAUGED_ERROR;
    }
    array->buffer[array->length++] = value;
    return GAUGED_OK;
}

GAUGED_EXPORT int gauged_array_sort(gauged_array_t *array) {
    if (array->length < 2) {
        return GAUGED_OK;
    }
    //Convert to uint32_t and then sort with either radixsort or mergesort
    uint32_t *buffer = (uint32_t *)array->buffer;
    int result = GAUGED_OK;
    for (size_t i = 0; i < array->length; i++) {
        buffer[i] ^= (-(buffer[i] >> 31)) | 0x80000000;
    }
    uint32_t *sorted = gauged_sort(buffer, array->length);
    if (!sorted) {
        result = GAUGED_ERROR;
    } else if (sorted != buffer) {
        free(buffer);
        buffer = sorted;
        array->buffer = (float *)sorted;
    }
    for (size_t i = 0; i < array->length; i++) {
        buffer[i] ^= ((buffer[i] >> 31) - 1) | 0x80000000;
    }
    return result;
}
