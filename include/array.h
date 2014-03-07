/*!
 * Gauged
 * https://github.com/chriso/gauged (MIT Licensed)
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#ifndef GAUGED_ARRAY_H_
#define GAUGED_ARRAY_H_

/**
 * An array of floats.
 */

typedef struct gauged_array_s {
    float *buffer;
    size_t size;
    size_t length;
} gauged_array_t;

/**
 * Create a new array.
 */

gauged_array_t *gauged_array_new(void);

#define GAUGED_ARRAY_INITIAL_SIZE 4

/**
 * Create a new array with the specified elements.
 */

gauged_array_t *gauged_array_new_values(size_t, ...);

/**
 * Free the specified array.
 */

void gauged_array_free(gauged_array_t *);

/**
 * Get the array buffer.
 */

float *gauged_array_export(const gauged_array_t *);

/**
 * Get the length of the array in bytes.
 */

size_t gauged_array_length(const gauged_array_t *);

/**
 * Clear the array.
 */

void gauged_array_clear(gauged_array_t *);

/**
 * Create a new array using the specified buffer and length in bytes.
 */

gauged_array_t *gauged_array_import(const float *, size_t);

/**
 * Add a float to the array.
 */

int gauged_array_append(gauged_array_t *, float);

/**
 * Sort the array.
 */

int gauged_array_sort(gauged_array_t *);

/**
 * Iterate over all floats in the array.
 */

#define GAUGED_ARRAY_FOREACH(array, element) \
    size_t ZTMP(i, __LINE__) = 0; \
    while (ZTMP(i, __LINE__) < array->length ? \
        (element = array->buffer[ZTMP(i, __LINE__)++], 1) : 0)

#endif
