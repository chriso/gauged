/*!
 * Gauged
 * https://github.com/chriso/gauged (MIT Licensed)
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#ifndef GAUGED_SORT_H_
#define GAUGED_SORT_H_

#include <stdint.h>

/**
 * Various sorting strategies.
 */

typedef struct gauged_mergesort_s {
    uint32_t *buffer;
    uint32_t *output;
    size_t size;
    size_t depth;
} gauged_mergesort_t;

#define GAUGED_SORT_INSERTIONSORT_MAX 64
#define GAUGED_SORT_RADIXSORT_MAX (1024 * 1024 / sizeof(uint32_t))
#define GAUGED_SORT_MERGESORT_MAX_DEPTH 3

/**
 * Sort an array.
 */

uint32_t *gauged_sort(uint32_t *, size_t);

#endif
