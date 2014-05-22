/*!
 * Gauged - https://github.com/chriso/gauged
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#include <stdlib.h>
#include <string.h>
#if !defined(WIN32) && !defined(_WIN32)
# include <pthread.h>
#endif

#include "common.h"
#include "sort.h"

static inline void gauged_sort_insertion(uint32_t *array, uint32_t offset, size_t end) {
    uint32_t x, y, temp;
    for (x = offset; x < end; ++x) {
        for (y = x; y > offset && array[y-1] > array[y]; y--) {
            temp = array[y];
            array[y] = array[y-1];
            array[y-1] = temp;
        }
    }
}

static void gauged_sort_radix(uint32_t *array, uint32_t offset, size_t end, uint32_t shift) {
    uint32_t x, y, value, temp;
    uint32_t last[256] = { 0 }, pointer[256];
    for (x = offset; x < end; ++x) {
        ++last[(array[x] >> shift) & 0xFF];
    }
    last[0] += offset;
    pointer[0] = offset;
    for (x = 1; x < 256; ++x) {
        pointer[x] = last[x-1];
        last[x] += last[x-1];
    }
    for (x = 0; x < 256; ++x) {
        while (pointer[x] != last[x]) {
            value = array[pointer[x]];
            y = (value >> shift) & 0xFF;
            while (x != y) {
                temp = array[pointer[y]];
                array[pointer[y]++] = value;
                value = temp;
                y = (value >> shift) & 0xFF;
            }
            array[pointer[x]++] = value;
        }
    }
    if (shift > 0) {
        shift -= 8;
        for (x = 0; x < 256; ++x) {
            temp = x > 0 ? pointer[x] - pointer[x-1] : pointer[0] - offset;
            if (temp > 64) {
                gauged_sort_radix(array, pointer[x] - temp, pointer[x], shift);
            } else if (temp > 1) {
                gauged_sort_insertion(array, pointer[x] - temp, pointer[x]);
            }
        }
    }
}

#if !defined(WIN32) && !defined(_WIN32)
static inline void gauged_sort_merge_buffer(size_t n, uint32_t * restrict out,
        size_t nl, uint32_t * restrict inl,
        size_t nu, uint32_t * restrict inu) {
    size_t pos = 0, i = 0, j = 0;
    while (pos < n) {
        if (j >= nu || (i < nl && inl[i] <= inu[j])) {
            out[pos++] = inl[i++];
        } else {
            out[pos++] = inu[j++];
        }
    }
}

static void *gauged_sort_merge(void *data) {
    gauged_mergesort_t *params = data;
    if (params->depth == GAUGED_SORT_MERGESORT_MAX_DEPTH ||
            params->size <= GAUGED_SORT_RADIXSORT_MAX) {
        if ((params->depth & 1) == 0) {
            gauged_sort_radix(params->buffer, 0, params->size, 24);
            // FIXME: I think I've derped my mergesort impl
            memcpy(params->output, params->buffer, params->size * sizeof(uint32_t));
        } else {
            gauged_sort_radix(params->output, 0, params->size, 24);
        }
    } else {
        pthread_t t1, t2;
        size_t split = params->size / 2;
        gauged_mergesort_t partition1 = {
            params->output,
            params->buffer,
            split,
            params->depth + 1
        };
        pthread_create(&t1, NULL, gauged_sort_merge, &partition1);
        gauged_mergesort_t partition2 = {
            params->output + split,
            params->buffer + split,
            params->size - split,
            params->depth + 1
        };
        pthread_create(&t2, NULL, gauged_sort_merge, &partition2);
        pthread_join(t1, NULL);
        pthread_join(t2, NULL);
        gauged_sort_merge_buffer(params->size, params->output,
            partition1.size, partition1.output,
            partition2.size, partition2.output);
    }
    return NULL;
}
#endif

GAUGED_EXPORT uint32_t *gauged_sort(uint32_t *array, size_t length) {
    if (length <= GAUGED_SORT_INSERTIONSORT_MAX) {
        gauged_sort_insertion(array, 0, length);
#if !defined(WIN32) && !defined(_WIN32)
    } else if (length <= GAUGED_SORT_RADIXSORT_MAX) {
        gauged_sort_radix(array, 0, length, 24);
    } else {
        uint32_t *output = malloc(length * sizeof(uint32_t));
        if (!output) {
            return NULL;
        }
        gauged_mergesort_t params = {
            array,
            output,
            length,
            0
        };
        gauged_sort_merge(&params);
        array = output;
    }
#else
    } else {
        gauged_sort_radix(array, 0, length, 24);
    }
#endif
    return array;
}
