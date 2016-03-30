/*!
 * Gauged - https://github.com/chriso/gauged
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "sort.h"

static inline void gauged_sort_insertion(uint32_t *array, uint32_t offset,
                                         size_t end) {
    uint32_t x, y, temp;
    for (x = offset; x < end; ++x) {
        for (y = x; y > offset && array[y - 1] > array[y]; y--) {
            temp = array[y];
            array[y] = array[y - 1];
            array[y - 1] = temp;
        }
    }
}

static void gauged_sort_radix(uint32_t *array, uint32_t offset, size_t end,
                              uint32_t shift) {
    uint32_t x, y, value, temp;
    uint32_t last[256] = {0}, pointer[256];
    for (x = offset; x < end; ++x) {
        ++last[(array[x] >> shift) & 0xFF];
    }
    last[0] += offset;
    pointer[0] = offset;
    for (x = 1; x < 256; ++x) {
        pointer[x] = last[x - 1];
        last[x] += last[x - 1];
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
            temp = x > 0 ? pointer[x] - pointer[x - 1] : pointer[0] - offset;
            if (temp > 64) {
                gauged_sort_radix(array, pointer[x] - temp, pointer[x], shift);
            } else if (temp > 1) {
                gauged_sort_insertion(array, pointer[x] - temp, pointer[x]);
            }
        }
    }
}

GAUGED_EXPORT uint32_t *gauged_sort(uint32_t *array, size_t length) {
    if (length <= GAUGED_SORT_INSERTIONSORT_MAX) {
        gauged_sort_insertion(array, 0, length);
    } else {
        gauged_sort_radix(array, 0, length, 24);
    }
    return array;
}
