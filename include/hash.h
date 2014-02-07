/*!
 * Gauged
 * https://github.com/chriso/gauged (MIT Licensed)
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

/**
 * xxHash - Fast Hash algorithm
 * Copyright (C) 2012-2013, Yann Collet.
 * BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)
 */

#ifndef GAUGED_HASH_H_
#define GAUGED_HASH_H_

#include <stdint.h>

/**
 * Hash using the XXHash Strong algorithm.
 */

typedef struct gauged_xxhash_s {
    unsigned int seed;
    unsigned int v1;
    unsigned int v2;
    unsigned int v3;
    unsigned int v4;
    unsigned long long total_len;
    char memory[16];
    int memsize;
} gauged_xxhash_t;

void gauged_hash_init(gauged_xxhash_t *hash);

void gauged_hash_update(gauged_xxhash_t *hash, const char *str, size_t length);

uint32_t gauged_hash_digest(gauged_xxhash_t *hash);

#endif
