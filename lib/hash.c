/*!
 * Gauged - https://github.com/chriso/gauged
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

/**
 * xxHash - Fast Hash algorithm
 * Copyright (C) 2012-2013, Yann Collet.
 * BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)
 */

#include <string.h>
#include <math.h>

#include "common.h"
#include "hash.h"

#define PRIME32_1 2654435761U
#define PRIME32_2 2246822519U
#define PRIME32_3 3266489917U
#define PRIME32_4  668265263U
#define PRIME32_5  374761393U

#define GAUGED_HASH_SEED 5132

#define XXH_LE32(p) *(unsigned int*)(p)
#define XXH_rotl32(x, r) ((x << r) | (x >> (32 - r)))

GAUGED_EXPORT void gauged_hash_init(gauged_xxhash_t *hash) {
    hash->seed = GAUGED_HASH_SEED;
    hash->v1 = GAUGED_HASH_SEED + PRIME32_1 + PRIME32_2;
    hash->v2 = GAUGED_HASH_SEED + PRIME32_2;
    hash->v3 = GAUGED_HASH_SEED + 0;
    hash->v4 = GAUGED_HASH_SEED - PRIME32_1;
    hash->total_len = 0;
    hash->memsize = 0;
}

GAUGED_EXPORT void gauged_hash_update(gauged_xxhash_t *hash, const char *str, size_t length) {
    const unsigned char* p = (const unsigned char *)str;
    const unsigned char* const bEnd = p + length;
    hash->total_len += length;
    if (hash->memsize + length < 16) {
        memcpy(hash->memory + hash->memsize, str, length);
        hash->memsize += length;
        return;
    }
    if (hash->memsize) {
        memcpy(hash->memory + hash->memsize, str, 16 - hash->memsize);
        {
            const unsigned int *p32 = (const unsigned int *) hash->memory;
            hash->v1 += XXH_LE32(p32) * PRIME32_2; hash->v1 = XXH_rotl32(hash->v1, 13);
            hash->v1 *= PRIME32_1; p32++;
            hash->v2 += XXH_LE32(p32) * PRIME32_2; hash->v2 = XXH_rotl32(hash->v2, 13);
            hash->v2 *= PRIME32_1; p32++;
            hash->v3 += XXH_LE32(p32) * PRIME32_2; hash->v3 = XXH_rotl32(hash->v3, 13);
            hash->v3 *= PRIME32_1; p32++;
            hash->v4 += XXH_LE32(p32) * PRIME32_2; hash->v4 = XXH_rotl32(hash->v4, 13);
            hash->v4 *= PRIME32_1; p32++;
        }
        p += 16-hash->memsize;
        hash->memsize = 0;
    }
    {
        const unsigned char* const limit = bEnd - 16;
        unsigned int v1 = hash->v1;
        unsigned int v2 = hash->v2;
        unsigned int v3 = hash->v3;
        unsigned int v4 = hash->v4;
        while (p <= limit) {
            v1 += XXH_LE32(p) * PRIME32_2; v1 = XXH_rotl32(v1, 13); v1 *= PRIME32_1; p+=4;
            v2 += XXH_LE32(p) * PRIME32_2; v2 = XXH_rotl32(v2, 13); v2 *= PRIME32_1; p+=4;
            v3 += XXH_LE32(p) * PRIME32_2; v3 = XXH_rotl32(v3, 13); v3 *= PRIME32_1; p+=4;
            v4 += XXH_LE32(p) * PRIME32_2; v4 = XXH_rotl32(v4, 13); v4 *= PRIME32_1; p+=4;
        }
        hash->v1 = v1;
        hash->v2 = v2;
        hash->v3 = v3;
        hash->v4 = v4;
    }
    if (p < bEnd) {
        memcpy(hash->memory, p, bEnd-p);
        hash->memsize = (int)(bEnd - p);
    }
}

GAUGED_EXPORT uint32_t gauged_hash_digest(gauged_xxhash_t *hash) {
    unsigned char *p = (unsigned char *) hash->memory;
    unsigned char *bEnd = (unsigned char *) hash->memory + hash->memsize;
    uint32_t h32;
    if (hash->total_len >= 16) {
        h32 = XXH_rotl32(hash->v1, 1) + XXH_rotl32(hash->v2, 7) +
            XXH_rotl32(hash->v3, 12) + XXH_rotl32(hash->v4, 18);
    } else {
        h32  = hash->seed + PRIME32_5;
    }
    h32 += (uint32_t) hash->total_len;
    while (p <= bEnd - 4) {
        h32 += XXH_LE32(p) * PRIME32_3;
        h32 = XXH_rotl32(h32, 17) * PRIME32_4 ;
        p += 4;
    }
    while (p < bEnd) {
        h32 += (*p) * PRIME32_5;
        h32 = XXH_rotl32(h32, 11) * PRIME32_1 ;
        p++;
    }
    h32 ^= h32 >> 15;
    h32 *= PRIME32_2;
    h32 ^= h32 >> 13;
    h32 *= PRIME32_3;
    h32 ^= h32 >> 16;
    return h32;
}
