/*!
 * Gauged
 * https://github.com/chriso/gauged (MIT Licensed)
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#ifndef GAUGED_WRITER_H_
#define GAUGED_WRITER_H_

#include <stdbool.h>

#include "map.h"
#include "hash.h"

/**
 * The writer is responsible for normalising gauges and their values.
 */

typedef struct gauged_writer_hash_node_s {
    char *key;
    gauged_map_t *map;
    gauged_array_t *array;
    uint32_t namespace_;
    uint32_t seed;
    struct gauged_writer_hash_node_s *next;
    struct gauged_writer_hash_node_s *array_next;
} gauged_writer_hash_node_t;

typedef struct gauged_writer_hash_s {
    gauged_writer_hash_node_t **nodes;
    size_t size;
    size_t count;
    gauged_writer_hash_node_t *head;
    gauged_writer_hash_node_t *tail;
    gauged_writer_hash_node_t *array_head;
    gauged_writer_hash_node_t *array_tail;
} gauged_writer_hash_t;

typedef struct gauged_writer_s {
    gauged_writer_hash_t *pending;
    size_t max_key;
    char *copy;
    char **buffer;
    size_t buffer_size;
    gauged_xxhash_t hash;
} gauged_writer_t;

/**
 * The initial size of the hash tables.
 */

#define GAUGED_WRITER_HASH_INITIAL 16

/**
 * gauged_writer_parse parameters
 */

#define GAUGED_WRITER_MAX_QUERY 32768
#define GAUGED_WRITER_MAX_PAIRS 4096

/**
 * Create a new writer.
 */

gauged_writer_t *gauged_writer_new(size_t max_key);

/**
 * Free the specified writer.
 */

void gauged_writer_free(gauged_writer_t *);

/**
 * Emit a namespace and key/value pair to the writer.
 */

#define GAUGED_KEY_OVERFLOW -1

int gauged_writer_emit(gauged_writer_t *, uint32_t namespace_,
    const char *key, float value);

/**
 * Emit multiple key/value pairs as a urlencoded string.
 */

int gauged_writer_emit_pairs(gauged_writer_t *, uint32_t namespace_,
    const char *pairs, uint32_t *data_points);

/**
 * Parse a query string into the writer buffer.
 */

void gauged_writer_parse_query(gauged_writer_t *writer, const char *);

/**
 * Flush pending arrayets.
 */

int gauged_writer_flush_arrays(gauged_writer_t *writer, uint32_t offset);

/**
 * Flush pending maps.
 */

int gauged_writer_flush_maps(gauged_writer_t *writer, bool soft);

#endif
