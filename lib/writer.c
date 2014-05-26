/*!
 * Gauged - https://github.com/chriso/gauged
 * Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
 */

#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <ctype.h>

#include "writer.h"

static inline void gauged_writer_hash_node_free(gauged_writer_hash_node_t *node) {
    gauged_map_free(node->map);
    gauged_array_free(node->array);
    free(node->key);
    free(node);
}

static inline gauged_writer_hash_t *gauged_writer_hash_new(size_t size) {
    gauged_writer_hash_t *hash = calloc(1, sizeof(gauged_writer_hash_t));
    if (!hash) {
        goto error;
    }
    hash->nodes = calloc(size, sizeof(gauged_writer_hash_node_t *));
    if (!hash->nodes) {
        goto error;
    }
    hash->size = size;
    return hash;
error:
    if (hash) free(hash);
    return NULL;
}

static inline void gauged_writer_hash_free(gauged_writer_hash_t *hash) {
    gauged_writer_hash_node_t *node;
    for (uint32_t i = 0; i < hash->size; i++) {
        if (hash->nodes[i]) {
            node = hash->nodes[i];
            gauged_writer_hash_node_free(node);
        }
    }
    free(hash->nodes);
    free(hash);
}

static int gauged_writer_hash_rehash(gauged_writer_hash_t *);

static inline gauged_writer_hash_node_t *gauged_writer_hash_get(gauged_writer_hash_t *hash,
        uint32_t namespace_, const char *key, uint32_t seed) {
    size_t mask = hash->size - 1;
    size_t hash_key = seed & mask;
    gauged_writer_hash_node_t *node;
    for (uint32_t j = 1; j < hash->size; j++) {
        if (!hash->nodes[hash_key]) {
            break;
        }
        node = hash->nodes[hash_key];
        if (node->seed == seed && node->namespace_ == namespace_ &&
                !strcmp(key, node->key)) {
            return node;
        }
        hash_key = (seed + j * j) & mask;
    }
    return NULL;
}

static inline int gauged_writer_hash_insert(gauged_writer_hash_t *hash, gauged_writer_hash_node_t *node) {
    if (hash->count > hash->size / 2) {
        gauged_writer_hash_rehash(hash);
    }
    size_t mask = hash->size - 1;
    size_t hash_key = node->seed & mask;
    for (uint32_t j = 1; j < hash->size; j++) {
        if (!hash->nodes[hash_key]) {
            hash->nodes[hash_key] = node;
            hash->count++;
            if (hash->tail) {
                hash->tail->next = node;
                hash->tail = node;
            } else {
                hash->head = hash->tail = node;
            }
            return GAUGED_OK;
        }
        hash_key = (node->seed + j * j) & mask;
    }
    if (!gauged_writer_hash_rehash(hash)) {
        return GAUGED_ERROR;
    }
    return gauged_writer_hash_insert(hash, node);
}

static inline int gauged_writer_hash_rehash(gauged_writer_hash_t *hash) {
    size_t current_size = hash->size;
    size_t current_count = hash->count;
    gauged_writer_hash_node_t *current_head = hash->head, *current_tail = hash->tail;
    gauged_writer_hash_node_t *node, **current_nodes = hash->nodes;
    hash->size *= 2;
    hash->count = 0;
    hash->head = hash->tail = NULL;
    hash->nodes = calloc(hash->size, sizeof(gauged_writer_hash_node_t *));
    if (!hash->nodes) {
        goto error;
    }
    for (size_t i = 0; i < current_size; i++) {
        node = current_nodes[i];
        if (node && !gauged_writer_hash_insert(hash, node)) {
            goto error;
        }
    }
    free(current_nodes);
    return GAUGED_OK;
error:
    if (hash->nodes) {
        free(hash->nodes);
    }
    hash->head = current_head;
    hash->tail = current_tail;
    hash->size = current_size;
    hash->nodes = current_nodes;
    hash->count = current_count;
    return GAUGED_ERROR;
}

GAUGED_EXPORT gauged_writer_t *gauged_writer_new(size_t max_key) {
    gauged_writer_t *writer = malloc(sizeof(gauged_writer_t));
    if (!writer) {
        return NULL;
    }
    writer->pending = gauged_writer_hash_new(GAUGED_WRITER_HASH_INITIAL);
    writer->buffer = malloc(GAUGED_WRITER_MAX_PAIRS * 2 * sizeof(char *));
    writer->copy = malloc(GAUGED_WRITER_MAX_QUERY * sizeof(char));
    if (!writer->pending || !writer->buffer || !writer->copy) {
        goto error;
    }
    writer->max_key = max_key;
    writer->buffer_size = 0;
    return writer;
error:
    if (writer->pending) {
        gauged_writer_hash_free(writer->pending);
    }
    if (writer->buffer) free(writer->buffer);
    if (writer->copy) free(writer->copy);
    free(writer);
    return NULL;
}

GAUGED_EXPORT void gauged_writer_free(gauged_writer_t *writer) {
    gauged_writer_hash_free(writer->pending);
    free(writer->buffer);
    free(writer->copy);
    free(writer);
}

GAUGED_EXPORT int gauged_writer_emit_pairs(gauged_writer_t *writer, uint32_t namespace_,
        const char *pairs, uint32_t *data_points) {
    gauged_writer_parse_query(writer, pairs);
    char *key, *value, *end_ptr;
    float float_value;
    int status;
    if (!writer->buffer_size) {
        return GAUGED_OK;
    }
    for (size_t i = 0; i <= writer->buffer_size - 2; ) {
        key = writer->buffer[i++];
        value = writer->buffer[i++];
        float_value = strtof(value, &end_ptr);
        if (value == end_ptr) { // NaN?
            continue;
        }
        status = gauged_writer_emit(writer, namespace_, key, float_value);
        switch (status) {
        case GAUGED_OK:
            *data_points += 1;
            break;
        case GAUGED_KEY_OVERFLOW:
            break;
        case GAUGED_ERROR:
            return GAUGED_ERROR;
        }
    }
    return GAUGED_OK;
}

GAUGED_EXPORT int gauged_writer_emit(gauged_writer_t *writer, uint32_t namespace_, const char *key, float value) {
    size_t key_len = strlen(key) + 1;
    if (writer->max_key && key_len > writer->max_key) {
        return GAUGED_KEY_OVERFLOW;
    }
    gauged_hash_init(&writer->hash);
    gauged_hash_update(&writer->hash, (char*)&namespace_, sizeof(uint32_t));
    gauged_hash_update(&writer->hash, key, key_len);
    uint32_t seed = gauged_hash_digest(&writer->hash);
    //See if the hash node already exists
    gauged_writer_hash_node_t *lookup = gauged_writer_hash_get(writer->pending,
        namespace_, key, seed);
    if (lookup) {
        if (!gauged_array_append(lookup->array, value)) {
            return GAUGED_ERROR;
        }
        if (writer->pending->array_tail != lookup && lookup->array_next == NULL) {
            if (writer->pending->array_tail) {
                writer->pending->array_tail->array_next = lookup;
                writer->pending->array_tail = lookup;
            } else {
                writer->pending->array_head = writer->pending->array_tail = lookup;
            }
        }
        return GAUGED_OK;
    }
    //Not found, create a new hash node
    gauged_writer_hash_node_t *node = malloc(sizeof(gauged_writer_hash_node_t));
    if (!node) {
        return GAUGED_ERROR;
    }
    node->array = gauged_array_new();
    node->map = gauged_map_new();
    node->key = malloc(sizeof(char) * key_len);
    if (!node->key || !node->array || !node->map) {
        goto error;
    }
    if (!gauged_array_append(node->array, value)) {
        goto error;
    }
    memcpy(node->key, key, key_len);
    node->namespace_ = namespace_;
    node->seed = seed;
    node->next = node->array_next = NULL;
    if (!gauged_writer_hash_insert(writer->pending, node)) {
        goto error;
    }
    if (writer->pending->array_tail) {
        writer->pending->array_tail->array_next = node;
        writer->pending->array_tail = node;
    } else {
        writer->pending->array_head = writer->pending->array_tail = node;
    }
    return GAUGED_OK;
error:
    if (node->array) gauged_array_free(node->array);
    if (node->map) gauged_map_free(node->map);
    if (node->key) free(node->key);
    free(node);
    return GAUGED_ERROR;
}

GAUGED_EXPORT int gauged_writer_flush_arrays(gauged_writer_t *writer, uint32_t offset) {
    gauged_writer_hash_node_t *node, *next;
    gauged_writer_hash_t *hash = writer->pending;
    for (node = hash->array_head; node; node = node->array_next) {
        if (!gauged_map_append(node->map, offset, node->array)) {
            goto error;
        }
        node->array->length = 0;
    }
    for (node = hash->array_head; node; node = next) {
        node->array->length = 0;
        next = node->array_next;
        node->array_next = NULL;
    }
    hash->array_head = hash->array_tail = NULL;
    return GAUGED_OK;
error:
    return GAUGED_ERROR;
}

GAUGED_EXPORT int gauged_writer_flush_maps(gauged_writer_t *writer, bool soft) {
    gauged_writer_hash_node_t *node;
    if (soft) {
        for (node = writer->pending->head; node; node = node->next) {
            node->map->length = 0;
        }
    } else {
        gauged_writer_hash_t *hash = writer->pending;
        for (size_t i = 0; i < hash->size; i++) {
            if (hash->nodes[i]) {
                node = hash->nodes[i];
                if (soft) {
                    node->map->length = 0;
                } else {
                    gauged_writer_hash_node_free(node);
                    hash->nodes[i] = NULL;
                }
            }
        }
        hash->head = hash->tail = NULL;
        hash->count = 0;
    }
    return GAUGED_OK;
}

static inline char gauged_writer_hex_decode(char c) {
    return isdigit(c) ? c - '0' : tolower(c) - 'a' + 10;
}

static void gauged_writer_url_decode(char *dst, const char *src, size_t len) {
    while (len-- && (*dst = *src++)) {
        if (*dst == '+') {
            *dst = ' ';
        } else if (*dst == '%' && src[0] && src[1] && len >= 2) {
            *dst = (gauged_writer_hex_decode(src[0]) << 4) | gauged_writer_hex_decode(src[1]);
            src += 2;
            len -= 2;
        }
        dst++;
    }
    *dst = '\0';
}

GAUGED_EXPORT void gauged_writer_parse_query(gauged_writer_t *writer, const char *query) {
    writer->buffer_size = 0;
    if (!query) {
        return;
    }
    size_t query_len = strlen(query) + 1;
    if (query_len >= 2 && query[query_len - 2] == '\n') {
        query_len--;
    }
    if (query_len > GAUGED_WRITER_MAX_QUERY) {
        query_len = GAUGED_WRITER_MAX_QUERY;
    }
    memcpy(writer->copy, query, query_len);
    writer->copy[query_len - 1] = '\0';
    char *str = writer->copy;
    char c;
    size_t key_width = 0, value_width = 0, pos = 0;
    bool in_key = true;
    char *key_start = str, *value_start = NULL;
    while (1) {
        c = *str++;
        if (c == '&' || c == '\0') {
            if (key_width) {
                 key_start[key_width] = '\0';
                 gauged_writer_url_decode(key_start, key_start, key_width);
                 if (value_start) {
                     value_start[value_width] = '\0';
                     gauged_writer_url_decode(value_start, value_start, value_width);
                     writer->buffer[pos++] = key_start;
                     writer->buffer[pos++] = value_start;
                     writer->buffer_size += 2;
                     if (writer->buffer_size == GAUGED_WRITER_MAX_PAIRS * 2) {
                         break;
                     }
                 }
            }
            if (c == '\0') {
                break;
            }
            key_width = value_width = 0;
            value_start = NULL;
            key_start = str;
            in_key = 1;
        } else if (in_key) {
            if (c == '=') {
                in_key = 0;
                value_start = str;
            } else {
                key_width++;
            }
        } else {
            value_width++;
        }
    }
}

GAUGED_EXPORT void init_gauged() {
}
