// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTATION ──────────────────────────────────────────────────────────

// Write an object to the store.
//
// Object format on disk:
//   "<type> <size>\0<data>"
//   where <type> is "blob", "tree", or "commit"
//   and <size> is the decimal string of the data length
//
// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // 1. Determine the type string
    const char *type_str;
    switch (type) {
        case OBJ_BLOB:   type_str = "blob";   break;
        case OBJ_TREE:   type_str = "tree";   break;
        case OBJ_COMMIT: type_str = "commit"; break;
        default: return -1;
    }

    // 2. Build the header: "<type> <size>\0"
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    // header_len does NOT include the null terminator, but we need to include it
    // in the object. header_len+1 bytes total (including '\0').

    // 3. Build full object = header (including '\0') + data
    size_t full_len = (size_t)(header_len + 1) + len;
    uint8_t *full = malloc(full_len);
    if (!full) return -1;
    memcpy(full, header, header_len + 1); // +1 includes the '\0'
    memcpy(full + header_len + 1, data, len);

    // 4. Compute SHA-256 of the full object
    ObjectID id;
    compute_hash(full, full_len, &id);

    // 5. Deduplication — if already stored, just return the hash
    if (object_exists(&id)) {
        if (id_out) *id_out = id;
        free(full);
        return 0;
    }

    // 6. Create shard directory (.pes/objects/XX/)
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);

    char shard_dir[512];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_dir, 0755); // Ignore error — may already exist

    // 7. Build final path and temp path
    char final_path[512];
    snprintf(final_path, sizeof(final_path), "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);

    char tmp_path[520];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", final_path);

    // 8. Write to temp file
    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        free(full);
        return -1;
    }

    ssize_t written = write(fd, full, full_len);
    free(full);
    if (written < 0 || (size_t)written != full_len) {
        close(fd);
        unlink(tmp_path);
        return -1;
    }

    // 9. fsync temp file to ensure data reaches disk
    fsync(fd);
    close(fd);

    // 10. Atomically rename temp -> final
    if (rename(tmp_path, final_path) != 0) {
        unlink(tmp_path);
        return -1;
    }

    // 11. fsync the shard directory to persist the rename
    int dir_fd = open(shard_dir, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    // 12. Return hash
    if (id_out) *id_out = id;
    return 0;
}

// Read an object from the store.
//
// Returns 0 on success, -1 on error (file not found, corrupt, etc.).
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // 1. Build the file path
    char path[512];
    object_path(id, path, sizeof(path));

    // 2. Open and read the entire file
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (file_size <= 0) {
        fclose(f);
        return -1;
    }

    uint8_t *raw = malloc((size_t)file_size);
    if (!raw) {
        fclose(f);
        return -1;
    }

    if (fread(raw, 1, (size_t)file_size, f) != (size_t)file_size) {
        fclose(f);
        free(raw);
        return -1;
    }
    fclose(f);

    // 3. Verify integrity: recompute SHA-256 and compare to the filename hash
    ObjectID computed;
    compute_hash(raw, (size_t)file_size, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(raw);
        return -1; // Corrupted object
    }

    // 4. Parse the header — find the '\0' separating header from data
    uint8_t *null_byte = memchr(raw, '\0', (size_t)file_size);
    if (!null_byte) {
        free(raw);
        return -1;
    }

    // 5. Parse type string
    if (strncmp((char *)raw, "blob ", 5) == 0) {
        if (type_out) *type_out = OBJ_BLOB;
    } else if (strncmp((char *)raw, "tree ", 5) == 0) {
        if (type_out) *type_out = OBJ_TREE;
    } else if (strncmp((char *)raw, "commit ", 7) == 0) {
        if (type_out) *type_out = OBJ_COMMIT;
    } else {
        free(raw);
        return -1;
    }

    // 6. Extract data portion (everything after '\0')
    uint8_t *data_start = null_byte + 1;
    size_t data_len = (size_t)file_size - (size_t)(data_start - raw);

    uint8_t *data_copy = malloc(data_len + 1); // +1 for safety null-term
    if (!data_copy) {
        free(raw);
        return -1;
    }
    memcpy(data_copy, data_start, data_len);
    data_copy[data_len] = '\0';

    free(raw);

    if (data_out) *data_out = data_copy;
    else free(data_copy);
    if (len_out) *len_out = data_len;

    return 0;
}
