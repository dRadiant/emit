/* Reference C reader for emit's state.snap files. Compiles with any C99:
 *
 *   cc -O2 -o reader reader.c
 *   ./reader /path/to/data/entity/state.snap
 *
 * Prints the cursor and the per-slot mutable slab sizes / immutable record
 * counts. Decoding entity records requires knowledge of the per-slot
 * schema, which the indexer holds at comptime; see examples/readers/entity-format.md.
 *
 * This reader assumes the ERC20 schema: 2 mutables (Account, Allowance)
 * and 2 immutables (Transfer, Approval). Other indexers: adjust the
 * NUM_MUTABLE / NUM_IMMUTABLE constants.
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define NUM_MUTABLE 2
#define NUM_IMMUTABLE 2

int main(int argc, char **argv) {
    if (argc != 2) { fprintf(stderr, "usage: %s <state.snap>\n", argv[0]); return 1; }

    int fd = open(argv[1], O_RDONLY);
    if (fd < 0) { perror("open"); return 1; }
    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); return 1; }
    const uint8_t *buf = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (buf == MAP_FAILED) { perror("mmap"); return 1; }

    if (memcmp(buf, "EMITSTAT", 8) != 0) { fprintf(stderr, "bad magic\n"); return 1; }
    uint32_t version;
    memcpy(&version, buf + 8, 4);
    if (version != 1) { fprintf(stderr, "unsupported version %u\n", version); return 1; }

    uint64_t cursor;
    memcpy(&cursor, buf + 12, 8);

    uint64_t mutable_bytes[NUM_MUTABLE];
    uint64_t immutable_counts[NUM_IMMUTABLE];
    memcpy(mutable_bytes, buf + 20, NUM_MUTABLE * 8);
    memcpy(immutable_counts, buf + 20 + NUM_MUTABLE * 8, NUM_IMMUTABLE * 8);

    printf("cursor:           %llu\n", (unsigned long long)cursor);
    for (int i = 0; i < NUM_MUTABLE; i++)
        printf("mutable[%d] bytes: %llu\n", i, (unsigned long long)mutable_bytes[i]);
    for (int i = 0; i < NUM_IMMUTABLE; i++)
        printf("immutable[%d] count: %llu\n", i, (unsigned long long)immutable_counts[i]);

    munmap((void *)buf, st.st_size);
    close(fd);
    return 0;
}
