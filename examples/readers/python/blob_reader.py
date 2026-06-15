"""Reference Python reader for a blob-bearing entity, no emit-sdk dependency.

Decodes the ENS example's `Registration` store, resolving each `name` field
(a variable-length blob) against `registrations.blobs.dat`. Usage:

    python3 blob_reader.py /path/to/ens/data/entity

The schema (record layout, which field is the blob) is comptime-known in the
indexer; readers supply it as external knowledge. See
examples/readers/entity-format.md for the byte layout.
"""

import struct
import sys

# ENS Registration schema (mutable, slot 0; blob slot 0):
#   id [32] BE key | owner [20] | expires u256 LE | name BlobRef(8)
REC = 32 + 20 + 32 + 8
NAME_OFF = 32 + 20 + 32  # byte offset of the name BlobRef within a record


def read(entity_dir: str):
    snap = open(f"{entity_dir}/state.snap", "rb").read()
    assert snap[:8] == b"EMITSTAT", snap[:8]
    version, cursor = struct.unpack_from("<IQ", snap, 8)
    assert version == 2, f"expected version 2 (blob schema), got {version}"

    # Header: magic(8) version(4) cursor(8) mutable_bytes[1](8)
    #         immutable_counts[0]() blob_bytes[1](8). Schema-known counts.
    mut_bytes = struct.unpack_from("<Q", snap, 20)[0]
    blob_bytes = struct.unpack_from("<Q", snap, 28)[0]
    header = 8 + 4 + 8 + 1 * 8 + 0 * 8 + 1 * 8
    body = snap[header:header + mut_bytes]

    blobs = open(f"{entity_dir}/registrations.blobs.dat", "rb").read()
    assert blobs[:8] == b"EMITBLOB"

    out = []
    for i in range(mut_bytes // REC):
        o = i * REC
        label = body[o:o + 32]
        owner = body[o + 32:o + 52]
        ref = struct.unpack_from("<Q", body, o + NAME_OFF)[0]
        offset, length = ref & ((1 << 40) - 1), ref >> 40
        name = blobs[offset:offset + length].decode("utf-8", "replace")
        out.append((label.hex(), "0x" + owner.hex(), name))
    return cursor, blob_bytes, out


if __name__ == "__main__":
    cursor, blob_bytes, regs = read(sys.argv[1])
    print(f"cursor: {cursor}")
    print(f"registrations: {len(regs)}")
    print(f"blob payload bytes: {blob_bytes}")
    for label, owner, name in regs[:10]:
        print(f"  {label[:12]}… owner={owner[:10]}… name={name!r}")
