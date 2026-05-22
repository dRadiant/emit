"""Reference Python reader for emit's state.snap files.

Decodes a MutableStore's records without any emit-sdk dependency. Usage
against the ERC20 example:

    python3 reader.py /path/to/erc20/data/state.snap

The schema (number of mutables, number of immutables, per-slot record size,
field layout) is comptime-known in the indexer. Readers consume that as
external knowledge — there is no per-slot descriptor on disk. See
docs/entity-format.md for the byte layout.
"""

import struct
import sys
from dataclasses import dataclass

# ERC20 schema. Slot 0: Account = id [20]u8 + balance u256.
# Slot 1: Allowance = id [40]u8 + value u256.
NUM_MUTABLE = 2
NUM_IMMUTABLE = 2  # Transfer, Approval -- counts only; records live in events.dat files.
ACCOUNT_RECORD_SIZE = 20 + 32

@dataclass
class Account:
    id: bytes      # 20 bytes (BE since it's the primary key)
    balance: int   # u256 LE

def read_state_snap(path: str):
    with open(path, "rb") as f:
        buf = f.read()

    magic, version, cursor = struct.unpack_from("<8s I Q", buf, 0)
    assert magic == b"EMITSTAT", f"bad magic: {magic!r}"
    assert version == 1, f"unsupported version: {version}"

    pos = 8 + 4 + 8
    mutable_bytes = struct.unpack_from(f"<{NUM_MUTABLE}Q", buf, pos)
    pos += NUM_MUTABLE * 8
    immutable_counts = struct.unpack_from(f"<{NUM_IMMUTABLE}Q", buf, pos)
    pos += NUM_IMMUTABLE * 8

    # Body: slabs concatenated in slot order. Slot 0 = Account slab.
    account_slab = buf[pos : pos + mutable_bytes[0]]
    assert len(account_slab) % ACCOUNT_RECORD_SIZE == 0
    accounts = []
    for i in range(0, len(account_slab), ACCOUNT_RECORD_SIZE):
        record = account_slab[i : i + ACCOUNT_RECORD_SIZE]
        # First field is BE per the format spec (sorted-by-bytes order).
        id_bytes = record[0:20]
        # u256 is little-endian for the data fields.
        balance = int.from_bytes(record[20:52], "little")
        accounts.append(Account(id=id_bytes, balance=balance))

    return cursor, accounts, immutable_counts

if __name__ == "__main__":
    cursor, accounts, immutable_counts = read_state_snap(sys.argv[1])
    print(f"cursor: {cursor}")
    print(f"accounts: {len(accounts)}")
    print(f"immutable counts (Transfer, Approval): {immutable_counts}")
    if accounts:
        a = accounts[0]
        print(f"first account: id=0x{a.id.hex()} balance={a.balance}")
