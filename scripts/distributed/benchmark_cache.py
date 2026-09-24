"""Evict and measure file residency while a benchmark PostgreSQL is stopped.

Linux-only. Does not claim to flush storage-controller caches or bound RAM.
"""
import argparse
import ctypes
import json
import mmap
import os
from pathlib import Path


def evict(pgdata):
    root = Path(pgdata)
    if not (root / 'PG_VERSION').is_file():
        raise RuntimeError('Not a PostgreSQL data directory')
    if (root / 'postmaster.pid').exists():
        raise RuntimeError('Stop PostgreSQL before evicting database files')
    # Refuse unhandled external tablespaces rather than asserting cold caches.
    if any((root / 'pg_tblspc').iterdir()):
        raise RuntimeError('External tablespaces require explicit cache preparation')
    os.sync()
    libc = ctypes.CDLL(None, use_errno=True)
    libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
    files = pages = resident = 0
    for path in sorted(root.rglob('*')):
        if not path.is_file():
            continue
        if path.is_symlink():
            raise RuntimeError(f'Unmanaged symlink: {path}')
        with path.open('rb') as stream:
            size = os.fstat(stream.fileno()).st_size
            os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
            if size:
                with mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_COPY) as mapping:
                    count = (size + mmap.PAGESIZE - 1) // mmap.PAGESIZE
                    vector = (ctypes.c_ubyte * count)()
                    anchor = ctypes.c_char.from_buffer(mapping)
                    address = ctypes.addressof(anchor)
                    del anchor
                    if libc.mincore(address, size, vector):
                        raise OSError(ctypes.get_errno(), f'mincore failed: {path}')
                    resident += sum(v & 1 for v in vector)
                    pages += count
            files += 1
    result = dict(files=files, pages=pages, resident_pages=resident,
                  policy='stopped_pg_fadvise_verified_mincore', controller_cache='uncontrolled')
    # A few partially filled metadata pages may remain; require <= 0.1% overall.
    if resident > max(8, pages * .001):
        raise RuntimeError(f'Database files remain resident: {result}')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('pgdata')
    print(json.dumps(evict(parser.parse_args().pgdata)))
