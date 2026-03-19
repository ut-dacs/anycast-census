#!/usr/bin/env python3
"""Build per-prefix anycast census history files.

Generates 256 binary files, one per IPv4 first octet:
  data/history/{0..255}.bin

Each file covers all 65,536 possible /24 prefixes within that /8.
Row i (0-indexed within the file) = prefix {octet}.{i>>8}.{i&0xff}.0/24

File format
-----------
Bytes 0-3   Magic 'ACPH'
Byte 4      Version = 1
Bytes 5-6   num_days (uint16 LE) — count of parquet dates parsed
Bytes 7-15  Reserved (zeros)
Bytes 16+   65,536 rows × ceil(num_days / 4) bytes
            Each row = one /24 prefix's full presence history.
            Each byte encodes 4 consecutive days (2 bits each):
              bits 7-6 = day group*4 + 0   (oldest in group)
              bits 5-4 = day group*4 + 1
              bits 3-2 = day group*4 + 2
              bits 1-0 = day group*4 + 3   (newest in group)
            Per-day encoding:
              00 = not detected in that day's census
              01 = reserved (unused)
              10 = low confidence anycast  (AB_max ≤ 2, GCD_max ≤ 1)
              11 = confident anycast       (AB_max > 2  OR GCD_max > 1)

Also writes
-----------
  data/history/dates.txt  — one ISO date per line; line number = day index.
                            Only dates where a parquet exists are listed.
                            Missing days (no parquet) are simply skipped.

Usage
-----
  python3 scripts/build_prefix_history.py [output_dir]

  output_dir defaults to  <repo_root>/data/history
  The GitHub Actions deploy workflow calls this during gh-pages generation.
"""

import gzip
import struct
import sys
from pathlib import Path

import duckdb
import numpy as np

MAGIC       = b'ACPH'
VERSION     = 1
HEADER_SIZE = 16
N_PER_OCTET = 65_536   # 256 × 256 — all /24s within one /8
N_OCTETS    = 256
N_TOTAL     = N_OCTETS * N_PER_OCTET   # 16,777,216


# ── helpers ─────────────────────────────────────────────────────────────────

def find_dates(repo_root: Path) -> list[tuple[str, Path]]:
    """Return sorted (date_str, parquet_path) for every present IPv4 parquet.
    Always uses IPv4-latest.parquet for today's date if it exists.
    """
    from datetime import datetime
    dates = []
    for year_dir in sorted(repo_root.glob('[0-9][0-9][0-9][0-9]')):
        for month_dir in sorted(year_dir.glob('[0-9][0-9]')):
            for day_dir in sorted(month_dir.glob('[0-9][0-9]')):
                p = day_dir / 'IPv4.parquet'
                if p.exists():
                    date_str = f'{year_dir.name}-{month_dir.name}-{day_dir.name}'
                    dates.append((date_str, p))

    # Always use IPv4-latest.parquet for today's date if it exists
    # This ensures we include the freshest data, even if a dated parquet exists
    latest_parquet = repo_root / 'IPv4-latest.parquet'
    if latest_parquet.exists():
        today = datetime.now().strftime('%Y-%m-%d')
        # Remove today's dated parquet if it exists (we'll replace it with latest)
        dates = [(d, p) for d, p in dates if d != today]
        # Always append today with the latest parquet
        dates.append((today, latest_parquet))

    return dates


# ── main build ──────────────────────────────────────────────────────────────

def build(repo_root: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    dates = find_dates(repo_root)
    if not dates:
        print('No IPv4.parquet files found — nothing to do.', file=sys.stderr)
        sys.exit(1)

    num_days = len(dates)
    row_size = (num_days + 3) // 4   # bytes per prefix: ceil(num_days / 4)

    data_gb = N_TOTAL * row_size / 1e9
    print(f'Dates found : {num_days}')
    print(f'Row size    : {row_size} B  ({num_days} days × 2 bits packed 4/byte)')
    print(f'Allocating  : {data_gb:.2f} GB  ({N_TOTAL:,} prefixes × {row_size} B)')

    # Allocate full array in RAM.  Shape: (16 777 216, row_size), dtype uint8.
    # For ~730 days this is ≈2.9 GB — fits within the 7 GB GitHub Actions runner.
    data = np.zeros((N_TOTAL, row_size), dtype=np.uint8)

    for day_idx, (date_str, parquet_path) in enumerate(dates):
        byte_pos  = day_idx >> 2                   # which byte within the row
        bit_shift = (3 - (day_idx & 3)) << 1       # which 2-bit slot in that byte

        print(f'  [{day_idx + 1:4d}/{num_days}] {date_str}', end='', flush=True)

        con = duckdb.connect()
        df = con.execute(f"""
            SELECT
                prefix,
                GREATEST(COALESCE(AB_ICMPv4,  0),
                         COALESCE(AB_TCPv4,   0),
                         COALESCE(AB_DNSv4,   0))  AS ab_max,
                GREATEST(COALESCE(GCD_ICMPv4, 0),
                         COALESCE(GCD_TCPv4,  0))  AS gcd_max
            FROM read_parquet('{parquet_path}')
        """).df()
        con.close()

        print(f'  ({len(df):,} prefixes)', flush=True)
        if df.empty:
            continue

        # Parse "a.b.c.0/24" → linear index  a*65536 + b*256 + c
        split = df['prefix'].str.split('.', expand=True)
        a     = split[0].astype(np.int32).values
        b     = split[1].astype(np.int32).values
        c     = split[2].astype(np.int32).values     # col 3 = "0/24" — ignored
        idx   = (a << 16) | (b << 8) | c            # 0 … 16,777,215

        ab_max  = df['ab_max'].values
        gcd_max = df['gcd_max'].values

        # 11 = confident, 10 = low confidence
        state = np.where(
            (gcd_max > 1) | (ab_max > 2),
            np.uint8(0b11),
            np.uint8(0b10),
        )

        # Scatter-OR into the data array (no duplicate prefix indices within
        # a single parquet, so direct indexing works).
        data[idx, byte_pos] |= (state << bit_shift).astype(np.uint8)

    # ── clean up old uncompressed files (if they exist) ────────────────────────
    for octet in range(N_OCTETS):
        old_path = output_dir / f'{octet}.bin'
        if old_path.exists():
            old_path.unlink()

    # ── write per-octet files (gzip-compressed) ──────────────────────────────
    print(f'\nWriting {N_OCTETS} octet files to {output_dir} (gzip-compressed) …')

    header = MAGIC + bytes([VERSION]) + struct.pack('<H', num_days) + b'\x00' * 9

    for octet in range(N_OCTETS):
        out_path = output_dir / f'{octet}.bin.gz'
        lo = octet * N_PER_OCTET
        hi = lo + N_PER_OCTET

        # Combine header + data, then gzip compress
        uncompressed = header + data[lo:hi].tobytes()
        with gzip.open(out_path, 'wb', compresslevel=9) as f:
            f.write(uncompressed)

        if octet % 64 == 0:
            mb = out_path.stat().st_size / 1e6
            print(f'  {out_path}  ({mb:.1f} MB)')

    # ── dates index ─────────────────────────────────────────────────────────
    dates_path = output_dir / 'dates.txt'
    with open(dates_path, 'w') as f:
        for date_str, _ in dates:
            f.write(date_str + '\n')

    total_mb = sum(
        (output_dir / f'{o}.bin.gz').stat().st_size for o in range(N_OCTETS)
    ) / 1e6
    print(f'\nDone.  {num_days} days · {total_mb:,.0f} MB (compressed) across {N_OCTETS} files.')
    print(f'Dates: {dates_path}  ({num_days} entries)')


if __name__ == '__main__':
    repo_root  = Path(__file__).resolve().parent.parent.parent  # docs/scripts → docs → repo
    output_dir = (
        Path(sys.argv[1]) if len(sys.argv) > 1
        else repo_root / 'docs' / 'data' / 'history'
    )
    build(repo_root, output_dir)
