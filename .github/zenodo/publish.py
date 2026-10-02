"""
Archive census months on Zenodo as a new version of the census record.

Each month becomes census-YYYY-MM.zip, holding that month's YYYY/MM/DD/ folders.
A new version keeps the files of the version before it, so the latest version holds every month archived so far.

usage: publish.py [--dry-run] YYYY-MM [YYYY-MM ...]
env:   ZENODO_TOKEN    access token
       ZENODO_URL      default https://zenodo.org
       ZENODO_CONCEPT  the record's concept ID, default 17174467
"""

import argparse
import datetime
import json
import os
import pathlib
import sys
import time
import zipfile

import requests

API = os.environ.get('ZENODO_URL', 'https://zenodo.org').rstrip('/') + '/api'
CONCEPT = os.environ.get('ZENODO_CONCEPT', '17174467')
METADATA = pathlib.Path(__file__).with_name('metadata.json')
DESCRIPTION = pathlib.Path(__file__).with_name('description.html')


def check(response):
    """
    The response, or exit with what Zenodo said.
    """
    if not response.ok:
        sys.exit(f"{response.request.method} {response.url}: HTTP {response.status_code}: {response.text[:1000]}")
    return response


def month_name(value):
    """
    A YYYY-MM argument, validated.
    """
    datetime.datetime.strptime(value, '%Y-%m')
    return value


def archive(month):
    """
    Zip one month's census folders into census-YYYY-MM.zip. Returns its path and the number of days.
    """
    year, mon = month.split('-')
    folder = pathlib.Path(year, mon)
    days = sorted(p for p in folder.iterdir() if p.is_dir()) if folder.is_dir() else []
    if not days:
        sys.exit(f"no census for {month}: {folder}/ is missing or empty")

    path = pathlib.Path(f"census-{month}.zip")
    with zipfile.ZipFile(path, 'w') as z:
        for f in sorted(folder.rglob('*')):
            if f.is_file():
                # Parquet is compressed already; the CSVs and stats are not
                stored = f.suffix == '.parquet'
                z.write(f, f.as_posix(), compress_type=zipfile.ZIP_STORED if stored else zipfile.ZIP_DEFLATED)
    return path, len(days)


def open_draft(session, latest):
    """
    The record's unpublished draft: the one a failed run left open, or else a new version of the
    latest, which starts out holding that version's files.
    """
    depositions = check(session.get(f"{API}/deposit/depositions", params={'all_versions': 'true', 'size': 100},
                                    timeout=60)).json()
    for deposition in depositions:
        if str(deposition.get('conceptrecid')) == CONCEPT and not deposition.get('submitted'):
            print(f"Continuing the unpublished draft {deposition['id']}")
            return check(session.get(deposition['links']['self'], timeout=60)).json()

    created = check(session.post(f"{API}/deposit/depositions/{latest['id']}/actions/newversion", timeout=60)).json()
    return check(session.get(created['links']['latest_draft'], timeout=60)).json()


def upload(session, draft, path, attempts=5):
    """
    Upload path into the draft, retrying when the connection drops or Zenodo has a server error.
    """
    for attempt in range(1, attempts + 1):
        try:
            with path.open('rb') as data:
                response = session.put(f"{draft['links']['bucket']}/{path.name}", data=data, timeout=3600)
            if response.ok:
                return
            if response.status_code < 500:
                check(response)
            error = f"HTTP {response.status_code}"
        except requests.exceptions.RequestException as e:
            error = type(e).__name__
        if attempt == attempts:
            sys.exit(f"could not upload {path.name}: {error}, {attempts} attempts")
        print(f"  attempt {attempt} failed ({error}), retrying in {60 * attempt} s")
        time.sleep(60 * attempt)
        # a partial upload may have left the file behind
        current = check(session.get(draft['links']['self'], timeout=60)).json()
        for f in current.get('files', []):
            if f['filename'] == path.name:
                check(session.delete(f['links']['self'], timeout=60))


def main():
    parser = argparse.ArgumentParser(description="Archive census months on Zenodo as a new version.")
    parser.add_argument('months', nargs='+', type=month_name, help="months to archive, YYYY-MM")
    parser.add_argument('--dry-run', action='store_true', help="build the zips and show the plan, change nothing on Zenodo")
    args = parser.parse_args()

    archives = [archive(m) for m in sorted(set(args.months))]
    for path, days in archives:
        print(f"{path.name}: {days} days, {path.stat().st_size / 1e6:,.1f} MB")

    latest = check(requests.get(f"{API}/records/{CONCEPT}/versions/latest", timeout=60)).json()
    kept = sorted(f['key'] for f in latest.get('files', []))
    replaced = sorted(p.name for p, _ in archives if p.name in kept)
    print(f"Latest version: {latest['id']} ({latest['metadata'].get('version')}), {len(kept)} files")
    if replaced:
        print(f"Replacing: {', '.join(replaced)}")
    if args.dry_run:
        print("Dry run: nothing published")
        return

    session = requests.Session()
    session.headers['Authorization'] = f"Bearer {os.environ['ZENODO_TOKEN']}"
    draft = open_draft(session, latest)

    present = {f['filename']: f for f in draft.get('files', [])}
    for i, (path, _) in enumerate(archives, 1):
        there = present.get(path.name)
        if there and there['filesize'] == path.stat().st_size:
            print(f"{path.name} ({i}/{len(archives)}): already in the draft")
            continue
        if there:
            check(session.delete(there['links']['self'], timeout=60))
        print(f"Uploading {path.name} ({i}/{len(archives)})")
        upload(session, draft, path)

    metadata = draft['metadata']
    metadata.pop('doi', None)  # the draft gets its own DOI on publication
    metadata.update(json.loads(METADATA.read_text()))
    metadata['description'] = DESCRIPTION.read_text()
    metadata['version'] = max(args.months)
    metadata['publication_date'] = datetime.date.today().isoformat()
    check(session.put(draft['links']['self'], json={'metadata': metadata}, timeout=60))

    published = check(session.post(draft['links']['publish'], timeout=300)).json()
    print(f"Published version {metadata['version']}: https://doi.org/{published['doi']}")


if __name__ == '__main__':
    main()
