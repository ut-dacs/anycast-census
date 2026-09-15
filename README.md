# LACeS Anycast Census

[![DOI](https://img.shields.io/badge/DOI-10.1145%2F3730567.3764484-blue)](https://doi.org/10.1145/3730567.3764484)
[![License: MPL 2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
[![Website](https://img.shields.io/badge/Website-manycast.net-blue)](https://manycast.net)

[LACeS: an Open, Fast, Responsible and Efficient Longitudinal Anycast Census System](https://doi.org/10.1145/3730567.3764484)

**Contact:** [remi.hendriks@utwente.nl](mailto:remi.hendriks@utwente.nl)

## Contents

- [Downloading census data](#downloading-census-data)
- [LACeS Explorer — interactive web interface](#laces-explorer--interactive-web-interface)
- [Using the Python helper](#using-the-python-helper)
- [Understanding the dataset](#understanding-the-dataset)
  - [Data recommendations](#data-recommendations)
  - [Detection methods](#detection-methods)
- [Data structure](#data-structure)
  - [Columns](#columns)
  - [Geolocation accuracy metrics](#geolocation-accuracy-metrics)
  - [Unicast geolocation results](#unicast-geolocation-results)
- [Measurement methodology](#measurement-methodology)
- [Running your own census](#running-your-own-census)
- [Citation](#citation)

## Downloading census data

### Available formats

The parquet files in this repository can be downloaded in alternative formats via the [manycast.net REST API](https://manycast.net/api/docs):

| Format | Example |
|--------|---------|
| `.parquet` | `https://manycast.net/api/v1/export/IPv4-latest.parquet` |
| `.parquet.gz` | `https://manycast.net/api/v1/export/IPv4-latest.parquet.gz` |
| `.csv.gz` | `https://manycast.net/api/v1/export/IPv4-latest.csv.gz` |
| `.json.gz` | `https://manycast.net/api/v1/export/IPv4-latest.json.gz` |

Replace `latest` with a specific date (e.g., `2026-03-22`) to download a historical snapshot. Both IPv4 and IPv6 are available with prefixes `IPv4-` and `IPv6-`.

**Example:**
```bash
curl -O https://manycast.net/api/v1/export/IPv4-2026-03-22.csv.gz
```

See the [full API documentation](https://manycast.net/api/docs) for querying individual prefixes, ASNs, and daily statistics.

## LACeS Explorer — interactive web interface

We provide an [interactive dashboard](https://manycast.net) for querying, visualizing, and exploring the census data:

**Search & lookup.** Enter any IP address, prefix, ASN, domain, or TLD to look up anycast status, geolocation of detected PoPs, and related network information.
Historical snapshots are available for all census dates since March 21, 2024.

**Compare mode.** Select two different census dates to track how anycast deployment changed over time.

**RIPE Atlas integration.** Run the geolocation algorithm on public RIPE Atlas results.

**Analytics.** Statistics over time, anycast by region, Hilbert curve of anycast space, ...

## Using the Python helper

The [`census_helper.py`](census_helper.py) file provides convenience functions for downloading and filtering the dataset. Requires `pandas`, `pyarrow`, and `requests`.

### Basic usage

```python
import census_helper

# Download latest snapshot and filter to high-confidence anycast prefixes
census = census_helper.download_latest("v4")
anycast = census_helper.filter_anycast(census, "v4")

# Or use comprehensive coverage (includes borderline cases)
anycast = census_helper.filter_anycast(census, "v4", confidence="comprehensive")
```

### Command-line usage

```bash
python census_helper.py --ip-version v4 --date latest --prefixes-only
python census_helper.py --ip-version v4 --date 2026-03-22 --confidence comprehensive
```

## Understanding the dataset

### Data recommendations

Depending on requirements we recommend the following filtering:

**High confidence:**
```
(AB > 3) || (GCD > 1)
```

**Comprehensive coverage:**
```
(AB > 1) || (GCD > 1)
```

### Detection methods

The census uses two detection methods:

**Anycast-based (AB):** Detects anycast using anycast.
A prefix is considered anycast if multiple PoPs receive replies.
See [MAnycast2](https://www.sysnet.ucsd.edu/sysnet/miscpapers/manycast2-imc20.pdf) for details.
Three probe protocols are used: ICMP/ping, TCP SYN/ACK, and DNS/UDP.

- **False positives (FPs):** This method is known to produce FPs, especially when the number of receiving PoPs is < 4.

**Latency-based (GCD):** Detects anycast using latency measurements and Great-Circle-Distance calculations.

- **False negatives (FNs):** Highly accurate, but struggles with detecting regional anycast (i.e., anycast deployed within a tight geographic area).

**NOTE**
We provide unicast geolocation results when AB > 1 but GCD == 1.
This may help with determining whether AB results are FPs or GCD results are FNs.

## Data structure

### File paths

**Latest files (updated daily):**
```
IPv4-latest.parquet
IPv6-latest.parquet
IPv4-latest.csv
IPv6-latest.csv
stats-latest
```

**Historical files (since March 21, 2024):**
```
YYYY/MM/DD/IPv4.parquet
YYYY/MM/DD/IPv6.parquet
YYYY/MM/DD/IPv4.csv
YYYY/MM/DD/IPv6.csv
YYYY/MM/DD/stats
```

### Columns

| Column | Description |
|--------|-------------|
| `prefix` | The candidate anycast /24 prefix (e.g., `1.0.0.0/24`) |
| `AB_ICMPv4/v6` | Locations found using anycast-based method (ICMP) |
| `AB_TCPv4/v6` | Locations found using anycast-based method (TCP SYNACK) |
| `AB_DNSv4/v6` | Locations found using anycast-based method (DNS/UDP) |
| `GCD_ICMPv4/v6` | Sites found using latency-based method (ICMP) |
| `GCD_TCPv4/v6` | Sites found using latency-based method (TCP) |
| `partial` | Whether partial anycast was detected (IPv4 only) |
| `backing_prefix` | Corresponding IP routing table prefix (RouteViews) |
| `ASN` | ASN(s) announcing the prefix (MOASes separated by `;`) |
| `locations` | Detailed geolocation data from detected sites (see below) |

### Locations column

| Field | Description |
|-------|-------------|
| `city` | Geolocated city using iGreedy's algorithm |
| `country_code` | 2-character country code (ISO 3166-1 alpha-2) |
| `airport_code` | Nearest airport IATA 3-letter code |
| `lat` | Airport latitude |
| `lon` | Airport longitude |
| `radius` | Radius of the RTT disc in kilometers |
| `candidate_diameter` | Maximum pairwise distance (km) between surviving candidate cities; smaller values indicate higher precision |
| `num_constraints` | Number of overlapping discs that refined the candidate set; higher values indicate higher confidence in the result |

The last three fields help in determining the confidence of geolocation results.

### CSV format

Limited data is provided in CSV format for ease of access via GitHub's Web UI:

```
prefix,number_of_sites,backing_prefix
1.1.1.0/24,67,1.1.1.0/24
```

## Measurement methodology

### IPv4 targets

We use the [USC/ISI ANT IPv4 hitlist](https://ant.isi.edu/datasets/index.html) (ranked ICMP/ping responsive IP addresses per /24), supplemented by:
- Public DNS nameservers
- OpenINTEL infra:ns records

### IPv6 targets

We use:
- AAAA records from [OpenINTEL](https://www.openintel.nl/)
- [IPv6Hitlist](https://ipv6hitlist.github.io/)
- [IPv6-SRA](https://ipv6-sra.realmv6.org/) from TU Dresden and HAW Hamburg

### Partial anycast

To minimize daily probing impact, we scan at /24 granularity.
However, some prefixes contain mixed unicast and anycast IP addresses (see paper for details) which we detect using multi-target probing.

## Running your own census

We make all measurement and analysis tooling publicly available under the MPL 2.0 license:

### Measurement tooling

[MAnycastR](https://github.com/rhendriks/MAnycastR) provides implementations for AB and GCD measurements

### Geolocation

[MiGreedy](https://github.com/rhendriks/MiGreedy) is our optimized implementation of [iGreedy's algorithm](https://ieeexplore.ieee.org/document/7470242) for IP geolocation, designed for large-scale production censuses.
It supports unicast geolocation, improved detection using intersection, and confidence/accuracy metrics.

## Citation

When using this dataset for academic research, please cite the following paper:

```bibtex
@inproceedings{10.1145/3730567.3764484,
  author = {Hendriks, Remi and Luckie, Matthew and Jonker, Mattijs and van Rijswijk-Deij, Roland},
  title = {LACeS: an Open, Fast, Responsible and Efficient Longitudinal Anycast Census System},
  year = {2025},
  booktitle = {Proceedings of the 2025 Internet Measurement Conference}
}
```
