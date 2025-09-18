# LACeS Anycast Census
LACeS: an Open, Fast, Responsible and Efficient Longitudinal Anycast Census System

This repository contains the dataset of the Anycast Census (detected /24 Anycast Prefixes), discovered using LACeS.
The repository is updated daily.

Contact [remi.hendriks@utwente.nl](mailto:remi.hendriks@utwente.nl)

## Recommendations for using the census

TLDR:
* We recommend filtering on `(AB > 3) || (GCD > 1)` when high confidence is needed.
* We recommend filtering on `(AB > 1) || (GCD > 1)` (all) when completeness is needed. 

### False detection of anycast
The anycast-based approach (AB) suffers from FPs (see [MAnycast2](https://www.sysnet.ucsd.edu/sysnet/miscpapers/manycast2-imc20.pdf)).
These FPs are especially prevalent when AB has a value of less than 3 (i.e., receiving replies at less than 3 sites).

### False detection of unicast
The latency-based approach (GCD) is highly accurate.
However, it has rare cases of FNs when anycast is deployed in small geographic regions (i.e., regional anycast).

### Recommendations
If high confidence is needed, ensure that AB is higher than 2 or GCD detects anycast.
If completeness is needed, use all prefixes in this census (either methodology detects anycast).

## Partial anycast
To minimize the impact of our daily probing methodology we scan at a /24 granularity.
However, there are cases of partial anycast where the /24 contains both unicast and anycast addresses.
Scanning at /32 granularity reveals ~1.0k /24s are partially anycast.
We flag these cases using bi-annual measurement data, but partial prefixes are dynamic over time.
Future work is providing an API for live measurements.

## IPv6
We use AAAA record addresses from [OpenINTEL](https://www.openintel.nl/) and TUM's public IPv6 hitlist [IPv6Hitlist](https://ipv6hitlist.github.io/).
To maintain reasonable probing times, we scan only the first /48 of aliased prefixes.
A join with the aliased prefixes set from TUM's hitlist, should give a more complete list of anycast prefixes.

## Citing LACeS
When making use of this dataset for academic research, please cite the following research paper.

```
@misc{hendriks2025laces,
      title={LACeS: an Open, Fast, Responsible and Efficient Longitudinal Anycast Census System}, 
      author={Remi Hendriks and Matthew Luckie and Mattijs Jonker and Raffaele Sommese and Roland van Rijswijk-Deij},
      year={2025},
      eprint={2503.20554},
      archivePrefix={arXiv},
      primaryClass={cs.NI},
      url={https://arxiv.org/abs/2503.20554}, 
}
```

## Anycast Detection Data Structure

Following we describe the structure of the provided census files.

### Detection Files
#### Path:

* Latest files (updated daily)
```
IPv4-latest.parquet
IPv6-latest.parquet
IPv4-latest.csv
IPv6-latest.csv
stats-latest

```

* Historical files (going back to March 21, 2024)
```
YYYY/MM/DD/IPv4.parquet
YYYY/MM/DD/IPv6.parquet
YYYY/MM/DD/IPv4.csv
YYYY/MM/DD/IPv6.csv
YYYY/MM/DD/stats
```
#### Structure:
**Example**
IPv4.parquet
```bash
prefix  AB_ICMPv4  AB_TCPv4  AB_DNSv4  GCD_ICMPv4  GCD_TCPv4  partial backing_prefix            ASN                                          locations
1.1.1.0/24         29        29        29          67         30    False     1.1.1.0/24          13335  [{'city': 'Honolulu', 'code_country': 'US', 'id': 'HNL', 'latitude': 21.3187007904, 'longitude': -157.9219970703}, ... ]
```

**Columns**
- `Prefix`: The candidate anycast /24 prefix being analyzed (e.g., "1.0.0.0/24").
- `AB_ICMPv4/v6`: Number of locations found using the anycast-based method (ICMP).
- `AB_TCPv4/v6`: Number of locations found using the anycast-based method (TCP SYNACK).
- `AB_DNSv4/v6`: Number of locations found using the anycast-based method (DNS/UDP).
- `GCD_ICMPv4/v6`: Number of sites found using the latency-based method (ICMP).
- `GCD_TCPv4/v6`: Number of sites found using the latency-based method (TCP).
- `partial`: Whether we detected partial anycast in this prefix (IPv4 only).
- `backing_prefix`: Corresponding IP routing table prefix (as observed using RouteViews).
- `ASN`: ASN(s) announcing the prefix (MOASes are separated by `_`).
- `locations`: Locations found using GCD (ICMP locations preferred).
`AB` and `GCD` detect anycast if number of sites found is larger than 1.
`backing_prefix` and `ASN` are from CAIDA's [prefix2as](https://www.caida.org/catalog/datasets/routeviews-prefix2as/) dataset.

`locations` has the following format:
- `city`: Geolocated city using iGreedy's algorithm.
- `country_code`: 2 character country code (ISO 3166-1 alpha-2).
- `id`: Nearest airport IATA 3 letter code.
- `lat`: Latitude of airport.
- `lon`: Longitude of airport.

Due to NDA agreements with hitlists providers, only /24 prefixes marked at least by one measurement method as anycast are reported.

IPv4.csv
```bash
prefix,number_of_sites,backing_prefix
1.1.1.0/24,67,1.1.1.0/24
```

We provide a .csv (with limited data) as it can be loaded using GitHub's Web UI for ease-of-access.
This contains the /24-IPv4 or /48-IPv6 prefixes detected as anycast using GCD, alongside the number of sites found using GCD, and the backing prefix.
