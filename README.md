# LACeS Anycast Census
[LACeS: an Open, Fast, Responsible and Efficient Longitudinal Anycast Census System](https://doi.org/10.1145/3730567.3764484)

[This repository](https://github.com/ut-dacs/anycast-census) contains the dataset of the Anycast Census (detected /24 Anycast Prefixes), discovered using LACeS.
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
If high confidence is needed, ensure that AB is higher than 3 or GCD detects anycast.
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
@inproceedings{10.1145/3730567.3764484,
      author = {Hendriks, Remi and Luckie, Matthew and Jonker, Mattijs and Sommese, Raffaele and van Rijswijk-Deij, Roland},
      title = {LACeS: An Open, Fast, Responsible and Efficient Longitudinal Anycast Census System},
      year = {2025},
      isbn = {9798400718601},
      publisher = {Association for Computing Machinery},
      address = {New York, NY, USA},
      url = {https://doi.org/10.1145/3730567.3764484},
      doi = {10.1145/3730567.3764484},
      abstract = {IP anycast replicates an address at multiple locations to reduce latency and enhance resilience. Due to anycast's crucial role in the modern Internet, earlier research introduced tools to perform anycast censuses. The first, iGreedy, uses latency measurements from geographically dispersed locations to map anycast deployments. The second, MAnycast2, uses anycast to perform a census of other anycast networks. MAnycast2's advantage is speed and coverage but suffers from problems with accuracy, while iGreedy is highly accurate but slower using author-defined probing rates and costlier. In this paper we address the shortcomings of both systems and present LACeS (Longitudinal Anycast Census System). Taking MAnycast2 as a basis, we completely redesign its measurement pipeline, and add support for distributed probing, additional protocols (DNS over UDP, TCP SYN/ACK, and IPv6) and latency measurements similar to iGreedy. We validate LACeS on an anycast testbed with 32 globally distributed nodes, compare against an external anycast production deployment, extensive latency measurements with RIPE Atlas and cross-check over 60\% of detected anycast using operator ground truth that shows LACeS achieves high accuracy. Finally, we provide a longitudinal analysis of anycast, covering 17+months, showing LACeS achieves high precision. We make continual daily LACeS censuses available to the community and release the source code of the tool under a permissive open source license.},
      booktitle = {Proceedings of the 2025 ACM Internet Measurement Conference},
      pages = {445–461},
      numpages = {17},
      keywords = {internet measurement, anycast, internet topology, routing, ip},
      location = {USA},
      series = {IMC '25}
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
- `city`: Geolocated city using [iGreedy's algorithm](https://ieeexplore.ieee.org/abstract/document/7470242).
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

## Running your own anycast census

We make all tooling publicly available (licensed under MPL2.0).
* First, we provide measurement tooling for performing AB and GCD measurements (available at [MAnycastR](https://github.com/rhendriks/MAnycastR)).
* Second, we provide our optimized implementation of [iGreedy](https://ieeexplore.ieee.org/abstract/document/7470242) (available at [MiGreedy](https://github.com/rhendriks/MiGreedy)).
