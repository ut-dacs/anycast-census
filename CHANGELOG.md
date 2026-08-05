# Changelog

## Incidents

* **2026-07-17 to 2026-07-24** — Outage problems at [TANGLED](https://tangled.dacs.utwente.nl/), reducing the number of anycast PoPs used in measurements.
* **2025-11-17** — [Ark](https://caida.org/projects/ark/) outage; only 32 VPs available for GCD.
* **2025-05-01 to 2025-06-03** — GCD running with only [TANGLED](https://tangled.dacs.utwente.nl/) VPs (32) available.
* **2025-03-26 to 2025-04-15** — Structural changes to the [Ark](https://caida.org/projects/ark/) platform temporarily reduced nodes available for GCD.
* **2024-08-22 to 2024-09-04** — Technical problems with the pipeline limited detection and enumeration results.
* **2024-10-01 to 2024-12-24** - Problems with [MAnycastR](https://github.com/rhendriks/MAnycastR) dropping valid DNS replies.

## Infrastructure

* **2026-08-05** — Scanning now performed using the `--responsive` flag of [MAnycastR](https://github.com/rhendriks/MAnycastR), reducing probing time and cost by 40%.
* **2026-01-09** — Increase in [Ark](https://caida.org/projects/ark/) vantage points, raising average IPv4 anycast enumeration by 15%.
* **2025-09-04** — Updated feedback loop (ICMPv6).
* **2025-09-03** — Updated feedback loop (ICMPv4).
* **2025-04-15** — GCD measurements now performed with up to 270 [Ark](https://caida.org/projects/ark/) VPs.
* **2024-07-23** — Added [TANGLED](https://tangled.dacs.utwente.nl/) nodes to [Ark](https://caida.org/projects/ark/) (GCD lower-bound enumeration of large anycast deployments, e.g. Cloudflare, increased ~20% for IPv4 and ~60% for IPv6; GCD-detected anycast prefixes increased ~0.7% (IPv4) and ~1.5% (IPv6)).
* **2024-05-31** — GCD measurements now performed using the Ark platform instead of [TANGLED](https://tangled.dacs.utwente.nl/) nodes.

## Hitlist Updates
* **2026-04-21** — IPv4 hitlists update.
* **2026-03-27** - Added [ODNS](https://odns-data.netd.cs.tu-dresden.de/) to the IPv4 DNS hitlist, covering an additional 150 anycast /24-prefixes.
* **2026-03-23** — IPv6 hitlists update, adding [SRA](https://ipv6-sra.realmv6.org/).
* **2026-01-19** — IPv4/IPv6 hitlists update.
* **2025-11-18** — IPv4 hitlist update (DNS and ICMP).
* **2025-08-18** — Updated TCPv4 and IPv6 hitlists.
* **2025-05-13** — IPv4 hitlists update.
* **2025-04-12** - Updated IPv6 hitlists.
* **2025-03-20** — IPv4 hitlists update.
* **2024-12-06** — IPv4 hitlists update.
* **2024-12-03** — Extended hitlist using /32-granularity measurement results (12.7k → 13.4k IPv4 GCD-confirmed anycast /24s).
* **2024-11-21** — IPv4 hitlist update (12.1k → 12.7k IPv4 GCD-confirmed anycast /24s).
* **2024-11-13** — Updated IPv6 hitlist ([TUM](https://ipv6hitlist.github.io/) and [OpenINTEL](https://openintel.nl/) addresses).
* **2024-08-14** — Expanded IPv4/IPv6 hitlist using [AnyCatch](https://github.com/bgptools/anycast-prefixes/) and [Public DNS Server List](https://public-dns.info/) addresses (~100 IPv4, ~230 IPv6 additional anycast prefixes found).
* **2024-08-10** — Updated IPv6 hitlist, [TUM](https://ipv6hitlist.github.io/) (~150 additional anycast /48s found).

## Features
* **2024-09-05** — Added a `stats` file to the daily upload, reporting statistics such as the number of nodes used in the GCD measurement.
* **2024-08-15** — Added AAAA record addresses for domain names pointing to IPv4 anycast addresses (~10 additional anycast /48s found).
* **2024-06-19** — Added a list of previously found anycast prefixes (feedback loop) that are always checked with GCD (daily IPv4 /24s found via GCD increased from ~12.1k to ~12.3k).
