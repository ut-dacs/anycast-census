# Changelog

* 31/05/2024 - GCD measurements are now performed using the Ark platform, instead of MAnycastR nodes
* 19/06/2024 - Added list of previously found anycast prefixes that are always checked with GCD. (increasing number of daily IPv4 /24s found with GCD from ~12.1k to ~12.3k)
* 23/07/2024 - Added MAnycastR nodes to Ark. (GCD lower bound enumeration of large anycast deployments (e.g., CloudFlare) increased by ~20% for IPv4, ~60% for IPv6), (GCD number of anycast prefixes detected increased by ~0.7% and ~1.5% for IPv4 and IPv6 respectively)
* 10/08/2024 - Updated IPv6 hitlist, TUM (~150 additional anycast /48s found)
* 14/08/2024 - Expanded IPv4, IPv6 hitlist using anycatch and public-dns.info addresses (~100, ~230 additional anycast prefixes found for IPv4 and IPv6 respectively)
* 15/08/2024 - Added AAAA record addresses for domain names that point to IPv4 anycast addresses (~10 additional anycasted /48s found)
* 22/08/2024 <-> 04/09/2024 - Technical problems with pipeline, limiting results in both detection and enumeration.
* 05/09/2024 - Added 'stats' file to daily upload, giving statistics about daily upload. Including number of available nodes used in the GCD measurement (as it affects performance)
* 13/11/2024 - Updated IPv6 hitlist (both TUM and OI addresses)
* 21/11/2024 - Updated IPv4 hitlist (12.1k -> 12.7k IPv4 GCD-confirmed anycast /24s) 
* 03/12/2024 - Extended hitlist using /32-granularity measurement results (12.7k -> 13.4k IPv4 GCD-confirmed anycast /24s)
* 06/12/2024 - Updated IPv4 hitlist
* 24/12/2024 - Fixed DNS probing
* 20/03/2025 - updated v4 hitlist
* 26/03/2025 <-> 15/04/2025 - structural changes to Ark platform (fewer nodes available for GCD)
* 15/04/2025 - GCD measurements now performed with up to 270 Ark VPs
* 13/05/2025 - updated v4 hitlist
* 01/05/2025 <-> 03/06/2025 - GCD with only 32 VPs available
* 18/08/2025 - Updated TCPv4 and IPv6 hitlists
* 03/09/2025 - Updated feedback loop (ICMPv4)
* 04/09/2025 - Updated feedback loop (ICMPv6)
* 13/11/2025 <-> 14/11/2025 - Routing anomaly at ca-yto anycast site. Site temporarily removed from AB measurements [ONGOING]
* 17/11/2025 - Ark outage -> only 32 VPs used for GCD
* 18/11/2025 - Update IPv4 hitlist (DNS and ICMP)
* 12/04/2025 - UPdate IPv6 hitlists
