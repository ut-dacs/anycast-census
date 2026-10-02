# Attribution

The census files in this repository contain data derived from the third-party sources below.

## Data in the published files

| Data source | Used in | License / terms |
|-------------|---------|-----------------|
| [GeoNames](https://www.geonames.org/) cities with a population of at least 15,000 | `locations`: `id`, `city`, `country_code`, `lat`, `lon` (since 2026-10-03) | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) |
| [CAIDA Routeviews Prefix to AS mappings](https://www.caida.org/catalog/datasets/routeviews-prefix2as/) | `backing_prefix`, `ASN` | [CAIDA Acceptable Use Agreement for Publicly Accessible Datasets](https://www.caida.org/about/legal/aua/public_aua/) |

## Measurement platforms

The latency-based (GCD) results are measured from:

- [CAIDA Ark](https://www.caida.org/projects/ark/) vantage points
- [TANGLED](https://tangled.dacs.utwente.nl/), the University of Twente anycast testbed

## Target lists

The hitlists used to select probe targets are listed under [Targets](README.md#targets).
They are not part of the published files where we only disclose the aggregate /24, /48 prefixes.
