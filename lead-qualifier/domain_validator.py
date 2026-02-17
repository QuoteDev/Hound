"""
DNS + GeoIP domain validation module.
Performs fast async A-record lookups and country filtering with CDN/cloud exceptions.
"""

import asyncio
import ipaddress
import os
import re
from pathlib import Path
from typing import Callable, Optional

import dns.asyncresolver
import dns.exception
import dns.resolver

from domain_cache import (
    get_cached_domain,
    get_cached_domains_batch,
    init_cache,
    set_cached_domain,
)

try:
    import geoip2.database
    import geoip2.errors
except Exception:  # pragma: no cover - guarded runtime fallback
    geoip2 = None


# Domain normalization regex (same as server.py)
DOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9.-]+\.[a-z]{2,}(?:/.*)?$", re.IGNORECASE)

DNS_LOOKUP_TIMEOUT_SECONDS = 3.0
DEFAULT_DNS_CONCURRENCY = 800

# Common CDN/cloud ASNs and CIDRs used for inconclusive-pass classification.
COMMON_CDN_ASNS = {
    "cloudflare": [13335],
    "aws": [16509, 14618],
    "gcp": [15169, 396982],
    "azure": [8075, 8068],
    "vercel": [396982],
    "fastly": [54113],
    "akamai": [20940, 16625],
}

COMMON_CDN_IPV4_RANGES = {
    "cloudflare": [
        "173.245.48.0/20",
        "103.21.244.0/22",
        "103.22.200.0/22",
        "103.31.4.0/22",
        "141.101.64.0/18",
        "108.162.192.0/18",
        "190.93.240.0/20",
        "188.114.96.0/20",
        "197.234.240.0/22",
        "198.41.128.0/17",
        "162.158.0.0/15",
        "104.16.0.0/13",
        "172.64.0.0/13",
        "131.0.72.0/22",
    ],
    "aws": [
        "13.32.0.0/15",
        "13.224.0.0/14",
        "18.64.0.0/14",
        "52.46.0.0/18",
        "52.82.128.0/19",
        "54.182.0.0/16",
    ],
    "gcp": [
        "34.64.0.0/10",
        "34.128.0.0/10",
        "35.184.0.0/13",
        "35.192.0.0/14",
        "35.196.0.0/15",
        "35.198.0.0/16",
    ],
    "azure": [
        "20.0.0.0/11",
        "20.33.0.0/16",
        "40.64.0.0/10",
        "52.224.0.0/11",
    ],
    "vercel": [
        "76.76.21.0/24",
        "76.76.22.0/24",
        "76.76.26.0/24",
        "216.198.79.0/24",
    ],
    "fastly": [
        "151.101.0.0/16",
        "146.75.0.0/16",
        "185.31.16.0/22",
    ],
    "akamai": [
        "23.32.0.0/11",
        "23.192.0.0/11",
        "96.6.0.0/15",
        "184.24.0.0/13",
    ],
}

_COMPILED_CDN_NETWORKS: list[tuple[str, ipaddress._BaseNetwork]] = []
for provider, cidrs in COMMON_CDN_IPV4_RANGES.items():
    for cidr in cidrs:
        try:
            _COMPILED_CDN_NETWORKS.append((provider, ipaddress.ip_network(cidr)))
        except ValueError:
            continue


def _default_geoip_db_path() -> str:
    env_path = os.getenv("GEOLITE2_COUNTRY_DB_PATH")
    if env_path:
        return env_path
    return str(Path(__file__).with_name("GeoLite2-Country.mmdb"))


_GEOIP_DB_PATH = _default_geoip_db_path()
_GEOIP_READER = None
_GEOIP_READER_ERROR = False


def get_cdn_reference_data() -> dict:
    """Expose CDN/cloud ASN and CIDR references for diagnostics/UI metadata."""
    return {
        "asns": COMMON_CDN_ASNS,
        "ipv4Cidrs": COMMON_CDN_IPV4_RANGES,
    }


def normalize_domain(d: str) -> str:
    """
    Strip protocol, www., trailing slash from a domain string.
    Same logic as server.py normalize_domain.
    """
    if not d:
        return ""
    d = d.strip().lower()
    for prefix in ("https://", "http://", "www."):
        if d.startswith(prefix):
            d = d[len(prefix):]
    if "/" in d:
        d = d.split("/")[0]
    return d


def _get_geoip_reader():
    global _GEOIP_READER, _GEOIP_READER_ERROR
    if _GEOIP_READER is not None:
        return _GEOIP_READER
    if _GEOIP_READER_ERROR:
        return None
    if geoip2 is None:
        _GEOIP_READER_ERROR = True
        return None

    db_path = Path(_GEOIP_DB_PATH)
    if not db_path.exists():
        _GEOIP_READER_ERROR = True
        return None

    try:
        _GEOIP_READER = geoip2.database.Reader(str(db_path))
    except Exception:
        _GEOIP_READER_ERROR = True
        return None
    return _GEOIP_READER


def _ip_in_known_cdn(ip: str) -> Optional[str]:
    try:
        parsed = ipaddress.ip_address(ip)
    except ValueError:
        return None
    for provider, network in _COMPILED_CDN_NETWORKS:
        if parsed in network:
            return provider
    return None


def _lookup_country_code(ip: str) -> Optional[str]:
    reader = _get_geoip_reader()
    if reader is None:
        return None

    try:
        record = reader.country(ip)
        code = (record.country.iso_code or "").upper()
        return code or None
    except Exception:
        return None


def _evaluate_geo_for_ips(resolved_ips: list[str]) -> dict:
    non_us_countries: set[str] = set()
    us_found = False
    unknown_found = False
    cdn_hits: set[str] = set()

    for ip in resolved_ips:
        provider = _ip_in_known_cdn(ip)
        if provider:
            cdn_hits.add(provider)
            continue

        country = _lookup_country_code(ip)
        if not country:
            unknown_found = True
            continue

        if country == "US":
            us_found = True
        else:
            non_us_countries.add(country)

    if non_us_countries:
        return {
            "geo_status": "non_us",
            "geo_country": ",".join(sorted(non_us_countries)),
            "geo_inconclusive": False,
            "is_eligible": False,
            "status": f"non_us_country:{','.join(sorted(non_us_countries))}",
        }

    if cdn_hits:
        return {
            "geo_status": "inconclusive_cdn",
            "geo_country": "",
            "geo_inconclusive": True,
            "is_eligible": True,
            "status": f"cdn_inconclusive:{','.join(sorted(cdn_hits))}",
        }

    if us_found:
        return {
            "geo_status": "us",
            "geo_country": "US",
            "geo_inconclusive": False,
            "is_eligible": True,
            "status": "us",
        }

    if unknown_found:
        return {
            "geo_status": "inconclusive_geo",
            "geo_country": "",
            "geo_inconclusive": True,
            "is_eligible": True,
            "status": "geo_inconclusive",
        }

    return {
        "geo_status": "inconclusive_geo",
        "geo_country": "",
        "geo_inconclusive": True,
        "is_eligible": True,
        "status": "geo_inconclusive",
    }


def _shape_result(
    domain: str,
    has_a_record: bool,
    is_alive: bool,
    status: str,
    resolved_ips: Optional[list[str]] = None,
    geo_status: str = "not_checked",
    geo_country: str = "",
    geo_inconclusive: bool = False,
    is_eligible: Optional[bool] = None,
) -> dict:
    ips = sorted(set(resolved_ips or []))
    if is_eligible is None:
        is_eligible = bool(is_alive)
    return {
        "domain": domain,
        "has_mx": False,
        "has_a_record": has_a_record,
        "is_alive": is_alive,
        "status": status,
        "resolved_ips": ips,
        "resolved_ips_csv": ",".join(ips),
        "geo_status": geo_status,
        "geo_country": geo_country,
        "geo_inconclusive": bool(geo_inconclusive),
        "is_eligible": bool(is_eligible),
    }


def _result_from_cache(input_domain: str, cached: dict) -> dict:
    status = str(cached.get("status") or "unknown")
    is_alive = bool(cached.get("is_alive"))
    is_eligible = bool(is_alive)

    if status.startswith("non_us_country"):
        is_eligible = False
    elif status in {"invalid", "nxdomain", "dns_timeout", "dns_unresolved", "no_a_record", "no domain", "no_domain"}:
        is_eligible = False

    if bool(cached.get("geo_inconclusive")) or status.startswith("cdn_inconclusive"):
        is_eligible = True

    return _shape_result(
        domain=input_domain,
        has_a_record=bool(cached.get("has_a_record")),
        is_alive=is_alive,
        status=status,
        resolved_ips=list(cached.get("resolved_ips") or []),
        geo_status=str(cached.get("geo_status") or "not_checked"),
        geo_country=str(cached.get("geo_country") or ""),
        geo_inconclusive=bool(cached.get("geo_inconclusive")),
        is_eligible=is_eligible,
    )


def _cached_result_is_usable(cached: dict) -> bool:
    status = str(cached.get("status") or "").strip().lower()
    legacy_statuses = {
        "has_mx",
        "has_a_record",
        "has_aaaa_record",
        "has_a_record_system",
        "no_dns_records",
    }
    if status in legacy_statuses:
        return False
    if bool(cached.get("is_alive")) and not list(cached.get("resolved_ips") or []):
        return False
    return True


async def check_domain_dns(domain: str, resolver: Optional[dns.asyncresolver.Resolver] = None) -> dict:
    """
    Resolve domain A records and evaluate GeoIP eligibility.

    Rules:
    - NXDOMAIN and 3s DNS timeout are disqualifying.
    - Non-US resolved IPs are disqualifying.
    - Known CDN/cloud IP ranges are inconclusive and pass-through.

    Returns:
        dict with {domain, has_a_record, is_alive, status, resolved_ips, geo_*, is_eligible}
    """
    if not domain or str(domain).lower() in ("unknown", "n/a", "none", ""):
        return _shape_result(
            domain=domain,
            has_a_record=False,
            is_alive=False,
            status="no_domain",
            is_eligible=False,
        )

    clean = normalize_domain(domain)
    if not clean or "." not in clean or not DOMAIN_RE.match(clean):
        return _shape_result(
            domain=domain,
            has_a_record=False,
            is_alive=False,
            status="invalid",
            is_eligible=False,
        )

    cached = await get_cached_domain(clean)
    if cached and _cached_result_is_usable(cached):
        return _result_from_cache(domain, cached)

    if resolver is None:
        resolver = dns.asyncresolver.Resolver()
        resolver.timeout = 1.0
        resolver.lifetime = DNS_LOOKUP_TIMEOUT_SECONDS

    try:
        a_records = await resolver.resolve(clean, "A", lifetime=DNS_LOOKUP_TIMEOUT_SECONDS)
        resolved_ips = sorted({r.address for r in a_records if getattr(r, "address", None)})
        if not resolved_ips:
            result = _shape_result(
                domain=domain,
                has_a_record=False,
                is_alive=False,
                status="no_a_record",
                is_eligible=False,
            )
            await set_cached_domain(clean, False, False, False, result["status"])
            return result

        geo_eval = _evaluate_geo_for_ips(resolved_ips)
        result = _shape_result(
            domain=domain,
            has_a_record=True,
            is_alive=True,
            status=geo_eval["status"],
            resolved_ips=resolved_ips,
            geo_status=geo_eval["geo_status"],
            geo_country=geo_eval["geo_country"],
            geo_inconclusive=geo_eval["geo_inconclusive"],
            is_eligible=geo_eval["is_eligible"],
        )
        await set_cached_domain(
            clean,
            False,
            True,
            True,
            result["status"],
            resolved_ips=result["resolved_ips"],
            geo_status=result["geo_status"],
            geo_country=result["geo_country"],
            geo_inconclusive=result["geo_inconclusive"],
        )
        return result
    except dns.resolver.NXDOMAIN:
        result = _shape_result(
            domain=domain,
            has_a_record=False,
            is_alive=False,
            status="nxdomain",
            is_eligible=False,
        )
        await set_cached_domain(clean, False, False, False, result["status"])
        return result
    except dns.resolver.NoAnswer:
        result = _shape_result(
            domain=domain,
            has_a_record=False,
            is_alive=False,
            status="no_a_record",
            is_eligible=False,
        )
        await set_cached_domain(clean, False, False, False, result["status"])
        return result
    except (dns.resolver.LifetimeTimeout, dns.exception.Timeout):
        return _shape_result(
            domain=domain,
            has_a_record=False,
            is_alive=False,
            status="dns_timeout",
            is_eligible=False,
        )
    except dns.resolver.NoNameservers:
        return _shape_result(
            domain=domain,
            has_a_record=False,
            is_alive=False,
            status="dns_unresolved",
            is_eligible=False,
        )
    except dns.exception.DNSException:
        return _shape_result(
            domain=domain,
            has_a_record=False,
            is_alive=False,
            status="dns_error",
            is_eligible=False,
        )
    except Exception:
        return _shape_result(
            domain=domain,
            has_a_record=False,
            is_alive=False,
            status="dns_error",
            is_eligible=False,
        )


async def check_domains_dns_batch(
    domains: list[str],
    concurrency: int = DEFAULT_DNS_CONCURRENCY,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> dict:
    """
    Check many domains concurrently using async A-record DNS + GeoIP validation.

    Args:
        domains: List of domain names to check
        concurrency: Max concurrent DNS queries (default 800)
        progress_callback: Optional callback(processed, total) for progress updates

    Returns:
        dict mapping domain -> detailed domain validation result
    """
    await init_cache()

    raw_cached_results = await get_cached_domains_batch(domains)
    cached_results = {
        domain: result
        for domain, result in raw_cached_results.items()
        if _cached_result_is_usable(result)
    }
    hydrated_cached = {domain: _result_from_cache(domain, result) for domain, result in cached_results.items()}

    uncached_domains = [d for d in domains if d not in cached_results]

    if not uncached_domains:
        if progress_callback:
            progress_callback(len(domains), len(domains))
        return hydrated_cached

    resolver = dns.asyncresolver.Resolver()
    resolver.timeout = 1.0
    resolver.lifetime = DNS_LOOKUP_TIMEOUT_SECONDS

    sem = asyncio.Semaphore(max(1, int(concurrency or DEFAULT_DNS_CONCURRENCY)))

    async def bounded_check(domain_name: str):
        async with sem:
            return await check_domain_dns(domain_name, resolver)

    batch_size = 1000
    uncached_results = {}
    total_uncached = len(uncached_domains)
    processed = len(cached_results)

    for i in range(0, total_uncached, batch_size):
        batch = uncached_domains[i:i + batch_size]
        tasks = [asyncio.create_task(bounded_check(d)) for d in batch]

        for future in asyncio.as_completed(tasks):
            try:
                result = await future
            except Exception:
                result = None

            if isinstance(result, dict):
                uncached_results[result["domain"]] = result

            processed += 1
            if progress_callback:
                progress_callback(processed, len(domains))

    return {**hydrated_cached, **uncached_results}


async def check_domain_with_smtp(domain: str, timeout: int = 10) -> dict:
    """
    Advanced domain validation with SMTP verification.

    This is more thorough but slower (~1-3 seconds per domain).
    Use only for critical leads or when high accuracy is required.

    Args:
        domain: Domain name to check
        timeout: SMTP connection timeout in seconds

    Returns:
        dict with {domain, has_mx, has_smtp, is_alive, status}

    Note: Not implemented in initial version. Placeholder for future enhancement.
    """
    _ = timeout

    result = await check_domain_dns(domain)

    if not result["has_a_record"]:
        return {
            **result,
            "has_smtp": False,
            "status": "no_a_record",
        }

    return {
        **result,
        "has_smtp": None,
        "status": result["status"],
    }
