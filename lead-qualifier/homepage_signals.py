"""
Homepage scraping + lightweight signal extraction for qualification.
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Callable, Optional
import hashlib

import dns.asyncresolver
import dns.exception
import dns.resolver
import httpx
from bs4 import BeautifulSoup
from domain_cache import init_cache, get_cached_homepages_batch, set_cached_homepage


DEFAULT_HOMEPAGE_TIMEOUT_SECONDS = 6.0
DEFAULT_HOMEPAGE_CONCURRENCY = 80
HOMEPAGE_MAX_BYTES = 200 * 1024
SOFT_DISQUALIFY_STRIKE_THRESHOLD = 3

HOMEPAGE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

B2B_POSITIVE_KEYWORDS = [
    "platform",
    "api",
    "integration",
    "enterprise",
    "teams",
    "dashboard",
    "analytics",
    "workflow",
    "compliance",
    "soc",
    "deploy",
    "infrastructure",
    "saas",
    "b2b",
    "developer",
    "automation",
    "data platform",
    "security",
    "governance",
    "knowledge base",
    "documentation",
    "pricing",
    "book demo",
    "request demo",
    "contact sales",
    "for business",
    "schedule demo",
    "get started",
    "solutions",
    "customers",
    "case study",
    "case studies",
    "roi",
    "onboarding",
    "client success",
    "customer success",
    "managed services",
    "professional services",
    "consulting",
    "implementation",
    "white paper",
    "webinar",
    "cloud",
    "devops",
    "machine learning",
    "artificial intelligence",
    "crm",
    "erp",
    "supply chain",
    "logistics",
    "fintech",
    "healthtech",
    "cybersecurity",
    "single sign on",
    "sso",
    "oauth",
    "scalable",
    "real time",
    "open source",
]

DISQUALIFY_SIGNAL_KEYWORDS = [
    "shop now",
    "add to cart",
    "free shipping",
    "download the app",
    "recipes",
    "portfolio site",
    "personal blog",
    "coming soon",
    "parked domain",
    "this domain is for sale",
    "wedding photography",
    "fashion store",
    "recipe blog",
    "buy now",
    "checkout",
    "shopping cart",
    "order now",
    "coupon code",
    "promo code",
    "return policy",
    "track your order",
    "baby registry",
    "gift card",
    "clearance sale",
    "daily deals",
    "flash sale",
    "handmade jewelry",
    "pet supplies",
    "beauty products",
    "dating",
    "horoscope",
    "gambling",
    "casino",
    "forex trading",
    "cryptocurrency trading",
    "weight loss",
    "diet pills",
    "domain parking",
    "under construction",
    "site not found",
    "403 forbidden",
]

NON_USD_CURRENCY_SYMBOLS = ["\u20ac", "\u00a3", "\u00a5", "\u20b9", "\u20a9", "\u20bd", "\u20ba", "\u20ab", "\u20aa"]

US_PHONE_RE = re.compile(r"(?:\+1[\s.\-]?)?(?:\(?\d{3}\)?[\s.\-]?)\d{3}[\s.\-]?\d{4}\b")
US_STATE_ABBR_RE = re.compile(
    r"\b(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|"
    r"NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY|DC)\b"
)


def _normalize_domain(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    for prefix in ("https://", "http://", "www."):
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
    host = raw.split("/", 1)[0].split(":", 1)[0].strip().strip(".")
    return host


def _first_words(text: str, limit: int) -> str:
    words = re.findall(r"\S+", text or "")
    if not words:
        return ""
    return " ".join(words[:limit])


def _extract_meta_description(soup: BeautifulSoup) -> str:
    if not soup:
        return ""
    selectors = [
        {"name": re.compile(r"^description$", re.IGNORECASE)},
        {"property": re.compile(r"^og:description$", re.IGNORECASE)},
        {"name": re.compile(r"^twitter:description$", re.IGNORECASE)},
    ]
    for attrs in selectors:
        tag = soup.find("meta", attrs=attrs)
        if not tag:
            continue
        content = str(tag.get("content") or "").strip()
        if content:
            return content
    return ""


def _collect_jsonld_strings(payload, out: list[str], depth: int = 0) -> None:
    if depth > 6:
        return
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_norm = str(key or "").strip().lower()
            if key_norm in {"name", "description", "headline", "about", "keywords", "category", "slogan", "text"}:
                if isinstance(value, str) and value.strip():
                    out.append(value.strip())
            _collect_jsonld_strings(value, out, depth + 1)
    elif isinstance(payload, list):
        for item in payload:
            _collect_jsonld_strings(item, out, depth + 1)
    elif isinstance(payload, str):
        text = payload.strip()
        if text and len(text.split()) <= 24:
            out.append(text)


def _extract_structured_text(soup: BeautifulSoup) -> str:
    if not soup:
        return ""
    values: list[str] = []
    scripts = soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.IGNORECASE)})
    for script in scripts[:12]:
        raw = str(script.string or script.get_text() or "").strip()
        if not raw or len(raw) > 200_000:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        _collect_jsonld_strings(parsed, values, depth=0)
    return " ".join(values)


def _extract_heading_text(soup: BeautifulSoup) -> str:
    if not soup:
        return ""
    values = []
    for tag in soup.find_all(["h1", "h2", "h3"], limit=40):
        text = tag.get_text(" ", strip=True)
        if text:
            values.append(text)
    return " ".join(values)


def _normalize_match_text(text: str) -> str:
    lowered = str(text or "").lower()
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


_STEM_SUFFIXES = ("ing", "tion", "ment", "ness", "ity", "ive", "ous", "ful", "able", "ible", "ated", "ized", "ise", "ize")


def _word_prefix_in(prefix: str, padded: str) -> bool:
    """Check if prefix appears at the start of any word in padded text."""
    idx = 0
    target = f" {prefix}"
    while True:
        pos = padded.find(target, idx)
        if pos < 0:
            return False
        # Must be preceded by a space (guaranteed by target) — just check it's a word prefix
        after_idx = pos + len(target)
        if after_idx >= len(padded):
            return True
        after_char = padded[after_idx]
        # Match if the next char is a space (exact) or a letter (prefix of longer word)
        if after_char == " " or after_char.isalnum():
            return True
        idx = pos + 1


def _keyword_hits(haystack_lower: str, keywords: list[str]) -> list[str]:
    hay = _normalize_match_text(haystack_lower)
    if not hay:
        return []
    padded = f" {hay} "
    out: list[str] = []
    for keyword in keywords:
        token = _normalize_match_text(keyword)
        if not token:
            continue
        # Exact word boundary match
        if f" {token} " in padded:
            out.append(keyword)
            continue
        # Multi-word phrases: try exact substring, then plural/singular of last word
        words = token.split()
        if len(words) >= 2:
            if token in hay:
                out.append(keyword)
                continue
            # Try plural/singular of the last word (e.g., "case study" -> "case studies")
            last = words[-1]
            prefix = " ".join(words[:-1])
            if last.endswith("y"):
                alt_last = last[:-1] + "ies"
            elif last.endswith("s"):
                alt_last = last[:-1]
            else:
                alt_last = last + "s"
            alt_phrase = f"{prefix} {alt_last}"
            if alt_phrase in hay:
                out.append(keyword)
                continue
        # Plural/singular variations for single words
        if len(token) >= 4:
            if token.endswith("ies"):
                singular = token[:-3] + "y"
            elif token.endswith("s"):
                singular = token[:-1]
            else:
                singular = token
            plural_s = singular + "s"
            plural_ies = singular[:-1] + "ies" if singular.endswith("y") else None
            for variant in (singular, plural_s, plural_ies):
                if variant and f" {variant} " in padded:
                    out.append(keyword)
                    break
            else:
                # Word-prefix matching: "deploy" matches "deployment", "deploying", etc.
                if len(token) >= 5 and _word_prefix_in(token, padded):
                    out.append(keyword)
                    continue
                # Stem suffix stripping: "automated" -> "automat" matches "automation"
                stem = token
                for suffix in _STEM_SUFFIXES:
                    if token.endswith(suffix) and len(token) > len(suffix) + 3:
                        stem = token[: -len(suffix)]
                        break
                if stem != token and len(stem) >= 4 and _word_prefix_in(stem, padded):
                    out.append(keyword)
                    continue
    return out


def _currency_signal(text: str) -> tuple[str, bool]:
    haystack = text or ""
    has_usd = ("$" in haystack) or bool(re.search(r"\busd\b", haystack, flags=re.IGNORECASE))
    non_usd_hits = [sym for sym in NON_USD_CURRENCY_SYMBOLS if sym in haystack]
    if non_usd_hits and has_usd:
        return f"mixed:{''.join(sorted(set(non_usd_hits)))}", False
    if non_usd_hits and not has_usd:
        return f"non_usd_only:{''.join(sorted(set(non_usd_hits)))}", True
    if has_usd:
        return "usd_present", False
    return "none", False


def _normalize_reason(reason: str) -> str:
    clean = str(reason or "").strip().lower()
    clean = re.sub(r"[^a-z0-9_]+", "_", clean)
    return clean.strip("_")


def _empty_signal_result(domain: str) -> dict:
    return {
        "domain": domain,
        "html_lang": "",
        "currency_signals": "none",
        "meta_title": "",
        "meta_description": "",
        "b2b_score": 0,
        "us_signals": False,
        "website_keywords_match": True,
        "website_exclude_hits": [],
        "homepage_status": "inconclusive:fetch_failed",
        "homepage_disqualified": False,
    }


async def _fetch_homepage_excerpt(
    client: httpx.AsyncClient,
    domain: str,
    max_bytes: int = HOMEPAGE_MAX_BYTES,
) -> tuple[Optional[str], str]:
    attempts = [f"https://{domain}", f"http://{domain}"]
    if not domain.startswith("www."):
        attempts.extend([f"https://www.{domain}", f"http://www.{domain}"])
    last_status = "fetch_failed"

    for url in attempts:
        try:
            async with client.stream("GET", url, follow_redirects=True) as response:
                status_label = f"http_{response.status_code}"
                if response.status_code >= 400:
                    last_status = status_label
                    continue
                chunks: list[bytes] = []
                total = 0
                async for chunk in response.aiter_bytes():
                    if not chunk:
                        continue
                    remaining = max_bytes - total
                    if remaining <= 0:
                        break
                    chunks.append(chunk[:remaining])
                    total += min(len(chunk), remaining)
                    if total >= max_bytes:
                        break
                raw = b"".join(chunks)
                if not raw:
                    last_status = "empty_response"
                    continue
                encoding = response.encoding or "utf-8"
                return raw.decode(encoding, errors="replace"), status_label
        except httpx.TimeoutException:
            last_status = "fetch_timeout"
        except httpx.ConnectError:
            last_status = "fetch_connect_error"
        except httpx.HTTPError as exc:
            last_status = f"fetch_http_error_{_normalize_reason(type(exc).__name__)}"
        except Exception as exc:
            last_status = f"fetch_error_{_normalize_reason(type(exc).__name__)}"

    # Fallback: resolve A records with dnspython and fetch by IP + Host header.
    try:
        resolver = dns.asyncresolver.Resolver()
        resolver.timeout = 1.0
        resolver.lifetime = 3.0
        a_records = await resolver.resolve(domain, "A", lifetime=3.0)
        ips = sorted({r.address for r in a_records if getattr(r, "address", None)})
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer):
        ips = []
    except (dns.resolver.LifetimeTimeout, dns.exception.Timeout):
        ips = []
    except Exception:
        ips = []

    for ip in ips[:3]:
        for scheme in ("https", "http"):
            try:
                async with client.stream(
                    "GET",
                    f"{scheme}://{ip}",
                    headers={**HOMEPAGE_HEADERS, "Host": domain},
                    follow_redirects=False,
                ) as response:
                    status_label = f"http_{response.status_code}_via_ip"
                    if response.status_code >= 300:
                        last_status = status_label
                        continue
                    chunks: list[bytes] = []
                    total = 0
                    async for chunk in response.aiter_bytes():
                        if not chunk:
                            continue
                        remaining = max_bytes - total
                        if remaining <= 0:
                            break
                        chunks.append(chunk[:remaining])
                        total += min(len(chunk), remaining)
                        if total >= max_bytes:
                            break
                    raw = b"".join(chunks)
                    if not raw:
                        last_status = "empty_response_via_ip"
                        continue
                    encoding = response.encoding or "utf-8"
                    return raw.decode(encoding, errors="replace"), status_label
            except Exception:
                continue

    return None, last_status


def _compute_homepage_signals(
    domain: str,
    html: str,
    website_keywords: list[str],
    website_exclude_keywords: Optional[list[str]] = None,
) -> dict:
    soup = BeautifulSoup(html or "", "lxml")

    html_lang = ""
    if soup.html:
        html_lang = str(soup.html.get("lang") or "").strip().lower()

    structured_text = _extract_structured_text(soup)
    heading_text = _extract_heading_text(soup)

    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.extract()

    meta_title = soup.title.get_text(" ", strip=True) if soup.title else ""
    meta_description = _extract_meta_description(soup)
    body_text = soup.get_text(" ", strip=True)
    first_3000_words = _first_words(body_text, 3000)
    b2b_text = " ".join([meta_title, meta_description, heading_text, structured_text, first_3000_words]).strip()
    b2b_text_lower = b2b_text.lower()
    signal_text = " ".join([meta_title, meta_description, heading_text, structured_text, body_text]).strip()
    signal_text_lower = signal_text.lower()

    b2b_hits = _keyword_hits(b2b_text_lower, B2B_POSITIVE_KEYWORDS)
    disqualify_hits = _keyword_hits(signal_text_lower, DISQUALIFY_SIGNAL_KEYWORDS)
    website_hits = _keyword_hits(signal_text_lower, website_keywords)
    exclude_kws = website_exclude_keywords or []
    website_exclude_hits = _keyword_hits(signal_text_lower, exclude_kws) if exclude_kws else []
    currency_signals, currency_disqualify = _currency_signal(signal_text)
    us_signals = bool(
        US_PHONE_RE.search(signal_text)
        or US_STATE_ABBR_RE.search(signal_text)
        or re.search(r"\b(united states|u\.s\.|usa)\b", signal_text, flags=re.IGNORECASE)
    )

    b2b_score = len(set([str(hit).lower() for hit in b2b_hits]))
    consumer_score = len(set([str(hit).lower() for hit in disqualify_hits]))
    hard_disqualify_reasons: list[str] = []
    soft_reasons: list[str] = []
    soft_strikes = 0

    def add_soft_reason(reason: str, weight: int = 1) -> None:
        nonlocal soft_strikes
        token = str(reason or "").strip()
        if not token:
            return
        soft_reasons.append(token)
        soft_strikes += max(1, int(weight))

    # Non-English pages are not automatically disqualified, but are lower confidence.
    if html_lang and not html_lang.startswith("en"):
        add_soft_reason("html_lang_not_en", weight=1)

    # Currency alone is noisy; only mark as lower confidence.
    if currency_disqualify and not us_signals and b2b_score == 0:
        add_soft_reason("non_usd_currency_without_usd", weight=1)

    # Strong consumer/ecommerce signatures with no B2B evidence are disqualifying.
    if consumer_score >= 2 and b2b_score == 0:
        hard_disqualify_reasons.append(f"consumer_signal_{_normalize_reason(disqualify_hits[0])}")
    elif consumer_score > 0:
        add_soft_reason(f"consumer_signal_{_normalize_reason(disqualify_hits[0])}", weight=1)

    # User-specified exclusion keywords: hard disqualify if any match.
    if website_exclude_hits:
        hard_disqualify_reasons.append(f"exclude_keyword_{_normalize_reason(website_exclude_hits[0])}")

    # Keyword mismatch is important user intent, but still treated as a weighted soft strike.
    if website_keywords and not website_hits:
        add_soft_reason("website_keywords_no_match", weight=2)

    # Low-information pages should be inconclusive, not hard-negative.
    if not website_keywords and b2b_score == 0 and consumer_score == 0:
        add_soft_reason("limited_b2b_signals", weight=1)

    if hard_disqualify_reasons:
        status = f"disqualified:{','.join(hard_disqualify_reasons)}"
        disqualified = True
    elif soft_strikes >= SOFT_DISQUALIFY_STRIKE_THRESHOLD:
        joined = ",".join(soft_reasons) if soft_reasons else "soft_signal_stack"
        status = f"disqualified:soft_strikes_{soft_strikes}:{joined}"
        disqualified = True
    elif soft_reasons:
        status = f"inconclusive:soft_strikes_{soft_strikes}:{','.join(soft_reasons)}"
        disqualified = False
    else:
        status = "eligible"
        disqualified = False

    return {
        "domain": domain,
        "html_lang": html_lang,
        "currency_signals": currency_signals,
        "meta_title": meta_title,
        "meta_description": meta_description,
        "b2b_score": b2b_score,
        "us_signals": us_signals,
        "website_keywords_match": True if not website_keywords else bool(website_hits),
        "website_exclude_hits": website_exclude_hits,
        "homepage_status": status,
        "homepage_disqualified": disqualified,
    }


async def collect_domain_homepage_signals(
    client: httpx.AsyncClient,
    domain: str,
    website_keywords: list[str],
    website_exclude_keywords: Optional[list[str]] = None,
) -> dict:
    clean = _normalize_domain(domain)
    if not clean or "." not in clean:
        result = _empty_signal_result(domain)
        result["homepage_status"] = "disqualified:invalid_domain"
        result["homepage_disqualified"] = True
        result["website_keywords_match"] = False
        return result

    html, fetch_status = await _fetch_homepage_excerpt(client, clean, max_bytes=HOMEPAGE_MAX_BYTES)
    if html is None:
        result = _empty_signal_result(domain)
        result["homepage_status"] = f"inconclusive:{fetch_status}"
        result["homepage_disqualified"] = False
        return result

    return _compute_homepage_signals(
        domain, html,
        website_keywords=website_keywords,
        website_exclude_keywords=website_exclude_keywords,
    )


async def collect_homepage_signals_batch(
    domains: list[str],
    website_keywords: Optional[list[str]] = None,
    website_exclude_keywords: Optional[list[str]] = None,
    concurrency: int = DEFAULT_HOMEPAGE_CONCURRENCY,
    timeout_seconds: float = DEFAULT_HOMEPAGE_TIMEOUT_SECONDS,
    progress_callback: Optional[Callable[[int, int], None]] = None,
    should_stop: Optional[Callable[[], bool]] = None,
    result_callback: Optional[Callable[[str, dict], None]] = None,
) -> dict[str, dict]:
    if not domains:
        return {}

    await init_cache()

    normalized_keywords = [
        str(word or "").strip().lower()
        for word in (website_keywords or [])
        if str(word or "").strip()
    ]
    normalized_exclude = [
        str(word or "").strip().lower()
        for word in (website_exclude_keywords or [])
        if str(word or "").strip()
    ]
    # Include exclude keywords in cache signature so cache invalidates when they change
    all_kw_for_sig = normalized_keywords + ["|EXCLUDE|"] + normalized_exclude
    keywords_sig = hashlib.sha1("\x1f".join(all_kw_for_sig).encode("utf-8")).hexdigest()

    unique_domains: list[str] = []
    seen_domains: set[str] = set()
    for value in domains:
        clean = _normalize_domain(str(value or ""))
        if not clean or clean in seen_domains:
            continue
        seen_domains.add(clean)
        unique_domains.append(clean)
    if not unique_domains:
        return {}

    cached = await get_cached_homepages_batch(unique_domains, keywords_sig=keywords_sig)
    out: dict[str, dict] = {str(domain): result for domain, result in cached.items() if isinstance(result, dict)}
    if result_callback:
        for domain, result in out.items():
            result_callback(domain, result)
    fetch_domains = [domain for domain in unique_domains if domain not in out]
    total = len(unique_domains)
    processed = len(out)
    if progress_callback:
        progress_callback(processed, total)

    if not fetch_domains:
        return out

    max_connections = min(max(int(concurrency or DEFAULT_HOMEPAGE_CONCURRENCY), 20), 120)
    sem = asyncio.Semaphore(max_connections)
    timeout = httpx.Timeout(timeout_seconds, connect=timeout_seconds, read=timeout_seconds, write=timeout_seconds)
    limits = httpx.Limits(max_connections=max_connections, max_keepalive_connections=max(20, max_connections // 2))

    async with httpx.AsyncClient(
        headers=HOMEPAGE_HEADERS,
        timeout=timeout,
        limits=limits,
        follow_redirects=True,
        verify=False,
    ) as client:

        async def _bounded(domain_value: str) -> dict:
            async with sem:
                return await collect_domain_homepage_signals(
                    client=client,
                    domain=domain_value,
                    website_keywords=normalized_keywords,
                    website_exclude_keywords=normalized_exclude,
                )

        tasks = [asyncio.create_task(_bounded(domain)) for domain in fetch_domains]
        pending = set(tasks)
        for future in asyncio.as_completed(tasks):
            if should_stop and should_stop():
                for task in pending:
                    if not task.done():
                        task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                break
            try:
                result = await future
                if isinstance(result, dict) and result.get("domain") is not None:
                    domain = str(result["domain"])
                    out[domain] = result
                    await set_cached_homepage(domain=domain, keywords_sig=keywords_sig, result=result)
                    if result_callback:
                        result_callback(domain, result)
            except Exception:
                pass
            finally:
                pending.discard(future)

            processed += 1
            if progress_callback:
                progress_callback(processed, total)
        return out
