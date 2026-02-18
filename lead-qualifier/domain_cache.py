"""
Domain validation caching layer using SQLite.
Provides async interface for caching domain validation results across sessions.
"""

import aiosqlite
from datetime import datetime, timedelta
from typing import Optional
import os
import json
from pathlib import Path


def _runtime_data_dir() -> Path:
    raw = str(os.getenv("HOUND_DATA_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path(__file__).resolve().parent


def _cache_db_path() -> str:
    data_dir = _runtime_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    return str(data_dir / "domain_cache.db")

# TTL settings
VALID_DOMAIN_TTL_DAYS = 7  # Cache valid domains for 7 days
DEAD_DOMAIN_TTL_HOURS = 24  # Cache dead domains for 24 hours
HOMEPAGE_TTL_HOURS = 72  # Cache homepage scrape results for 3 days


def _deserialize_ips(raw: Optional[str]) -> list[str]:
    value = str(raw or "").strip()
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _serialize_ips(ips: list[str]) -> str:
    return ",".join([str(ip).strip() for ip in (ips or []) if str(ip).strip()])


def _safe_parse_checked_at(value: str) -> Optional[datetime]:
    """Parse checked_at timestamps defensively for backward compatibility."""
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt)
        except Exception:
            continue
    return None


async def init_cache():
    """Initialize the domain cache database with schema."""
    async with aiosqlite.connect(_cache_db_path()) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS domain_cache (
                domain TEXT PRIMARY KEY,
                has_mx BOOLEAN NOT NULL,
                has_a_record BOOLEAN NOT NULL,
                is_alive BOOLEAN NOT NULL,
                status TEXT NOT NULL,
                resolved_ips TEXT NOT NULL DEFAULT '',
                geo_status TEXT NOT NULL DEFAULT 'not_checked',
                geo_country TEXT NOT NULL DEFAULT '',
                geo_inconclusive BOOLEAN NOT NULL DEFAULT 0,
                checked_at TIMESTAMP NOT NULL
            )
        """)
        # Backward-compatible migration for existing cache DBs.
        async with db.execute("PRAGMA table_info(domain_cache)") as cursor:
            existing = {str(row[1]).lower() async for row in cursor}
        if "resolved_ips" not in existing:
            await db.execute("ALTER TABLE domain_cache ADD COLUMN resolved_ips TEXT NOT NULL DEFAULT ''")
        if "geo_status" not in existing:
            await db.execute("ALTER TABLE domain_cache ADD COLUMN geo_status TEXT NOT NULL DEFAULT 'not_checked'")
        if "geo_country" not in existing:
            await db.execute("ALTER TABLE domain_cache ADD COLUMN geo_country TEXT NOT NULL DEFAULT ''")
        if "geo_inconclusive" not in existing:
            await db.execute("ALTER TABLE domain_cache ADD COLUMN geo_inconclusive BOOLEAN NOT NULL DEFAULT 0")
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_checked_at
            ON domain_cache(checked_at)
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS homepage_cache (
                cache_key TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                keywords_sig TEXT NOT NULL,
                result_json TEXT NOT NULL,
                checked_at TIMESTAMP NOT NULL
            )
        """)
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_homepage_checked_at
            ON homepage_cache(checked_at)
        """)
        await db.commit()


async def get_cached_domain(domain: str) -> Optional[dict]:
    """
    Retrieve cached domain validation result if not expired.

    Returns:
        dict with {has_mx, has_a_record, is_alive, status, checked_at} or None if not cached/expired
    """
    async with aiosqlite.connect(_cache_db_path()) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT
                domain, has_mx, has_a_record, is_alive, status,
                resolved_ips, geo_status, geo_country, geo_inconclusive, checked_at
            FROM domain_cache
            WHERE domain = ?
        """, (domain.lower(),)) as cursor:
            row = await cursor.fetchone()

            if not row:
                return None

            # Check if cache is expired
            checked_at = _safe_parse_checked_at(row["checked_at"])
            if not checked_at:
                return None
            now = datetime.now()

            # Different TTL for alive vs dead domains
            if row["is_alive"]:
                ttl = timedelta(days=VALID_DOMAIN_TTL_DAYS)
            else:
                ttl = timedelta(hours=DEAD_DOMAIN_TTL_HOURS)

            if now - checked_at > ttl:
                # Cache expired
                return None

            return {
                "domain": row["domain"],
                "has_mx": bool(row["has_mx"]),
                "has_a_record": bool(row["has_a_record"]),
                "is_alive": bool(row["is_alive"]),
                "status": row["status"],
                "resolved_ips": _deserialize_ips(row["resolved_ips"]),
                "geo_status": row["geo_status"] or "not_checked",
                "geo_country": row["geo_country"] or "",
                "geo_inconclusive": bool(row["geo_inconclusive"]),
                "checked_at": row["checked_at"]
            }


async def set_cached_domain(
    domain: str,
    has_mx: bool,
    has_a_record: bool,
    is_alive: bool,
    status: str,
    resolved_ips: Optional[list[str]] = None,
    geo_status: str = "not_checked",
    geo_country: str = "",
    geo_inconclusive: bool = False,
):
    """
    Store domain validation result in cache.

    Args:
        domain: Domain name (will be lowercased)
        has_mx: Whether domain has MX records
        has_a_record: Whether domain has A record
        is_alive: Overall liveness status
        status: Status message
    """
    async with aiosqlite.connect(_cache_db_path()) as db:
        await db.execute("""
            INSERT OR REPLACE INTO domain_cache
            (
                domain, has_mx, has_a_record, is_alive, status,
                resolved_ips, geo_status, geo_country, geo_inconclusive, checked_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            domain.lower(),
            has_mx,
            has_a_record,
            is_alive,
            status,
            _serialize_ips(resolved_ips or []),
            geo_status or "not_checked",
            geo_country or "",
            bool(geo_inconclusive),
            datetime.now().isoformat()
        ))
        await db.commit()


async def get_cached_domains_batch(domains: list[str]) -> dict:
    """
    Retrieve multiple cached domains in one query.

    Returns:
        dict mapping domain -> {has_mx, has_a_record, is_alive, status} for cached entries
    """
    if not domains:
        return {}

    # Lowercase all domains for lookup
    domain_map = {d.lower(): d for d in domains}
    lowercase_domains = list(domain_map.keys())

    async with aiosqlite.connect(_cache_db_path()) as db:
        db.row_factory = aiosqlite.Row
        placeholders = ",".join("?" * len(lowercase_domains))
        query = f"""
            SELECT
                domain, has_mx, has_a_record, is_alive, status,
                resolved_ips, geo_status, geo_country, geo_inconclusive, checked_at
            FROM domain_cache
            WHERE domain IN ({placeholders})
        """

        results = {}
        now = datetime.now()

        async with db.execute(query, lowercase_domains) as cursor:
            async for row in cursor:
                # Check expiration
                checked_at = _safe_parse_checked_at(row["checked_at"])
                if not checked_at:
                    continue

                if row["is_alive"]:
                    ttl = timedelta(days=VALID_DOMAIN_TTL_DAYS)
                else:
                    ttl = timedelta(hours=DEAD_DOMAIN_TTL_HOURS)

                if now - checked_at <= ttl:
                    # Use original casing from input
                    original_domain = domain_map[row["domain"]]
                    results[original_domain] = {
                        "has_mx": bool(row["has_mx"]),
                        "has_a_record": bool(row["has_a_record"]),
                        "is_alive": bool(row["is_alive"]),
                        "status": row["status"],
                        "resolved_ips": _deserialize_ips(row["resolved_ips"]),
                        "geo_status": row["geo_status"] or "not_checked",
                        "geo_country": row["geo_country"] or "",
                        "geo_inconclusive": bool(row["geo_inconclusive"]),
                    }

        return results


async def clear_expired_cache():
    """Remove expired entries from cache to keep database size manageable."""
    async with aiosqlite.connect(_cache_db_path()) as db:
        now = datetime.now()
        valid_cutoff = (now - timedelta(days=VALID_DOMAIN_TTL_DAYS)).isoformat()
        dead_cutoff = (now - timedelta(hours=DEAD_DOMAIN_TTL_HOURS)).isoformat()
        homepage_cutoff = (now - timedelta(hours=HOMEPAGE_TTL_HOURS)).isoformat()

        await db.execute("""
            DELETE FROM domain_cache
            WHERE (is_alive = 1 AND checked_at < ?)
               OR (is_alive = 0 AND checked_at < ?)
        """, (valid_cutoff, dead_cutoff))
        await db.execute("""
            DELETE FROM homepage_cache
            WHERE checked_at < ?
        """, (homepage_cutoff,))

        await db.commit()


async def clear_all_cache():
    """Clear all cached domain entries. Useful for manual cache invalidation."""
    async with aiosqlite.connect(_cache_db_path()) as db:
        await db.execute("DELETE FROM domain_cache")
        await db.execute("DELETE FROM homepage_cache")
        await db.commit()


async def get_cache_stats() -> dict:
    """Get cache statistics for monitoring."""
    async with aiosqlite.connect(_cache_db_path()) as db:
        db.row_factory = aiosqlite.Row

        # Total entries
        async with db.execute("SELECT COUNT(*) as count FROM domain_cache") as cursor:
            row = await cursor.fetchone()
            total = row["count"]

        # Alive vs dead breakdown
        async with db.execute("SELECT is_alive, COUNT(*) as count FROM domain_cache GROUP BY is_alive") as cursor:
            breakdown = {}
            async for row in cursor:
                breakdown["alive" if row["is_alive"] else "dead"] = row["count"]

        # Expired count
        now = datetime.now()
        valid_cutoff = (now - timedelta(days=VALID_DOMAIN_TTL_DAYS)).isoformat()
        dead_cutoff = (now - timedelta(hours=DEAD_DOMAIN_TTL_HOURS)).isoformat()

        async with db.execute("""
            SELECT COUNT(*) as count FROM domain_cache
            WHERE (is_alive = 1 AND checked_at < ?)
               OR (is_alive = 0 AND checked_at < ?)
        """, (valid_cutoff, dead_cutoff)) as cursor:
            row = await cursor.fetchone()
            expired = row["count"]

        async with db.execute("SELECT COUNT(*) as count FROM homepage_cache") as cursor:
            row = await cursor.fetchone()
            homepage_total = row["count"]

        homepage_cutoff = (now - timedelta(hours=HOMEPAGE_TTL_HOURS)).isoformat()
        async with db.execute(
            "SELECT COUNT(*) as count FROM homepage_cache WHERE checked_at < ?",
            (homepage_cutoff,),
        ) as cursor:
            row = await cursor.fetchone()
            homepage_expired = row["count"]

        return {
            "total_entries": total,
            "alive_domains": breakdown.get("alive", 0),
            "dead_domains": breakdown.get("dead", 0),
            "expired_entries": expired,
            "homepage_entries": homepage_total,
            "homepage_expired_entries": homepage_expired,
        }


def _homepage_cache_key(domain: str, keywords_sig: str) -> str:
    return f"{str(domain or '').strip().lower()}|{str(keywords_sig or '').strip()}"


async def get_cached_homepages_batch(domains: list[str], keywords_sig: str) -> dict[str, dict]:
    """
    Retrieve cached homepage scrape results for a domain list + keyword signature.
    Returns {domain: result_dict} for non-expired cache rows.
    """
    clean_domains = []
    seen = set()
    for domain in domains or []:
        token = str(domain or "").strip().lower()
        if not token or token in seen:
            continue
        seen.add(token)
        clean_domains.append(token)
    if not clean_domains:
        return {}

    keys = [_homepage_cache_key(domain, keywords_sig) for domain in clean_domains]
    placeholders = ",".join("?" * len(keys))
    query = f"""
        SELECT cache_key, domain, result_json, checked_at
        FROM homepage_cache
        WHERE cache_key IN ({placeholders})
    """

    out: dict[str, dict] = {}
    now = datetime.now()
    ttl = timedelta(hours=HOMEPAGE_TTL_HOURS)
    async with aiosqlite.connect(_cache_db_path()) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, keys) as cursor:
            async for row in cursor:
                checked_at = _safe_parse_checked_at(row["checked_at"])
                if not checked_at or (now - checked_at) > ttl:
                    continue
                try:
                    parsed = json.loads(str(row["result_json"] or "{}"))
                except Exception:
                    continue
                if isinstance(parsed, dict):
                    out[str(row["domain"]).strip().lower()] = parsed
    return out


async def set_cached_homepage(domain: str, keywords_sig: str, result: dict):
    """Store homepage scrape result under domain + keyword signature."""
    clean_domain = str(domain or "").strip().lower()
    if not clean_domain:
        return
    payload = result if isinstance(result, dict) else {}
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    key = _homepage_cache_key(clean_domain, keywords_sig)
    async with aiosqlite.connect(_cache_db_path()) as db:
        await db.execute("""
            INSERT OR REPLACE INTO homepage_cache
            (cache_key, domain, keywords_sig, result_json, checked_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            key,
            clean_domain,
            str(keywords_sig or "").strip(),
            encoded,
            datetime.now().isoformat(),
        ))
        await db.commit()
