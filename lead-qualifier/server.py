#!/usr/bin/env python3
"""
Lead Qualifier API — FastAPI + Polars + RapidFuzz
Upload CSVs, define ICP rules, fuzzy-match, and export qualified leads.
Includes async concurrent domain liveness verification.
"""

import asyncio
import argparse
import io
import json
import os
import pickle
import re
import shutil
import ssl
import sys
import tempfile
import time
import traceback
from datetime import datetime, timezone
from uuid import uuid4
from typing import Any, Optional
from pathlib import Path
from urllib.parse import urlsplit

import aiohttp
import polars as pl
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from rapidfuzz import fuzz

# Import DNS-based domain validation
from domain_validator import check_domains_dns_batch, get_cdn_reference_data
from domain_cache import (
    init_cache, get_cache_stats, clear_all_cache,
    get_cached_scrapes_batch, set_cached_scrapes_batch,
    clear_scrape_cache, get_scrape_cache_stats,
)
from homepage_signals import collect_homepage_signals_batch
from scraper.pipeline import (
    Target as ScrapeTarget,
    build_paths as build_scrape_paths,
    run_phase1_async as run_scrape_phase1_async,
    run_phase2_fallback as run_scrape_phase2_fallback,
    extract_keywords as extract_scrape_keywords,
)

app = FastAPI(title="Kennel — Hound Suite")

APP_BASE_DIR = Path(__file__).resolve().parent


def _resolve_data_dir_from_env() -> Path:
    raw = str(os.getenv("HOUND_DATA_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser()
    if getattr(sys, "frozen", False):
        return Path.home() / ".hound-qualifier"
    return APP_BASE_DIR


DATA_DIR = _resolve_data_dir_from_env()

STATIC_DIR = APP_BASE_DIR / "static"
FONTS_DIR = APP_BASE_DIR / "resources" / "fonts"
SESSION_TTL_SECONDS = 60 * 60 * 24 * 30
SESSION_STORE: dict[str, dict] = {}
SESSION_STORE_DIR = DATA_DIR / "session_store"
SESSION_FILE_SUFFIX = ".session.pkl"
APP_BOOT_TS = time.time()


def _refresh_data_paths_from_env() -> None:
    global DATA_DIR, SESSION_STORE_DIR, SCRAPE_JOB_DIR
    DATA_DIR = _resolve_data_dir_from_env()
    SESSION_STORE_DIR = DATA_DIR / "session_store"
    SCRAPE_JOB_DIR = DATA_DIR / "scrape_jobs"

UPLOAD_CHUNK_SIZE = 1024 * 1024
MAX_UPLOAD_BYTES = 1024 * 1024 * 1024  # 1 GB limit (supports 600k+ rows)
PREVIEW_ROW_LIMIT = 8
TYPE_SAMPLE_LIMIT = 250
VALUE_SAMPLE_SCAN_LIMIT = 5000
VALUE_SAMPLE_LIMIT = 50

_EMAIL_TYPE_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
URL_RE = re.compile(r"^(?:https?://|www\.)", re.IGNORECASE)
DOMAIN_RE = re.compile(r"^[a-z0-9][a-z0-9.-]+\.[a-z]{2,}(?:/.*)?$", re.IGNORECASE)
NUMERIC_RE = re.compile(r"^-?\d+(?:[.,]\d+)?$")
BLOCKED_DOMAIN_CATEGORIES = {
    "blogs": [
        "wordpress.com", "blogspot.com", "medium.com", "ghost.io",
        "substack.com", "tumblr.com", "wixsite.com", "weebly.com",
        "squarespace.com", "typepad.com", "blogger.com", "hubpages.com",
    ],
    "dev_hosting": [
        "github.io", "github.com", "gitlab.io", "gitlab.com",
        "netlify.app", "vercel.app", "herokuapp.com", "fly.dev",
        "render.com", "railway.app", "repl.co", "replit.com",
        "stackblitz.com", "codepen.io", "codesandbox.io",
        "pages.dev", "workers.dev", "surge.sh",
    ],
    "social": [
        "facebook.com", "instagram.com", "twitter.com", "x.com",
        "linkedin.com", "tiktok.com", "pinterest.com", "reddit.com",
        "youtube.com", "twitch.tv", "discord.gg", "discord.com",
        "snapchat.com", "threads.net",
    ],
    "parked": [
        "example.com", "example.org", "example.net", "test.com",
        "localhost", "0.0.0.0", "127.0.0.1",
        "godaddy.com", "sedo.com", "afternic.com", "dan.com",
        "namecheap.com", "hover.com",
    ],
    "email": [
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
        "aol.com", "icloud.com", "mail.com", "protonmail.com",
        "zoho.com", "yandex.com", "gmx.com", "fastmail.com",
    ],
    "marketplaces": [
        "myshopify.com", "shopify.com", "etsy.com", "amazon.com",
        "ebay.com", "alibaba.com", "aliexpress.com", "wish.com",
        "bigcartel.com",
    ],
}

# Country name normalization for geo_country match type
COUNTRY_ALIASES = {
    "us": "US", "usa": "US", "united states": "US", "united states of america": "US", "america": "US",
    "uk": "GB", "gb": "GB", "united kingdom": "GB", "great britain": "GB", "england": "GB",
    "ca": "CA", "canada": "CA",
    "au": "AU", "australia": "AU",
    "de": "DE", "germany": "DE", "deutschland": "DE",
    "fr": "FR", "france": "FR",
    "es": "ES", "spain": "ES", "españa": "ES",
    "it": "IT", "italy": "IT", "italia": "IT",
    "nl": "NL", "netherlands": "NL", "holland": "NL",
    "se": "SE", "sweden": "SE",
    "no": "NO", "norway": "NO",
    "dk": "DK", "denmark": "DK",
    "fi": "FI", "finland": "FI",
    "ie": "IE", "ireland": "IE",
    "nz": "NZ", "new zealand": "NZ",
    "sg": "SG", "singapore": "SG",
    "jp": "JP", "japan": "JP",
    "kr": "KR", "south korea": "KR", "korea": "KR",
    "in": "IN", "india": "IN",
    "br": "BR", "brazil": "BR",
    "mx": "MX", "mexico": "MX",
    "il": "IL", "israel": "IL",
    "ch": "CH", "switzerland": "CH",
    "at": "AT", "austria": "AT",
    "be": "BE", "belgium": "BE",
    "pt": "PT", "portugal": "PT",
    "pl": "PL", "poland": "PL",
    "cz": "CZ", "czech republic": "CZ", "czechia": "CZ",
}

def normalize_country(value: str) -> str:
    """Normalize a country name/code to a 2-letter ISO code."""
    v = str(value or "").strip().lower()
    return COUNTRY_ALIASES.get(v, v.upper()[:2] if len(v) == 2 else v)

RESOLVED_IPS_COLUMN = "resolved_ips"
HTML_LANG_COLUMN = "html_lang"
CURRENCY_SIGNALS_COLUMN = "currency_signals"
META_TITLE_COLUMN = "meta_title"
META_DESCRIPTION_COLUMN = "meta_description"
B2B_SCORE_COLUMN = "b2b_score"
US_SIGNALS_COLUMN = "us_signals"
WEBSITE_KEYWORDS_MATCH_COLUMN = "website_keywords_match"
HOMEPAGE_STATUS_COLUMN = "homepage_status"
SCRAPE_JOB_DIR = DATA_DIR / "scrape_jobs"
SCRAPE_PHASE1_CONCURRENCY = int(os.getenv("HOUND_SCRAPE_PHASE1_CONCURRENCY", "700"))
SCRAPE_PHASE1_TIMEOUT = float(os.getenv("HOUND_SCRAPE_PHASE1_TIMEOUT", "8.0"))
SCRAPE_PHASE1_RETRY = int(os.getenv("HOUND_SCRAPE_PHASE1_RETRY", "2"))
SCRAPE_PHASE2_CONCURRENCY = int(os.getenv("HOUND_SCRAPE_PHASE2_CONCURRENCY", "80"))
SCRAPE_PHASE2_TIMEOUT = float(os.getenv("HOUND_SCRAPE_PHASE2_TIMEOUT", "15.0"))
SCRAPE_KEYWORD_TOP_K = int(os.getenv("HOUND_SCRAPE_KEYWORD_TOP_K", "20"))
SCRAPE_COLUMN_MAP = {
    "domain": "scrape_domain",
    "url": "scrape_url",
    "status": "scrape_status",
    "http_status": "scrape_http_status",
    "title": "scrape_title",
    "meta_description": "scrape_meta_description",
    "og_title": "scrape_og_title",
    "og_description": "scrape_og_description",
    "h1_h3": "scrape_h1_h3",
    "body_text": "scrape_body_text",
    "scraped_at": "scrape_scraped_at",
    "error": "scrape_error",
    "phase": "scrape_phase",
    "scraped_keywords": "scrape_keywords",
}
SCRAPE_ENRICH_COLUMNS = list(SCRAPE_COLUMN_MAP.values())


@app.get("/")
async def root():
    return RedirectResponse("/static/index.html")


@app.get("/api/health")
async def health():
    """Lightweight readiness probe for desktop shell startup checks."""
    return {
        "ok": True,
        "app": "hound-suite",
        "uptimeSeconds": round(max(0.0, time.time() - APP_BOOT_TS), 3),
        "dataDir": str(DATA_DIR),
    }


@app.on_event("startup")
async def startup_event():
    """Initialize domain cache on server startup."""
    _prepare_runtime_data_layout()
    await init_cache()
    _load_persisted_sessions()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def no_cache_static_assets(request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path.startswith("/static/") and (path.endswith(".jsx") or path.endswith(".css") or path.endswith(".html")):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _prepare_runtime_data_layout() -> None:
    """
    Ensure writable runtime storage exists and migrate legacy local files on first run.
    """
    _refresh_data_paths_from_env()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SESSION_STORE_DIR.mkdir(parents=True, exist_ok=True)
    SCRAPE_JOB_DIR.mkdir(parents=True, exist_ok=True)
    os.environ["HOUND_DATA_DIR"] = str(DATA_DIR)

    if DATA_DIR == APP_BASE_DIR:
        return

    legacy_cache_db = APP_BASE_DIR / "domain_cache.db"
    target_cache_db = DATA_DIR / "domain_cache.db"
    if legacy_cache_db.exists() and not target_cache_db.exists():
        try:
            shutil.copy2(legacy_cache_db, target_cache_db)
        except Exception:
            traceback.print_exc()

    legacy_session_store = APP_BASE_DIR / "session_store"
    if legacy_session_store.exists() and legacy_session_store.is_dir():
        for path in legacy_session_store.glob(f"*{SESSION_FILE_SUFFIX}"):
            target = SESSION_STORE_DIR / path.name
            if target.exists():
                continue
            try:
                shutil.copy2(path, target)
            except Exception:
                traceback.print_exc()

def ensure_csv_filename(file_name: Optional[str]) -> None:
    """Validate incoming file extension for CSV-focused flows."""
    if not file_name:
        return
    lower = file_name.lower()
    if not (lower.endswith(".csv") or lower.endswith(".txt") or lower.endswith(".tsv")):
        raise HTTPException(status_code=400, detail="Only CSV/TSV uploads are supported.")


async def read_upload_bytes(file: UploadFile, max_bytes: int = MAX_UPLOAD_BYTES) -> bytes:
    """Read uploaded file in chunks with explicit size guard."""
    ensure_csv_filename(file.filename)
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = await file.read(UPLOAD_CHUNK_SIZE)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(
                status_code=413,
                detail=f"Upload exceeds {max_bytes // (1024 * 1024)}MB limit.",
            )
        chunks.append(chunk)
    raw = b"".join(chunks)
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    return raw


def detect_csv_separator(raw: bytes) -> str:
    """Infer likely CSV delimiter from early lines."""
    probe = raw[:8192].decode("utf-8-sig", errors="replace")
    lines = [ln for ln in probe.splitlines()[:5] if ln.strip()]
    if not lines:
        return ","
    candidates = [",", ";", "\t", "|"]
    scores = {}
    for sep in candidates:
        counts = [line.count(sep) for line in lines]
        scores[sep] = (sum(counts), max(counts) if counts else 0)
    best = max(candidates, key=lambda sep: (scores[sep][0], scores[sep][1]))
    return best if scores[best][0] > 0 else ","


def sanitize_column_names(columns: list[str]) -> list[str]:
    """Normalize blank/duplicate headers while preserving readability."""
    seen: dict[str, int] = {}
    normalized = []
    for idx, raw_name in enumerate(columns, start=1):
        base = (raw_name or "").strip()
        if not base:
            base = f"column_{idx}"
        key = base.lower()
        count = seen.get(key, 0) + 1
        seen[key] = count
        normalized.append(base if count == 1 else f"{base}_{count}")
    return normalized


def parse_numeric_value(value: str) -> Optional[float]:
    """Parse numeric-like strings across common CSV formats."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    cleaned = raw.replace("$", "").replace("%", "").replace(" ", "")
    if cleaned.count(",") > 0 and cleaned.count(".") > 0:
        if cleaned.rfind(",") > cleaned.rfind("."):
            # EU style: 1.234,56
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            # US style: 1,234.56
            cleaned = cleaned.replace(",", "")
    elif cleaned.count(",") > 0 and cleaned.count(".") == 0:
        # Could be decimal comma or thousands separator.
        if cleaned.count(",") == 1 and len(cleaned.split(",")[1]) <= 2:
            cleaned = cleaned.replace(",", ".")
        else:
            cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except Exception:
        return None


def looks_like_date(value: str) -> bool:
    """Check whether a scalar resembles a date/time string."""
    raw = str(value).strip()
    if not raw:
        return False
    sample = raw.replace("/", "-").replace(".", "-")
    fmts = (
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
    )
    for fmt in fmts:
        try:
            datetime.strptime(sample, fmt)
            return True
        except Exception:
            continue
    return False


def sample_distinct_values(series: pl.Series, limit: int = VALUE_SAMPLE_LIMIT, scan_limit: int = VALUE_SAMPLE_SCAN_LIMIT) -> list[str]:
    """Collect an ordered distinct sample without materializing full unique sets."""
    out: list[str] = []
    seen: set[str] = set()
    for value in series.head(scan_limit).to_list():
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        out.append(text)
        if len(out) >= limit:
            break
    return out


def read_csv_bytes(raw: bytes) -> pl.DataFrame:
    """Read CSV bytes with delimiter detection and resilient parsing."""
    separator_hint = detect_csv_separator(raw)
    separators = [separator_hint] + [sep for sep in [",", ";", "\t", "|"] if sep != separator_hint]
    read_errors = []
    parsed_candidates: list[tuple[str, pl.DataFrame]] = []

    for sep in separators:
        try:
            df = pl.read_csv(
                io.BytesIO(raw),
                separator=sep,
                infer_schema_length=500,
                try_parse_dates=False,
                truncate_ragged_lines=True,
                ignore_errors=True,
                null_values=["", "null", "NULL", "n/a", "N/A", "na", "NA"],
            )
            if df.width > 0:
                parsed_candidates.append((sep, df))
        except Exception as exc:
            read_errors.append(str(exc))

    if parsed_candidates:
        # Prefer parses with more than one column; among those pick the widest schema.
        multi_col = [(sep, df) for sep, df in parsed_candidates if df.width > 1]
        chosen_sep, chosen_df = max(
            multi_col if multi_col else parsed_candidates,
            key=lambda item: item[1].width,
        )
        _ = chosen_sep  # Reserved for future diagnostics.
        cleaned_names = sanitize_column_names(chosen_df.columns)
        if cleaned_names != chosen_df.columns:
            chosen_df = chosen_df.rename(dict(zip(chosen_df.columns, cleaned_names)))
        return chosen_df

    # Final fallback with forgiving UTF-8 decode.
    text = raw.decode("utf-8-sig", errors="replace")
    df = pl.read_csv(
        io.StringIO(text),
        infer_schema_length=500,
        try_parse_dates=False,
        truncate_ragged_lines=True,
        ignore_errors=True,
    )
    cleaned_names = sanitize_column_names(df.columns)
    if cleaned_names != df.columns:
        df = df.rename(dict(zip(df.columns, cleaned_names)))
    if df.width == 0:
        raise HTTPException(status_code=400, detail=f"Unable to parse CSV. {read_errors[:1]}")
    return df


def infer_column_type(col_name: str, series: pl.Series) -> str:
    """Infer a lightweight semantic type for UI previews."""
    name = (col_name or "").lower()
    if any(k in name for k in ("email", "mail")):
        return "email"
    if any(k in name for k in ("url", "website", "domain", "link", "site")):
        return "link"
    if any(k in name for k in ("date", "time", "created", "updated")):
        return "date"

    sample = [str(v).strip() for v in series.drop_nulls().head(TYPE_SAMPLE_LIMIT).to_list() if str(v).strip()]
    if not sample:
        return "text"

    total = len(sample)
    lower = [s.lower() for s in sample]
    email_count = sum(1 for s in lower if _EMAIL_TYPE_RE.match(s))
    link_count = sum(1 for s in lower if URL_RE.match(s) or DOMAIN_RE.match(s))
    bool_count = sum(1 for s in lower if s in ("true", "false", "yes", "no", "1", "0"))
    date_count = sum(1 for s in sample if looks_like_date(s))
    numeric_count = sum(1 for s in sample if parse_numeric_value(s) is not None or NUMERIC_RE.match(s))

    if email_count / total >= 0.65:
        return "email"
    if link_count / total >= 0.6:
        return "link"
    if date_count / total >= 0.55:
        return "date"
    if bool_count / total >= 0.8:
        return "boolean"
    if numeric_count / total >= 0.65:
        return "number"
    return "text"


HEADER_TOKEN_ALIASES = {
    "e-mail": "email",
    "mail": "email",
    "emails": "email",
    "emailaddress": "email",
    "workemail": "email",
    "website": "website",
    "web": "website",
    "site": "website",
    "url": "website",
    "urls": "website",
    "homepage": "website",
    "domain": "website",
    "domains": "website",
    "websiteurl": "website",
    "webaddress": "website",
    "weburl": "website",
    "organisation": "company",
    "organization": "company",
    "org": "company",
    "account": "company",
    "business": "company",
    "companyname": "company",
    "orgname": "company",
    "firm": "company",
    "linkedinurl": "linkedin",
    "linkedinprofile": "linkedin",
    "linkedinprofileurl": "linkedin",
    "li": "linkedin",
    "telephone": "phone",
    "tel": "phone",
    "mobile": "phone",
    "phonenumber": "phone",
    "mobilenumber": "phone",
    "cell": "phone",
    "fname": "first",
    "given": "first",
    "givenname": "first",
    "firstname": "first",
    "lname": "last",
    "surname": "last",
    "familyname": "last",
    "lastname": "last",
    "jobtitle": "title",
    "roletitle": "title",
    "position": "title",
    "designation": "title",
    "arr": "revenue",
    "funding": "revenue",
    "totalfunding": "revenue",
    "raised": "revenue",
    "headcount": "employees",
    "staff": "employees",
    "teamsize": "employees",
    "employeecount": "employees",
    "numemployees": "employees",
}

GENERIC_HEADER_TOKENS = {"name", "type", "value", "id", "status", "date", "description"}


def _tokenize_header_name(value: str) -> list[str]:
    raw = str(value or "").strip().lower()
    if not raw:
        return []
    collapsed = re.sub(r"[^a-z0-9]+", " ", raw).strip()
    if not collapsed:
        return []
    tokens = []
    for token in collapsed.split():
        mapped = HEADER_TOKEN_ALIASES.get(token, token)
        tokens.append(mapped)
    return tokens


def _normalize_header_name(value: str) -> str:
    return " ".join(_tokenize_header_name(value))


def _header_match_score(
    source_name: str,
    source_type: str,
    canonical_name: str,
    canonical_type: str,
    match_threshold: float = 92.0,
) -> tuple[float, str]:
    source_norm = _normalize_header_name(source_name)
    canonical_norm = _normalize_header_name(canonical_name)
    if not source_norm or not canonical_norm:
        return 0.0, "none"

    if source_norm == canonical_norm:
        return 100.0, "normalized_exact"

    source_tokens = source_norm.split()
    canonical_tokens = canonical_norm.split()
    score = float(fuzz.token_set_ratio(source_norm, canonical_norm))
    strategy = "fuzzy"

    if (
        len(source_tokens) == 1
        and len(canonical_tokens) > 1
        and source_tokens[0] in GENERIC_HEADER_TOKENS
    ):
        score -= 16
    if (
        len(canonical_tokens) == 1
        and len(source_tokens) > 1
        and canonical_tokens[0] in GENERIC_HEADER_TOKENS
    ):
        score -= 16

    if source_type == canonical_type:
        score += 2
    elif source_type != "text" and canonical_type != "text":
        score -= 12

    # Type-affinity boost for borderline matches (85-92 range)
    if 85.0 <= score < match_threshold:
        if source_type == canonical_type and source_type != "text":
            score += 8
            strategy = "type_boosted"
        elif {source_type, canonical_type} <= {"link", "email"}:
            score += 5
            strategy = "type_boosted"

    return max(0.0, min(100.0, score)), strategy


def merge_dataframes_with_schema_mapping(
    datasets: list[dict[str, Any]],
    match_threshold: float = 92.0,
) -> tuple[pl.DataFrame, list[dict]]:
    """
    Merge multiple parsed dataframes by smart column-name mapping.
    Columns with strong semantic/name similarity are merged; otherwise
    they are preserved as unique columns in the consolidated frame.
    """
    if not datasets:
        return pl.DataFrame(), []

    canonical_columns: list[dict[str, str]] = []
    mapped_frames: list[pl.DataFrame] = []
    mapping_report: list[dict] = []

    for item in datasets:
        file_name = str(item.get("fileName") or "dataset.csv")
        df = item.get("df")
        if not isinstance(df, pl.DataFrame):
            continue

        assigned_targets: set[str] = set()
        column_aliases: list[tuple[str, str]] = []
        mapped_columns: list[dict] = []

        for col in df.columns:
            inferred_type = infer_column_type(col, df[col].cast(pl.Utf8, strict=False))
            target = col
            strategy = "new_column"
            score = 100.0

            best_match = None
            best_score = -1.0
            best_strategy = "none"
            for canonical in canonical_columns:
                canonical_name = canonical["name"]
                if canonical_name in assigned_targets:
                    continue
                candidate_score, candidate_strategy = _header_match_score(
                    source_name=col,
                    source_type=inferred_type,
                    canonical_name=canonical_name,
                    canonical_type=canonical["inferredType"],
                    match_threshold=match_threshold,
                )
                if candidate_score > best_score:
                    best_score = candidate_score
                    best_match = canonical_name
                    best_strategy = candidate_strategy

            if best_match and best_score >= match_threshold:
                target = best_match
                strategy = best_strategy
                score = best_score
            else:
                canonical_columns.append({
                    "name": col,
                    "inferredType": inferred_type,
                })

            assigned_targets.add(target)
            column_aliases.append((col, target))
            confidence = "new" if strategy == "new_column" else (
                "exact" if score >= 98.0 else (
                "high" if score >= 92.0 else "moderate"
            ))
            mapped_columns.append({
                "sourceColumn": col,
                "targetColumn": target,
                "matchScore": round(score, 2),
                "strategy": strategy,
                "confidence": confidence,
            })

        mapped_df = df.select([pl.col(source).alias(target) for source, target in column_aliases])
        mapped_frames.append(mapped_df)
        mapping_report.append({
            "fileName": file_name,
            "columns": mapped_columns,
        })

    if not mapped_frames:
        return pl.DataFrame(), mapping_report

    combined = pl.concat(mapped_frames, how="diagonal_relaxed")
    cleaned_names = sanitize_column_names(combined.columns)
    if cleaned_names != combined.columns:
        combined = combined.rename(dict(zip(combined.columns, cleaned_names)))
    return combined, mapping_report


def _resolve_upload_files(
    files: Optional[list[UploadFile]],
    file: Optional[UploadFile],
) -> list[UploadFile]:
    resolved = [f for f in (files or []) if f is not None]
    if file is not None:
        resolved.insert(0, file)
    return resolved


def _summarize_file_names(file_names: list[str], fallback: str) -> str:
    cleaned = [str(name or "").strip() for name in file_names if str(name or "").strip()]
    if not cleaned:
        return fallback
    if len(cleaned) == 1:
        return cleaned[0]
    return f"{cleaned[0]} + {len(cleaned) - 1} more"


async def _parse_upload_datasets(upload_files: list[UploadFile], label: str) -> dict[str, Any]:
    if not upload_files:
        raise HTTPException(status_code=400, detail=f"No {label} files were uploaded.")

    parsed: list[dict[str, Any]] = []
    for idx, upload in enumerate(upload_files, start=1):
        raw = await read_upload_bytes(upload)
        df = read_csv_bytes(raw)
        file_name = str(upload.filename or f"{label}_{idx}.csv")
        parsed.append({
            "fileName": file_name,
            "raw": raw,
            "df": df,
            "rows": df.height,
            "columns": df.width,
        })

    combined_df, mapping = merge_dataframes_with_schema_mapping(parsed)
    return {
        "df": combined_df,
        "mapping": mapping,
        "fileNames": [item["fileName"] for item in parsed],
        "raws": [item["raw"] for item in parsed],
        "inputRows": int(sum(item["rows"] for item in parsed)),
        "inputColumns": int(max((item["columns"] for item in parsed), default=0)),
    }


def build_columns_info(df: pl.DataFrame) -> tuple[list[dict], list[dict]]:
    """Return legacy columns info plus richer column profiles."""
    columns_info = []
    column_profiles = []
    total_rows = max(df.height, 1)
    mv_cols = detect_multivalue_columns(df)

    for col in df.columns:
        utf_series = df[col].cast(pl.Utf8, strict=False)
        non_null = utf_series.drop_nulls()
        unique_count = int(non_null.n_unique()) if non_null.len() else 0
        sample = sample_distinct_values(non_null, limit=VALUE_SAMPLE_LIMIT, scan_limit=VALUE_SAMPLE_SCAN_LIMIT)
        null_count = df.height - non_null.len()

        columns_info.append({
            "name": col,
            "dataType": str(df[col].dtype),
            "uniqueCount": unique_count,
            "sampleValues": sample,
            "totalRows": df.height,
            "isMultiValue": col in mv_cols,
            "separator": mv_cols.get(col),
        })

        column_profiles.append({
            "name": col,
            "dataType": str(df[col].dtype),
            "inferredType": infer_column_type(col, utf_series),
            "nullRate": round(null_count / total_rows, 4),
            "uniqueCount": unique_count,
            "sampleValues": sample[:8],
            "isMultiValue": col in mv_cols,
            "separator": mv_cols.get(col),
        })
    return columns_info, column_profiles


def detect_multivalue_columns(df: pl.DataFrame, threshold: float = 0.30) -> dict[str, str]:
    """Detect columns where values contain multiple items separated by ;, comma, or |."""
    result = {}
    separators = [";", "|"]  # comma excluded by default — too many false positives with names/addresses
    for col in df.columns:
        utf = df[col].cast(pl.Utf8, strict=False).drop_nulls()
        if utf.len() < 5:
            continue
        total = utf.len()
        best_sep = None
        best_rate = 0.0
        for sep in separators:
            count = utf.str.contains(re.escape(sep), literal=True).sum()
            rate = count / total
            if rate > best_rate:
                best_rate = rate
                best_sep = sep
        if best_rate >= threshold and best_sep:
            result[col] = best_sep
    return result


def build_column_anomalies(column_profiles: list[dict], total_rows: int) -> dict:
    """Return lightweight data-shape anomalies for UI confidence panels."""
    empty_heavy = []
    duplicate_heavy = []
    if total_rows <= 0:
        return {"emptyHeavyColumns": empty_heavy, "duplicateHeavyColumns": duplicate_heavy}

    for profile in column_profiles:
        name = profile.get("name", "")
        null_rate = float(profile.get("nullRate", 0))
        unique_count = int(profile.get("uniqueCount", 0))
        distinct_ratio = unique_count / max(total_rows, 1)
        if null_rate >= 0.45:
            empty_heavy.append({"name": name, "nullRate": round(null_rate, 3)})
        if distinct_ratio <= 0.08:
            duplicate_heavy.append({"name": name, "distinctRatio": round(distinct_ratio, 3)})

    return {
        "emptyHeavyColumns": empty_heavy[:6],
        "duplicateHeavyColumns": duplicate_heavy[:6],
    }


def _safe_parse_iso_datetime(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    candidate = raw.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(f"{candidate}T00:00:00")
    except ValueError:
        return None


def _build_view_filter_expr(view_filter: dict, columns: list[str]) -> Optional[pl.Expr]:
    field = str(view_filter.get("field") or "").strip()
    op = str(view_filter.get("op") or "").strip().lower()
    value = str(view_filter.get("value") or "").strip()

    if not field or field not in columns:
        return None

    text_col = pl.col(field).cast(pl.Utf8, strict=False).fill_null("")
    lower_col = text_col.str.to_lowercase()
    lower_value = value.lower()

    if op == "contains":
        if not value:
            return None
        return lower_col.str.contains(lower_value, literal=True)
    if op == "equals":
        return lower_col == lower_value
    if op == "not_equals":
        return lower_col != lower_value
    if op == "is_empty":
        return text_col.str.strip_chars() == ""
    if op == "is_not_empty":
        return text_col.str.strip_chars() != ""
    if op in ("before", "after"):
        target_dt = _safe_parse_iso_datetime(value)
        if target_dt:
            parsed_col = text_col.str.strptime(pl.Datetime, strict=False)
            return parsed_col < target_dt if op == "before" else parsed_col > target_dt
        if not value:
            return None
        return text_col < value if op == "before" else text_col > value
    return None


def _apply_view_filters(df: pl.DataFrame, view_filters_raw: str, allowed_columns: list[str]) -> pl.DataFrame:
    raw = str(view_filters_raw or "").strip() or "[]"
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid viewFilters payload.") from exc

    if not isinstance(parsed, list):
        raise HTTPException(status_code=400, detail="viewFilters payload must be an array.")

    working = df
    for item in parsed:
        if not isinstance(item, dict):
            continue
        expr = _build_view_filter_expr(item, allowed_columns)
        if expr is not None:
            working = working.filter(expr)
    return working


def _session_file_path(session_id: str) -> Path:
    return SESSION_STORE_DIR / f"{session_id}{SESSION_FILE_SUFFIX}"


def _compact_run_snapshot(run: Optional[dict], now: Optional[float] = None) -> Optional[dict]:
    if not isinstance(run, dict):
        return None

    timestamp = now or time.time()
    status = str(run.get("status") or "idle")
    compact = {
        "runId": run.get("runId"),
        "status": status,
        "stage": run.get("stage", "idle"),
        "progress": float(run.get("progress", 0.0)),
        "message": run.get("message", ""),
        "processedRows": int(run.get("processedRows", 0)),
        "totalRows": int(run.get("totalRows", 0)),
        "qualifiedCount": int(run.get("qualifiedCount", 0)),
        "removedCount": int(run.get("removedCount", 0)),
        "removedBreakdown": run.get("removedBreakdown", {
            "removedFilter": 0,
            "removedDomain": 0,
            "removedHubspot": 0,
            "removedIntraDedupe": 0,
        }),
        "startedAt": run.get("startedAt"),
        "finishedAt": run.get("finishedAt"),
        "pausedAt": run.get("pausedAt"),
        "pauseRequested": bool(run.get("pauseRequested")),
        "finishOnPause": bool(run.get("finishOnPause")),
        "error": run.get("error", ""),
    }

    if status in {"running", "pausing"}:
        compact.update({
            "status": "paused",
            "stage": compact.get("stage", "idle"),
            "message": "Qualification was paused when the server restarted. Resume to continue.",
            "pausedAt": timestamp,
            "pauseRequested": False,
        })

    if status in {"running", "pausing", "paused"}:
        for key in (
            "qualifiedIds",
            "removedFilterIds",
            "removedDomainIds",
            "removedHubspotIds",
            "removedIntraDedupeIds",
        ):
            value = run.get(key)
            if isinstance(value, set):
                compact[key] = set(value)
            elif isinstance(value, list):
                compact[key] = set(value)
            else:
                compact[key] = set()
        compact["removedFilterReasonById"] = dict(run.get("removedFilterReasonById") or {})
        compact["removedDomainReasonById"] = dict(run.get("removedDomainReasonById") or {})
        compact["removedHubspotDetailById"] = {
            int(k): v
            for k, v in dict(run.get("removedHubspotDetailById") or {}).items()
            if str(k).isdigit() and isinstance(v, dict)
        }
        compact["removedIntraDedupeReasonById"] = dict(run.get("removedIntraDedupeReasonById") or {})
        if run.get("runConfig"):
            compact["runConfig"] = dict(run["runConfig"])
    else:
        compact["qualifiedIds"] = set()
        compact["removedFilterIds"] = set()
        compact["removedDomainIds"] = set()
        compact["removedHubspotIds"] = set()
        compact["removedIntraDedupeIds"] = set()
        compact["removedFilterReasonById"] = {}
        compact["removedDomainReasonById"] = {}
        compact["removedHubspotDetailById"] = {}
        compact["removedIntraDedupeReasonById"] = {}
    return compact


def _compact_scrape_snapshot(scrape: Optional[dict], now: Optional[float] = None) -> Optional[dict]:
    if not isinstance(scrape, dict):
        return None

    timestamp = now or time.time()
    status = str(scrape.get("status") or "idle")
    compact = {
        "scrapeId": scrape.get("scrapeId"),
        "status": status,
        "stage": scrape.get("stage", "idle"),
        "progress": float(scrape.get("progress", 0.0)),
        "message": scrape.get("message", ""),
        "processed": int(scrape.get("processed", 0)),
        "total": int(scrape.get("total", 0)),
        "ok": int(scrape.get("ok", 0)),
        "fail": int(scrape.get("fail", 0)),
        "ratePerSec": float(scrape.get("ratePerSec", 0.0)),
        "domainField": scrape.get("domainField", ""),
        "outputDir": scrape.get("outputDir", ""),
        "startedAt": scrape.get("startedAt"),
        "finishedAt": scrape.get("finishedAt"),
        "error": scrape.get("error", ""),
        "result": scrape.get("result"),
    }
    if status == "running":
        compact.update({
            "status": "error",
            "stage": "error",
            "progress": 1.0,
            "message": "Scrape job was interrupted when the server restarted.",
            "error": "interrupted_by_restart",
            "finishedAt": timestamp,
        })
    return compact


def _serialize_session_for_disk(session_id: str, session: dict) -> dict:
    source_raws = [raw for raw in (session.get("sourceRaws") or []) if raw]
    csv_raw = session.get("csvRaw")
    if not source_raws and csv_raw:
        source_raws = [csv_raw]

    dedupe_raws = [raw for raw in (session.get("dedupeRaws") or []) if raw]
    dedupe_raw = session.get("dedupeRaw")
    if not dedupe_raws and dedupe_raw:
        dedupe_raws = [dedupe_raw]

    return {
        "sessionId": session_id,
        "csvRaw": csv_raw,
        "dfParquet": session.get("dfParquet"),
        "fileName": session.get("fileName"),
        "sourceFileNames": list(session.get("sourceFileNames") or []),
        "sourceRaws": source_raws,
        "sourceMapping": list(session.get("sourceMapping") or []),
        "sourceRows": int(session.get("sourceRows") or 0),
        "columns": list(session.get("columns") or []),
        "columnProfiles": list(session.get("columnProfiles") or []),
        "previewRows": list(session.get("previewRows") or []),
        "anomalies": dict(session.get("anomalies") or {}),
        "dedupeRaw": dedupe_raw,
        "dedupeName": session.get("dedupeName"),
        "dedupeFileNames": list(session.get("dedupeFileNames") or []),
        "dedupeRaws": dedupe_raws,
        "dedupeMapping": list(session.get("dedupeMapping") or []),
        "dedupeSourceRows": int(session.get("dedupeSourceRows") or 0),
        "workspaceConfig": dict(session.get("workspaceConfig") or {}),
        "activeRun": _compact_run_snapshot(session.get("activeRun")),
        "activeScrape": _compact_scrape_snapshot(session.get("activeScrape")),
        "lastRunStatus": session.get("lastRunStatus"),
        "createdAt": float(session.get("createdAt") or time.time()),
        "updatedAt": float(session.get("updatedAt") or time.time()),
    }


def _persist_session(session_id: str, session: dict) -> None:
    try:
        SESSION_STORE_DIR.mkdir(parents=True, exist_ok=True)
        path = _session_file_path(session_id)
        tmp = path.with_suffix(path.suffix + ".tmp")
        payload = _serialize_session_for_disk(session_id, session)
        with tmp.open("wb") as handle:
            pickle.dump(payload, handle, protocol=pickle.HIGHEST_PROTOCOL)
        tmp.replace(path)
    except Exception:
        traceback.print_exc()


def _delete_persisted_session(session_id: str) -> None:
    path = _session_file_path(session_id)
    if path.exists():
        try:
            path.unlink()
        except Exception:
            traceback.print_exc()


def _load_persisted_sessions() -> None:
    SESSION_STORE.clear()
    SESSION_STORE_DIR.mkdir(parents=True, exist_ok=True)
    now = time.time()

    for path in SESSION_STORE_DIR.glob(f"*{SESSION_FILE_SUFFIX}"):
        try:
            with path.open("rb") as handle:
                payload = pickle.load(handle)
        except Exception:
            try:
                path.unlink()
            except Exception:
                traceback.print_exc()
            continue

        if not isinstance(payload, dict):
            try:
                path.unlink()
            except Exception:
                traceback.print_exc()
            continue

        session_id = str(payload.get("sessionId") or "").strip() or path.name[: -len(SESSION_FILE_SUFFIX)]
        updated_at = float(payload.get("updatedAt") or now)
        if (now - updated_at) > SESSION_TTL_SECONDS:
            try:
                path.unlink()
            except Exception:
                traceback.print_exc()
            continue

        SESSION_STORE[session_id] = {
            "csvRaw": payload.get("csvRaw"),
            "dfParquet": payload.get("dfParquet"),
            "df": None,
            "fileName": payload.get("fileName") or "dataset.csv",
            "sourceFileNames": list(payload.get("sourceFileNames") or []),
            "sourceRaws": list(payload.get("sourceRaws") or []),
            "sourceMapping": list(payload.get("sourceMapping") or []),
            "sourceRows": int(payload.get("sourceRows") or 0),
            "columns": list(payload.get("columns") or []),
            "columnProfiles": list(payload.get("columnProfiles") or []),
            "previewRows": list(payload.get("previewRows") or []),
            "anomalies": dict(payload.get("anomalies") or {}),
            "dedupeRaw": payload.get("dedupeRaw"),
            "dedupeName": payload.get("dedupeName"),
            "dedupeFileNames": list(payload.get("dedupeFileNames") or []),
            "dedupeRaws": list(payload.get("dedupeRaws") or []),
            "dedupeMapping": list(payload.get("dedupeMapping") or []),
            "dedupeSourceRows": int(payload.get("dedupeSourceRows") or 0),
            "dedupeDf": None,
            "workspaceConfig": dict(payload.get("workspaceConfig") or {}),
            "activeRun": _compact_run_snapshot(payload.get("activeRun"), now=now),
            "activeScrape": _compact_scrape_snapshot(payload.get("activeScrape"), now=now),
            "lastRunResult": None,
            "lastRunStatus": payload.get("lastRunStatus"),
            "createdAt": float(payload.get("createdAt") or updated_at),
            "updatedAt": updated_at,
        }


_last_stale_cleanup: float = 0.0
_STALE_CLEANUP_INTERVAL: float = 30.0


def _clean_stale_sessions() -> None:
    global _last_stale_cleanup
    now = time.time()
    if now - _last_stale_cleanup < _STALE_CLEANUP_INTERVAL:
        return
    _last_stale_cleanup = now
    stale = [sid for sid, payload in SESSION_STORE.items() if (now - payload.get("updatedAt", now)) > SESSION_TTL_SECONDS]
    for sid in stale:
        SESSION_STORE.pop(sid, None)
        _delete_persisted_session(sid)


def _put_session(
    raw_csv: bytes,
    file_name: str,
    df: pl.DataFrame,
    columns_info: list[dict],
    column_profiles: list[dict],
    preview_rows: list[dict],
    anomalies: dict,
    dedupe_raw: Optional[bytes] = None,
    dedupe_name: Optional[str] = None,
    dedupe_df: Optional[pl.DataFrame] = None,
    source_raws: Optional[list[bytes]] = None,
    source_file_names: Optional[list[str]] = None,
    source_mapping: Optional[list[dict]] = None,
    source_rows: Optional[int] = None,
    dedupe_raws: Optional[list[bytes]] = None,
    dedupe_file_names: Optional[list[str]] = None,
    dedupe_mapping: Optional[list[dict]] = None,
    dedupe_source_rows: Optional[int] = None,
) -> str:
    _clean_stale_sessions()
    sid = uuid4().hex
    now = time.time()
    SESSION_STORE[sid] = {
        "csvRaw": raw_csv,
        "dfParquet": None,
        "df": df,
        "fileName": file_name,
        "sourceFileNames": list(source_file_names or ([file_name] if file_name else [])),
        "sourceRaws": list(source_raws or ([raw_csv] if raw_csv else [])),
        "sourceMapping": list(source_mapping or []),
        "sourceRows": int(source_rows if source_rows is not None else df.height),
        "columns": columns_info,
        "columnProfiles": column_profiles,
        "previewRows": preview_rows,
        "anomalies": anomalies,
        "dedupeRaw": dedupe_raw,
        "dedupeName": dedupe_name,
        "dedupeFileNames": list(dedupe_file_names or ([dedupe_name] if dedupe_name else [])),
        "dedupeRaws": list(dedupe_raws or ([dedupe_raw] if dedupe_raw else [])),
        "dedupeMapping": list(dedupe_mapping or []),
        "dedupeSourceRows": int(dedupe_source_rows if dedupe_source_rows is not None else (dedupe_df.height if isinstance(dedupe_df, pl.DataFrame) else 0)),
        "dedupeDf": dedupe_df,
        "workspaceConfig": {},
        "activeRun": None,
        "activeScrape": None,
        "lastRunResult": None,
        "lastRunStatus": None,
        "createdAt": now,
        "updatedAt": now,
    }
    _persist_session(sid, SESSION_STORE[sid])
    return sid


def _touch_session(sid: str) -> dict:
    _clean_stale_sessions()
    session = SESSION_STORE.get(sid)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or expired.")
    session["updatedAt"] = time.time()
    return session


def _set_session_dedupe(
    sid: str,
    dedupe_raw: Optional[bytes],
    dedupe_name: Optional[str],
    dedupe_df: Optional[pl.DataFrame] = None,
    dedupe_raws: Optional[list[bytes]] = None,
    dedupe_file_names: Optional[list[str]] = None,
    dedupe_mapping: Optional[list[dict]] = None,
    dedupe_source_rows: Optional[int] = None,
) -> dict:
    session = _touch_session(sid)
    session["dedupeRaw"] = dedupe_raw
    session["dedupeName"] = dedupe_name
    session["dedupeRaws"] = list(dedupe_raws or ([dedupe_raw] if dedupe_raw else []))
    session["dedupeFileNames"] = list(dedupe_file_names or ([dedupe_name] if dedupe_name else []))
    session["dedupeMapping"] = list(dedupe_mapping or [])
    session["dedupeSourceRows"] = int(dedupe_source_rows if dedupe_source_rows is not None else (dedupe_df.height if isinstance(dedupe_df, pl.DataFrame) else 0))
    session["dedupeDf"] = dedupe_df
    session["updatedAt"] = time.time()
    _persist_session(sid, session)
    return session


def _dataframe_to_parquet_bytes(df: pl.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.write_parquet(buf, compression="zstd")
    return buf.getvalue()


def _refresh_session_dataframe_metadata(session: dict, df: pl.DataFrame) -> None:
    columns_info, column_profiles = build_columns_info(df)
    session["columns"] = columns_info
    session["columnProfiles"] = column_profiles
    session["previewRows"] = df.head(PREVIEW_ROW_LIMIT).to_dicts()
    session["anomalies"] = build_column_anomalies(column_profiles, df.height)


def _replace_session_dataframe(session: dict, df: pl.DataFrame) -> None:
    session["df"] = df
    try:
        session["dfParquet"] = _dataframe_to_parquet_bytes(df)
    except Exception:
        session["dfParquet"] = None
        traceback.print_exc()
    _refresh_session_dataframe_metadata(session, df)
    if not session.get("sourceRows"):
        session["sourceRows"] = int(df.height)


def _get_session_df(session: dict) -> pl.DataFrame:
    """Get cached parsed source dataframe, parsing once if needed."""
    if isinstance(session.get("df"), pl.DataFrame):
        return session["df"]
    parquet_raw = session.get("dfParquet")
    if parquet_raw:
        try:
            df = pl.read_parquet(io.BytesIO(parquet_raw))
            session["df"] = df
            if not session.get("sourceRows"):
                session["sourceRows"] = int(df.height)
            if not session.get("columns") or not session.get("columnProfiles"):
                _refresh_session_dataframe_metadata(session, df)
                session["updatedAt"] = time.time()
            return df
        except Exception:
            traceback.print_exc()
    source_raws = [raw for raw in (session.get("sourceRaws") or []) if raw]
    if len(source_raws) > 1:
        parsed = [{
            "fileName": file_name or f"source_{idx + 1}.csv",
            "df": read_csv_bytes(raw),
        } for idx, (file_name, raw) in enumerate(
            zip(session.get("sourceFileNames") or [], source_raws)
        )]
        if len(parsed) < len(source_raws):
            for idx in range(len(parsed), len(source_raws)):
                parsed.append({
                    "fileName": f"source_{idx + 1}.csv",
                    "df": read_csv_bytes(source_raws[idx]),
                })
        df, mapping = merge_dataframes_with_schema_mapping(parsed)
        session["sourceMapping"] = mapping
    else:
        csv_raw = session.get("csvRaw")
        if not csv_raw:
            raise HTTPException(status_code=500, detail="Session source data is unavailable.")
        df = read_csv_bytes(csv_raw)
    session["df"] = df
    if not session.get("sourceRows"):
        session["sourceRows"] = int(df.height)
    if not session.get("columns") or not session.get("columnProfiles"):
        _refresh_session_dataframe_metadata(session, df)
        session["updatedAt"] = time.time()
    return df


def _get_session_dedupe_df(session: dict) -> Optional[pl.DataFrame]:
    """Get cached parsed dedupe dataframe, parsing once if needed."""
    dedupe_df = session.get("dedupeDf")
    if isinstance(dedupe_df, pl.DataFrame):
        return dedupe_df
    dedupe_raws = [raw for raw in (session.get("dedupeRaws") or []) if raw]
    if len(dedupe_raws) > 1:
        parsed = []
        names = session.get("dedupeFileNames") or []
        for idx, raw in enumerate(dedupe_raws, start=1):
            parsed.append({
                "fileName": names[idx - 1] if idx - 1 < len(names) else f"dedupe_{idx}.csv",
                "df": read_csv_bytes(raw),
            })
        combined, mapping = merge_dataframes_with_schema_mapping(parsed)
        session["dedupeDf"] = combined
        session["dedupeMapping"] = mapping
        if not session.get("dedupeSourceRows"):
            session["dedupeSourceRows"] = int(combined.height)
        return combined
    dedupe_raw = session.get("dedupeRaw")
    if not dedupe_raw:
        return None
    parsed = read_csv_bytes(dedupe_raw)
    session["dedupeDf"] = parsed
    if not session.get("dedupeSourceRows"):
        session["dedupeSourceRows"] = int(parsed.height)
    return parsed


def _rebuild_dedupe_from_raws(
    dedupe_raws: list[bytes],
    dedupe_file_names: list[str],
) -> tuple[Optional[pl.DataFrame], list[dict], int]:
    """
    Parse and merge all dedupe raws into one dataframe.
    Returns (merged_df, mapping_report, input_rows_total).
    """
    clean_raws = [raw for raw in (dedupe_raws or []) if raw]
    if not clean_raws:
        return None, [], 0

    parsed: list[dict[str, Any]] = []
    input_rows = 0
    for idx, raw in enumerate(clean_raws, start=1):
        file_name = (
            dedupe_file_names[idx - 1]
            if idx - 1 < len(dedupe_file_names) and str(dedupe_file_names[idx - 1] or "").strip()
            else f"dedupe_{idx}.csv"
        )
        df = read_csv_bytes(raw)
        input_rows += int(df.height)
        parsed.append({
            "fileName": file_name,
            "df": df,
        })

    merged_df, mapping = merge_dataframes_with_schema_mapping(parsed)
    return merged_df, mapping, input_rows


def _build_session_payload(session_id: str, session: dict) -> dict[str, Any]:
    source_df = _get_session_df(session)
    source_file_names = list(session.get("sourceFileNames") or [])
    source_name = session.get("fileName") or _summarize_file_names(source_file_names, "dataset.csv")
    dedupe_df = _get_session_dedupe_df(session)
    dedupe_file_names = list(session.get("dedupeFileNames") or [])
    dedupe_name = session.get("dedupeName") or (_summarize_file_names(dedupe_file_names, "hubspot.csv") if dedupe_file_names else None)

    dedupe_payload: dict[str, Any] = {
        "enabled": bool(dedupe_df is not None and dedupe_df.height > 0),
        "fileName": dedupe_name,
        "fileNames": dedupe_file_names,
        "fileCount": len(dedupe_file_names),
    }
    if dedupe_df is not None:
        # Cache infer_dedupe_matches — deterministic for same column sets
        cache_key = (tuple(source_df.columns), tuple(dedupe_df.columns))
        cached = session.get("_inferredMatchesCache")
        if isinstance(cached, tuple) and len(cached) == 2 and cached[0] == cache_key:
            inferred_matches = cached[1]
        else:
            inferred_matches = infer_dedupe_matches(source_df.columns, dedupe_df.columns)
            session["_inferredMatchesCache"] = (cache_key, inferred_matches)
        primary_match = inferred_matches[0] if inferred_matches else {"sourceColumn": None, "hubspotColumn": None, "keyType": None}
        dedupe_payload.update({
            "columns": dedupe_df.columns,
            "totalRows": dedupe_df.height,
            "sourceRows": int(session.get("dedupeSourceRows") or dedupe_df.height),
            "sourceMappings": list(session.get("dedupeMapping") or []),
            "inferredMatch": {
                "sourceColumn": primary_match.get("sourceColumn"),
                "hubspotColumn": primary_match.get("hubspotColumn"),
                "keyType": primary_match.get("keyType"),
            },
            "inferredMatches": inferred_matches,
        })

    payload = {
        "sessionId": session_id,
        "fileName": source_name,
        "fileNames": source_file_names,
        "fileCount": len(source_file_names),
        "columns": list(session.get("columns") or []),
        "columnProfiles": list(session.get("columnProfiles") or []),
        "previewRows": list(session.get("previewRows") or []),
        "totalRows": source_df.height,
        "sourceRows": int(session.get("sourceRows") or source_df.height),
        "sourceMappings": list(session.get("sourceMapping") or []),
        "anomalies": dict(session.get("anomalies") or {}),
        "workspaceConfig": dict(session.get("workspaceConfig") or {}),
        "dedupe": dedupe_payload,
    }
    active_run = _serialize_run_snapshot(session.get("activeRun"), include_result=False)
    if active_run.get("status") != "idle":
        payload["activeRun"] = active_run
    active_scrape = _serialize_scrape_snapshot(session.get("activeScrape"), include_result=False)
    if active_scrape.get("status") != "idle":
        payload["activeScrape"] = active_scrape
    return payload


def _parse_rules_payload(rules_raw: str) -> list[dict]:
    try:
        parsed = json.loads(str(rules_raw or "[]"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid rules payload.") from exc
    if not isinstance(parsed, list):
        raise HTTPException(status_code=400, detail="Rules payload must be an array.")
    return [item for item in parsed if isinstance(item, dict)]


def _build_status_sets_from_rows(rows: list[dict]) -> dict:
    qualified_ids: set[int] = set()
    removed_filter_ids: set[int] = set()
    removed_domain_ids: set[int] = set()
    removed_hubspot_ids: set[int] = set()
    removed_filter_reasons: dict[int, str] = {}
    removed_domain_reasons: dict[int, str] = {}
    removed_hubspot_detail_by_id: dict[int, dict] = {}

    for row in rows:
        row_id = int(row.get("_rowId", -1))
        if row_id < 0:
            continue
        status = str(row.get("_rowStatus") or "")
        reasons = row.get("_rowReasons") or []
        if status == "qualified":
            qualified_ids.add(row_id)
        elif status == "removed_filter":
            removed_filter_ids.add(row_id)
            if reasons:
                removed_filter_reasons[row_id] = str(reasons[0])
        elif status == "removed_domain":
            removed_domain_ids.add(row_id)
            if reasons:
                removed_domain_reasons[row_id] = str(reasons[0]).replace("domain_", "")
        elif status == "removed_hubspot":
            removed_hubspot_ids.add(row_id)
            detail = row.get("_dedupeMatch")
            if isinstance(detail, dict):
                removed_hubspot_detail_by_id[row_id] = detail

    return {
        "qualifiedIds": qualified_ids,
        "removedFilterIds": removed_filter_ids,
        "removedFilterReasonById": removed_filter_reasons,
        "removedDomainIds": removed_domain_ids,
        "removedHubspotIds": removed_hubspot_ids,
        "removedDomainReasonById": removed_domain_reasons,
        "removedHubspotDetailById": removed_hubspot_detail_by_id,
    }


def _serialize_run_snapshot(run: Optional[dict], include_result: bool = True) -> dict:
    if not isinstance(run, dict):
        return {
            "status": "idle",
            "stage": "idle",
            "progress": 0.0,
            "message": "",
            "processedRows": 0,
            "totalRows": 0,
            "qualifiedCount": 0,
            "removedCount": 0,
            "removedBreakdown": {
                "removedFilter": 0,
                "removedDomain": 0,
                "removedHubspot": 0,
                "removedIntraDedupe": 0,
            },
            "completed": True,
            "pauseRequested": False,
            "finishOnPause": False,
            "pausedAt": None,
            "error": "",
        }

    payload = {
        "runId": run.get("runId"),
        "status": run.get("status", "idle"),
        "stage": run.get("stage", "idle"),
        "progress": float(run.get("progress", 0.0)),
        "message": run.get("message", ""),
        "processedRows": int(run.get("processedRows", 0)),
        "totalRows": int(run.get("totalRows", 0)),
        "qualifiedCount": int(run.get("qualifiedCount", 0)),
        "removedCount": int(run.get("removedCount", 0)),
        "removedBreakdown": run.get("removedBreakdown", {
            "removedFilter": 0,
            "removedDomain": 0,
            "removedHubspot": 0,
            "removedIntraDedupe": 0,
        }),
        "completed": run.get("status") in ("done", "error"),
        "pauseRequested": bool(run.get("pauseRequested")),
        "finishOnPause": bool(run.get("finishOnPause")),
        "pausedAt": run.get("pausedAt"),
        "error": run.get("error", ""),
        "startedAt": run.get("startedAt"),
        "finishedAt": run.get("finishedAt"),
    }
    if include_result and run.get("status") == "done" and isinstance(run.get("result"), dict):
        payload["result"] = run["result"]
    return payload


def _serialize_scrape_snapshot(scrape: Optional[dict], include_result: bool = True) -> dict:
    if not isinstance(scrape, dict):
        return {
            "status": "idle",
            "stage": "idle",
            "progress": 0.0,
            "message": "",
            "processed": 0,
            "total": 0,
            "ok": 0,
            "fail": 0,
            "ratePerSec": 0.0,
            "domainField": "",
            "outputDir": "",
            "completed": True,
            "error": "",
        }

    payload = {
        "scrapeId": scrape.get("scrapeId"),
        "status": scrape.get("status", "idle"),
        "stage": scrape.get("stage", "idle"),
        "progress": float(scrape.get("progress", 0.0)),
        "message": scrape.get("message", ""),
        "processed": int(scrape.get("processed", 0)),
        "total": int(scrape.get("total", 0)),
        "ok": int(scrape.get("ok", 0)),
        "fail": int(scrape.get("fail", 0)),
        "ratePerSec": float(scrape.get("ratePerSec", 0.0)),
        "domainField": scrape.get("domainField", ""),
        "outputDir": scrape.get("outputDir", ""),
        "completed": scrape.get("status") in ("done", "error"),
        "error": scrape.get("error", ""),
        "startedAt": scrape.get("startedAt"),
        "finishedAt": scrape.get("finishedAt"),
    }
    if include_result and scrape.get("status") == "done" and isinstance(scrape.get("result"), dict):
        payload["result"] = scrape["result"]
    return payload


def _resolve_row_annotation(row_id: int, run_state: Optional[dict]) -> tuple[str, list[str], Optional[dict]]:
    if not isinstance(run_state, dict):
        return "qualified", ["preview_only"], None

    removed_filter_ids = run_state.get("removedFilterIds") or set()
    removed_domain_ids = run_state.get("removedDomainIds") or set()
    removed_hubspot_ids = run_state.get("removedHubspotIds") or set()
    removed_intra_dedupe_ids = run_state.get("removedIntraDedupeIds") or set()
    qualified_ids = run_state.get("qualifiedIds") or set()
    removed_filter_reason_by_id = run_state.get("removedFilterReasonById") or {}
    removed_domain_reason_by_id = run_state.get("removedDomainReasonById") or {}
    removed_hubspot_detail_by_id = run_state.get("removedHubspotDetailById") or {}
    run_status = str(run_state.get("status") or "idle")

    if row_id in removed_intra_dedupe_ids:
        return "removed_intra_dedupe", ["intra_dedupe_duplicate"], None
    if row_id in removed_filter_ids:
        return "removed_filter", [str(removed_filter_reason_by_id.get(row_id, "rule_filter_mismatch"))], None
    if row_id in removed_domain_ids:
        detail = str(removed_domain_reason_by_id.get(row_id, "unreachable"))
        return "removed_domain", [f"domain_{detail}".replace("/", "_").replace(" ", "_").replace(":", "_")], None
    if row_id in removed_hubspot_ids:
        detail = removed_hubspot_detail_by_id.get(row_id)
        return "removed_hubspot", ["hubspot_duplicate_match"], detail if isinstance(detail, dict) else None
    if row_id in qualified_ids:
        return "qualified", ["qualified_passed_all_checks"], None

    if run_status in {"running", "pausing"}:
        return "processing", ["qualification_in_progress"], None
    if run_status == "paused":
        return "processing", ["qualification_paused_pending"], None
    if run_status == "done":
        return "removed_filter", ["rule_filter_mismatch"], None
    return "qualified", ["preview_only"], None


def _domain_result_allows_row(result: Optional[dict]) -> bool:
    """
    Domain validation should remove rows for definitive DNS/Geo failures.
    CDN/cloud and unknown geo states can pass as inconclusive.
    """
    if not isinstance(result, dict):
        return True

    if "is_eligible" in result:
        return bool(result.get("is_eligible"))

    status = str(result.get("status") or "").strip().lower()
    if status.startswith("cdn_inconclusive") or status.startswith("geo_inconclusive"):
        return True

    definitive_dead = {
        "invalid",
        "nxdomain",
        "dns_timeout",
        "dns_unresolved",
        "no_a_record",
        "dns_error",
        "no_domain",
        "no domain",
    }
    if status.startswith("non_us_country"):
        return False
    if status in definitive_dead:
        return False
    return bool(result.get("is_alive"))


def _domain_result_resolved_ips_csv(result: Optional[dict]) -> str:
    if not isinstance(result, dict):
        return ""
    csv_value = str(result.get("resolved_ips_csv") or "").strip()
    if csv_value:
        return csv_value
    ips = result.get("resolved_ips")
    if isinstance(ips, list):
        return ",".join([str(ip).strip() for ip in ips if str(ip).strip()])
    return ""


def _normalize_domain_col(df: pl.DataFrame, domain_field: str) -> pl.DataFrame:
    """Add a __domain_key column via a single map_elements call, only if not already present."""
    if "__domain_key" in df.columns:
        return df
    return df.with_columns(
        pl.col(domain_field)
        .cast(pl.Utf8)
        .map_elements(lambda d: normalize_domain_key(str(d or "")), return_dtype=pl.Utf8)
        .alias("__domain_key")
    )


def _build_resolved_ips_columns(df: pl.DataFrame, domain_field: str, domain_results: dict) -> pl.DataFrame:
    """Add resolved_ips column via vectorized join instead of per-row map_elements."""
    df = _normalize_domain_col(df, domain_field)
    lookup_rows = []
    for domain_key, result in domain_results.items():
        lookup_rows.append({"__domain_key": domain_key, RESOLVED_IPS_COLUMN: _domain_result_resolved_ips_csv(result)})
    if not lookup_rows:
        return df.with_columns(pl.lit("").alias(RESOLVED_IPS_COLUMN))
    lookup_df = pl.DataFrame(lookup_rows)
    joined = df.join(lookup_df, on="__domain_key", how="left")
    return joined.with_columns(pl.col(RESOLVED_IPS_COLUMN).fill_null(""))


def _build_homepage_signal_columns(df: pl.DataFrame, domain_field: str, homepage_results: dict) -> pl.DataFrame:
    """Add all homepage signal columns via a single vectorized join."""
    df = _normalize_domain_col(df, domain_field)
    signal_keys = [
        ("html_lang", HTML_LANG_COLUMN, ""),
        ("currency_signals", CURRENCY_SIGNALS_COLUMN, "none"),
        ("meta_title", META_TITLE_COLUMN, ""),
        ("meta_description", META_DESCRIPTION_COLUMN, ""),
        ("b2b_score", B2B_SCORE_COLUMN, 0),
        ("us_signals", US_SIGNALS_COLUMN, False),
        ("website_keywords_match", WEBSITE_KEYWORDS_MATCH_COLUMN, False),
        ("homepage_status", HOMEPAGE_STATUS_COLUMN, "inconclusive:missing_homepage_signals"),
    ]
    if not homepage_results:
        for _, col_name, default in signal_keys:
            df = df.with_columns(pl.lit(default).alias(col_name))
        return df
    lookup_rows = []
    for domain_key, result in homepage_results.items():
        row = {"__domain_key": domain_key}
        for src_key, col_name, default in signal_keys:
            val = result.get(src_key)
            row[col_name] = val if val is not None else default
        lookup_rows.append(row)
    lookup_df = pl.DataFrame(lookup_rows)
    # Drop any pre-existing signal columns before joining
    existing_signal_cols = [col for _, col, _ in signal_keys if col in df.columns]
    if existing_signal_cols:
        df = df.drop(existing_signal_cols)
    joined = df.join(lookup_df, on="__domain_key", how="left")
    for _, col_name, default in signal_keys:
        joined = joined.with_columns(pl.col(col_name).fill_null(default))
    return joined


def _build_domain_alive_mask(df: pl.DataFrame, domain_field: str, domain_results: dict) -> pl.Series:
    """Build a boolean mask for domain liveness via vectorized join."""
    df = _normalize_domain_col(df, domain_field)
    alive_map = {
        dk: _domain_result_allows_row(result)
        for dk, result in domain_results.items()
    }
    lookup_rows = [{"__domain_key": dk, "__domain_alive": alive} for dk, alive in alive_map.items()]
    if not lookup_rows:
        return pl.Series("__domain_alive", [False] * df.height)
    lookup_df = pl.DataFrame(lookup_rows)
    joined = df.select("__domain_key").join(lookup_df, on="__domain_key", how="left")
    return joined["__domain_alive"].fill_null(False)


def _build_homepage_alive_mask(df: pl.DataFrame, domain_field: str, homepage_results: dict) -> pl.Series:
    """Build a boolean mask for homepage qualification via vectorized join."""
    df = _normalize_domain_col(df, domain_field)
    alive_map = {
        dk: _homepage_result_allows_row(result)
        for dk, result in homepage_results.items()
    }
    lookup_rows = [{"__domain_key": dk, "__hp_alive": alive} for dk, alive in alive_map.items()]
    if not lookup_rows:
        return pl.Series("__hp_alive", [True] * df.height)
    lookup_df = pl.DataFrame(lookup_rows)
    joined = df.select("__domain_key").join(lookup_df, on="__domain_key", how="left")
    return joined["__hp_alive"].fill_null(True)


def _homepage_result_allows_row(result: Optional[dict]) -> bool:
    if not isinstance(result, dict) or not result:
        return True
    status = str(result.get("homepage_status") or "").lower()
    if status.startswith("inconclusive:"):
        return True
    if status.startswith("disqualified:fetch_failed"):
        return True
    if status in ("", "unknown", "not_checked"):
        return True
    if "homepage_disqualified" in result:
        return not bool(result.get("homepage_disqualified"))
    if status.startswith("disqualified"):
        return False
    return True


def _parse_form_bool(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _parse_blocklist_categories(payload: Optional[str]) -> dict[str, bool]:
    """Parse JSON dict of category→bool for domain blocklist."""
    if not payload:
        return {cat: True for cat in BLOCKED_DOMAIN_CATEGORIES}
    try:
        parsed = json.loads(str(payload))
        if isinstance(parsed, dict):
            return {str(k): bool(v) for k, v in parsed.items()}
    except (json.JSONDecodeError, TypeError):
        pass
    return {cat: True for cat in BLOCKED_DOMAIN_CATEGORIES}


def _parse_custom_blocked_domains(payload: Optional[str]) -> list[str]:
    """Parse JSON array or comma/newline-separated list of custom blocked domains."""
    if not payload:
        return []
    try:
        parsed = json.loads(str(payload))
        if isinstance(parsed, list):
            return [str(d).strip().lower() for d in parsed if str(d).strip()]
    except (json.JSONDecodeError, TypeError):
        pass
    parts = re.split(r"[,\n]+", str(payload))
    return [p.strip().lower() for p in parts if p.strip()]


def _normalize_tld_token(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    raw = raw.replace("*.", "")
    raw = raw.strip(".")
    raw = re.sub(r"[^a-z0-9.-]", "", raw)
    if not raw:
        return ""
    return f".{raw}"


def _parse_tld_list_payload(payload: Optional[str]) -> set[str]:
    """
    Accept JSON arrays or comma/newline-separated strings of TLD suffixes.
    Returns canonical tokens like ".co.uk" or ".com".
    """
    raw = str(payload or "").strip()
    if not raw:
        return set()

    items: list[str]
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            items = [str(v) for v in parsed]
        elif isinstance(parsed, str):
            items = re.split(r"[\s,]+", parsed)
        else:
            items = [raw]
    except Exception:
        items = re.split(r"[\s,]+", raw)

    out = set()
    for item in items:
        token = _normalize_tld_token(item)
        if token:
            out.add(token)
    return out


def _parse_website_keywords_payload(payload: Optional[str]) -> list[str]:
    raw = str(payload or "").strip()
    if not raw:
        return []

    values: list[str]
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            values = [str(v) for v in parsed]
        elif isinstance(parsed, str):
            values = re.split(r"[\n,]+", parsed)
        else:
            values = [raw]
    except Exception:
        values = re.split(r"[\n,]+", raw)

    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = str(value or "").strip().lower()
        if not token or token in seen:
            continue
        seen.add(token)
        deduped.append(token)
    return deduped


def _parse_export_columns_payload(payload: Optional[str]) -> list[str]:
    raw = str(payload or "").strip()
    if not raw:
        return []

    values: list[str]
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            values = [str(v) for v in parsed]
        elif isinstance(parsed, str):
            values = re.split(r"[\n,]+", parsed)
        else:
            values = [raw]
    except Exception:
        values = re.split(r"[\n,]+", raw)

    selected: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = str(value or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        selected.append(token)
    return selected


def _apply_export_column_selection(df: pl.DataFrame, selected_columns: list[str]) -> pl.DataFrame:
    if not isinstance(df, pl.DataFrame):
        return df
    if not selected_columns:
        return df

    keep = [col for col in selected_columns if col in df.columns]
    if not keep:
        return df.select([])
    return df.select(keep)


def normalize_domain(d: str) -> str:
    """Strip protocol, www., trailing slash from a domain string."""
    if not d or not isinstance(d, str):
        return ""
    d = d.strip().lower()
    for prefix in ("https://", "http://", "www."):
        if d.startswith(prefix):
            d = d[len(prefix):]
    return d.rstrip("/").strip()


def _extract_domain_host(value: Optional[str]) -> str:
    clean = normalize_domain(str(value or ""))
    if not clean:
        return ""
    host = clean.split("/", 1)[0].split(":", 1)[0].strip().lower().strip(".")
    return host


def _host_matches_tld(host: str, tld: str) -> bool:
    suffix = str(tld or "").strip().lower().lstrip(".")
    if not suffix or not host:
        return False
    return host == suffix or host.endswith(f".{suffix}")


def _match_tld_suffix(host: str, tlds: set[str]) -> Optional[str]:
    for token in sorted(tlds, key=len, reverse=True):
        if _host_matches_tld(host, token):
            return token
    return None


def _is_country_code_root(host: str) -> bool:
    if not host or "." not in host:
        return False
    root = host.split(".")[-1]
    return len(root) == 2 and root.isalpha()


def _evaluate_tld_filter(
    domain_value: Optional[str],
    disallowed_tlds: set[str],
    allowed_tlds: set[str],
    exclude_country_tlds: bool,
) -> tuple[bool, Optional[str], Optional[str]]:
    host = _extract_domain_host(domain_value)
    if not host:
        return True, None, None

    matched_allow = _match_tld_suffix(host, allowed_tlds)
    if matched_allow:
        return True, None, None

    matched_disallow = _match_tld_suffix(host, disallowed_tlds)
    if matched_disallow:
        code = f"disallowed_tld_{matched_disallow.lstrip('.').replace('.', '_')}"
        status = f"disallowed tld ({matched_disallow})"
        return False, code, status

    if exclude_country_tlds and _is_country_code_root(host):
        root_tld = f".{host.split('.')[-1]}"
        code = f"disallowed_tld_{root_tld.lstrip('.')}"
        status = f"disallowed tld ({root_tld})"
        return False, code, status

    return True, None, None


def _apply_domain_tld_filter(
    df: pl.DataFrame,
    domain_field: str,
    disallowed_tlds: set[str],
    allowed_tlds: set[str],
    exclude_country_tlds: bool,
) -> tuple[pl.DataFrame, int, int, list[dict], dict[int, str]]:
    """
    Apply TLD-based filtering and return:
    (filtered_df, removed_count, checked_domains_count, dead_domains, reasons_by_row_id)
    """
    if df.height == 0 or domain_field not in df.columns:
        return df, 0, 0, [], {}

    domain_values = df[domain_field].cast(pl.Utf8).to_list()
    unique_domains = {
        d for d in domain_values
        if d and str(d).strip().lower() not in ("unknown", "n/a", "")
    }

    decision_by_domain: dict[str, tuple[bool, Optional[str], Optional[str]]] = {}
    for domain in unique_domains:
        decision_by_domain[domain] = _evaluate_tld_filter(
            domain_value=domain,
            disallowed_tlds=disallowed_tlds,
            allowed_tlds=allowed_tlds,
            exclude_country_tlds=exclude_country_tlds,
        )

    disallowed_domain_values = [d for d, (allowed, _, _) in decision_by_domain.items() if not allowed]
    col = df[domain_field].cast(pl.Utf8)
    is_blank = col.is_null() | col.str.strip_chars().str.to_lowercase().is_in(["", "unknown", "n/a"])
    keep_mask = is_blank | ~col.is_in(disallowed_domain_values)

    filtered = df.filter(keep_mask)
    removed_count = df.height - filtered.height
    if removed_count <= 0:
        return filtered, 0, len(unique_domains), [], {}

    dead_domains = []
    for domain, (allowed, _, status) in decision_by_domain.items():
        if not allowed:
            dead_domains.append({"domain": domain, "status": status or "disallowed tld"})

    reasons_by_row_id: dict[int, str] = {}
    removed_rows = df.filter(~keep_mask).select(["__row_id", domain_field]).to_dicts()
    for item in removed_rows:
        domain_value = item.get(domain_field)
        _, code, _ = decision_by_domain.get(domain_value, (False, "disallowed_tld", None))
        reasons_by_row_id[int(item["__row_id"])] = str(code or "disallowed_tld")

    return filtered, removed_count, len(unique_domains), dead_domains, reasons_by_row_id


def normalize_company_text(value: str) -> str:
    """Normalize company-like text for duplicate detection."""
    if not value:
        return ""
    cleaned = re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()
    return re.sub(r"\s+", " ", cleaned)


def normalize_link(value: str) -> str:
    """Legacy link normalization kept for compatibility."""
    if not value:
        return ""
    raw = str(value).strip().lower()
    raw = raw.replace("https://", "").replace("http://", "").replace("www.", "")
    return raw.rstrip("/").strip()


_COMMON_SUBDOMAINS = ("www.", "app.", "mail.", "blog.", "m.", "ww1.", "ww2.", "www2.", "web.", "portal.")


def normalize_domain_key(value: str) -> str:
    """Normalize domain/website-like values into canonical host."""
    if not value:
        return ""
    raw = str(value).strip().lower()
    if not raw or " " in raw:
        return ""

    # If protocol is missing, urlsplit treats host as path.
    candidate = raw if re.match(r"^[a-z][a-z0-9+.-]*://", raw) else f"http://{raw}"
    try:
        parsed = urlsplit(candidate)
    except Exception:
        cleaned = raw.replace("https://", "").replace("http://", "").replace("www.", "")
        cleaned = cleaned.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0].split(":", 1)[0].strip(".")
        return cleaned
    host = (parsed.netloc or parsed.path or "").strip().lower()
    if not host:
        return ""
    host = host.split("/", 1)[0].split("@")[-1].split(":", 1)[0].strip(".")
    for prefix in _COMMON_SUBDOMAINS:
        if host.startswith(prefix):
            host = host[len(prefix):]
            break
    return host


def is_blocked_domain(host: str, blocked_suffixes: list[str]) -> Optional[str]:
    """Check if a normalized domain matches any blocked domain suffix.
    Returns the matching blocked suffix or None."""
    if not host or not blocked_suffixes:
        return None
    for suffix in blocked_suffixes:
        if host == suffix or host.endswith(f".{suffix}"):
            return suffix
    return None


def build_blocked_suffixes(categories: dict[str, bool], custom_domains: list[str]) -> list[str]:
    """Build flat list of blocked domain suffixes from enabled categories + custom list."""
    suffixes = []
    for cat_name, enabled in categories.items():
        if enabled and cat_name in BLOCKED_DOMAIN_CATEGORIES:
            suffixes.extend(BLOCKED_DOMAIN_CATEGORIES[cat_name])
    for domain in custom_domains:
        cleaned = str(domain).strip().lower()
        if cleaned and cleaned not in suffixes:
            suffixes.append(cleaned)
    return suffixes


def normalize_linkedin_key(value: str) -> str:
    """Normalize linkedin URLs/handles while preserving profile path."""
    if not value:
        return ""
    raw = str(value).strip().lower()
    if not raw:
        return ""

    # Handles like @companyname
    if raw.startswith("@"):
        return raw.lstrip("@").strip()

    if " " in raw and "linkedin" not in raw:
        return ""
    if "linkedin" not in raw and "/" not in raw and "." not in raw:
        # likely plain linkedin handle (without @)
        return raw

    candidate = raw if re.match(r"^[a-z][a-z0-9+.-]*://", raw) else f"https://{raw}"
    try:
        parsed = urlsplit(candidate)
    except Exception:
        cleaned = raw.replace("https://", "").replace("http://", "").replace("www.", "")
        cleaned = cleaned.split("?", 1)[0].split("#", 1)[0].rstrip("/")
        return cleaned if "linkedin" in cleaned else ""
    host = (parsed.netloc or "").strip().lower()
    path = (parsed.path or "").strip().lower()

    if host.startswith("www."):
        host = host[4:]

    if not host:
        # fallback for non-URL values
        cleaned = raw.replace("https://", "").replace("http://", "").replace("www.", "")
        return cleaned.rstrip("/").strip()

    if host.endswith("linkedin.com"):
        path = path.rstrip("/")
        return f"{host}{path}" if path else host
    return f"{host}{path}".rstrip("/")


EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def normalize_email_key(value: str) -> str:
    """Normalize email addresses for deduplication comparison."""
    if not value:
        return ""
    raw = str(value).strip().lower()
    if not raw or not EMAIL_RE.match(raw):
        return ""
    return raw


def _split_multivalue_tokens(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    parts = [part.strip() for part in re.split(r"[,\n;|]+", raw) if part and str(part).strip()]
    return parts if len(parts) > 1 else [raw]


def _extract_normalized_keys(value: str, key_class: str) -> list[str]:
    tokens = _split_multivalue_tokens(value) if key_class in ("domain", "linkedin", "email") else [str(value or "")]
    out: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if key_class == "domain":
            key = normalize_domain_key(token)
        elif key_class == "linkedin":
            key = normalize_linkedin_key(token)
        elif key_class == "email":
            key = normalize_email_key(token)
        else:
            key = normalize_company_text(token)
        if not key or key in seen or key in {"unknown", "n/a", "none", "null"}:
            continue
        seen.add(key)
        out.append(key)
    return out


def _collect_unique_normalized_domains(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        raw = str(value or "").strip()
        if not raw or raw.lower() in {"unknown", "n/a", "none", "null"}:
            continue
        key = normalize_domain_key(raw)
        if not key or key in {"unknown", "n/a", "none", "null"} or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def guess_key_column(columns: list[str], preferred_class: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
    """
    Guess best key column and key class.
    Key classes: domain, linkedin, email, company.
    """
    class_hints = {
        "domain": ["domain", "website", "url", "site", "homepage", "web"],
        "linkedin": ["linkedin", "li url", "li", "linkedin url"],
        "email": ["email", "e-mail", "mail"],
        "company": ["company", "account", "organization", "org", "name"],
    }

    def find_for_class(key_class: str) -> Optional[str]:
        hints = class_hints[key_class]
        for hint in hints:
            for col in columns:
                if hint in col.lower():
                    return col
        return None

    if preferred_class:
        preferred = find_for_class(preferred_class)
        if preferred:
            return preferred, preferred_class

    for key_class in ("domain", "linkedin", "email", "company"):
        found = find_for_class(key_class)
        if found:
            return found, key_class
    return None, None


def guess_key_columns(columns: list[str]) -> dict[str, list[str]]:
    """Return candidate key columns by class using lightweight heuristics."""
    class_hints = {
        "domain": ["domain", "website", "url", "homepage"],
        "linkedin": ["linkedin", "li url", "linkedin url"],
        "email": ["email", "e-mail", "mail"],
        "company": ["company", "account", "organization", "org", "name"],
    }
    company_exclusions = (
        " id",
        "ids",
        "owner",
        "associated",
        "parent",
        "child",
        "deal",
        "ticket",
        "contact",
        "project",
        "quote",
        "task",
        "lead",
        "campaign",
        "source",
        "record",
        "date",
        "time",
        "number of",
        "count",
        "industry",
        "keyword",
        "domain",
        "url",
        "facebook",
        "linkedin",
        "twitter",
        "revenue",
        "employee",
        "country",
        "state",
        "city",
        "postal",
        "phone",
        "address",
    )
    domain_exclusions = ("logo", "technolog", "pagerank", "page rank", "tranco", "umbrella")
    email_exclusions = ("email owner", "email status", "email type", "email domain", "email count")
    out: dict[str, list[str]] = {"domain": [], "linkedin": [], "email": [], "company": []}
    for key_class, hints in class_hints.items():
        for col in columns:
            col_lower = col.lower()
            if key_class == "domain" and "linkedin" in col_lower:
                continue
            if key_class == "domain" and any(token in col_lower for token in domain_exclusions):
                continue
            if key_class == "linkedin" and not (
                "linkedin" in col_lower or "li url" in col_lower
            ):
                continue
            if key_class == "email" and any(token in col_lower for token in email_exclusions):
                continue
            if key_class == "company" and any(token in col_lower for token in ("first name", "last name", "fullname", "full name")):
                continue
            if key_class == "company" and any(token in col_lower for token in company_exclusions):
                continue
            if any(hint in col_lower for hint in hints):
                out[key_class].append(col)
    return out


def infer_dedupe_matches(source_columns: list[str], dedupe_columns: list[str]) -> list[dict]:
    """Infer how source keys can be compared against dedupe keys across key types."""
    source_by_class = guess_key_columns(source_columns)
    dedupe_by_class = guess_key_columns(dedupe_columns)
    matches: list[dict] = []
    for key_type in ("domain", "linkedin", "email", "company"):
        source_candidates = source_by_class.get(key_type) or []
        source_col = source_candidates[0] if source_candidates else None
        hubspot_cols = dedupe_by_class.get(key_type, [])
        if source_col and hubspot_cols:
            matches.append({
                "keyType": key_type,
                "sourceColumn": source_col,
                "sourceColumns": source_candidates,
                "hubspotColumn": hubspot_cols[0],
                "hubspotColumns": hubspot_cols,
            })
    return matches


def _fuzzy_matches_any(normalized: str, reference_set: set[str], threshold: float = 90.0) -> bool:
    """Check if a normalized company name fuzzy-matches any reference value."""
    if not normalized or not reference_set:
        return False
    for ref in reference_set:
        if fuzz.ratio(normalized, ref) >= threshold:
            return True
    return False


def _first_fuzzy_match(normalized: str, reference_set: set[str], threshold: float = 90.0) -> str:
    """Return first fuzzy-matching reference value, or empty string."""
    if not normalized or not reference_set:
        return ""
    for ref in reference_set:
        if fuzz.ratio(normalized, ref) >= threshold:
            return ref
    return ""


def build_dedupe_key_set(df: pl.DataFrame, column_name: str, key_class: str) -> set[str]:
    """Build normalized comparison keys from a dataframe column."""
    if not column_name or column_name not in df.columns:
        return set()

    values = df[column_name].cast(pl.Utf8).drop_nulls().to_list()
    out = set()
    for value in values:
        for key in _extract_normalized_keys(value, key_class):
            out.add(key)
    return out


def apply_hubspot_dedupe(
    qualified: pl.DataFrame,
    dedupe_raw: Optional[bytes] = None,
    dedupe_df: Optional[pl.DataFrame] = None,
) -> tuple[pl.DataFrame, dict]:
    """
    Remove rows from `qualified` that already exist in the HubSpot CSV.
    Returns updated df and metadata.
    """
    info = {
        "enabled": False,
        "removedCount": 0,
        "checkedCount": 0,
        "candidateColumn": None,
        "hubspotColumn": None,
        "keyType": None,
        "matches": [],
        "removedDetailsByRowId": {},
        "warnings": [],
    }
    if not dedupe_raw and dedupe_df is None:
        return qualified, info

    info["enabled"] = True
    hubspot_df = dedupe_df if dedupe_df is not None else read_csv_bytes(dedupe_raw or b"")
    if qualified.height == 0 or hubspot_df.height == 0:
        return qualified, info

    inferred_matches = infer_dedupe_matches(qualified.columns, hubspot_df.columns)
    if not inferred_matches:
        info["warnings"].append("Could not infer matching columns for HubSpot dedupe.")
        return qualified, info

    info["checkedCount"] = qualified.height

    strong_key_present_mask: Optional[pl.Series] = None
    strong_hit_mask: Optional[pl.Series] = None
    company_hit_mask: Optional[pl.Series] = None
    reference_keys_by_class: dict[str, set[str]] = {}
    reference_origin_by_class: dict[str, dict[str, dict[str, str]]] = {}
    source_cols_by_class: dict[str, list[str]] = {}
    use_fuzzy_by_class: dict[str, bool] = {}
    active_matches: list[dict] = []
    for match in inferred_matches:
        key_class = str(match.get("keyType") or "")
        source_cols = [str(col) for col in (match.get("sourceColumns") or []) if str(col).strip() and str(col) in qualified.columns]
        if not source_cols:
            source_col = str(match.get("sourceColumn") or "")
            if source_col and source_col in qualified.columns:
                source_cols = [source_col]
        hubspot_cols = [str(col) for col in (match.get("hubspotColumns") or []) if str(col).strip()]
        if not key_class or not source_cols or not hubspot_cols:
            continue

        reference_keys: set[str] = set()
        reference_origin: dict[str, dict[str, str]] = {}
        for hubspot_col in hubspot_cols:
            if hubspot_col not in hubspot_df.columns:
                continue
            for value in hubspot_df[hubspot_col].cast(pl.Utf8).drop_nulls().to_list():
                keys = _extract_normalized_keys(value, key_class)
                if not keys:
                    continue
                for key in keys:
                    if key not in reference_origin:
                        reference_origin[key] = {
                            "hubspotColumn": hubspot_col,
                            "hubspotValue": str(value or "")[:240],
                        }
                    reference_keys.add(key)
        if not reference_keys:
            continue

        use_fuzzy = key_class == "company" and len(reference_keys) <= 50_000
        reference_keys_by_class[key_class] = reference_keys
        reference_origin_by_class[key_class] = reference_origin
        source_cols_by_class[key_class] = source_cols
        use_fuzzy_by_class[key_class] = use_fuzzy
        class_hit_mask: Optional[pl.Series] = None
        class_has_key_mask: Optional[pl.Series] = None
        for source_col in source_cols:
            candidate_series = qualified[source_col].cast(pl.Utf8)
            key_presence_col_mask = candidate_series.map_elements(
                lambda value: bool(_extract_normalized_keys(value, key_class)),
                return_dtype=pl.Boolean,
            )
            if use_fuzzy:
                hit_col_mask = candidate_series.map_elements(
                    lambda value: any(
                        key in reference_keys or _fuzzy_matches_any(key, reference_keys, 90.0)
                        for key in _extract_normalized_keys(value, key_class)
                    ),
                    return_dtype=pl.Boolean,
                )
            else:
                hit_col_mask = candidate_series.map_elements(
                    lambda value: any(key in reference_keys for key in _extract_normalized_keys(value, key_class)),
                    return_dtype=pl.Boolean,
                )
            class_has_key_mask = key_presence_col_mask if class_has_key_mask is None else (class_has_key_mask | key_presence_col_mask)
            class_hit_mask = hit_col_mask if class_hit_mask is None else (class_hit_mask | hit_col_mask)
        if class_hit_mask is None:
            continue

        if key_class in {"domain", "linkedin", "email"}:
            strong_key_present_mask = class_has_key_mask if strong_key_present_mask is None else (strong_key_present_mask | class_has_key_mask)
            strong_hit_mask = class_hit_mask if strong_hit_mask is None else (strong_hit_mask | class_hit_mask)
        elif key_class == "company":
            company_hit_mask = class_hit_mask if company_hit_mask is None else (company_hit_mask | class_hit_mask)

        active_matches.append({
            "keyType": key_class,
            "sourceColumn": source_cols[0],
            "sourceColumns": source_cols,
            "hubspotColumn": hubspot_cols[0],
            "hubspotColumns": hubspot_cols,
            "referenceCount": len(reference_keys),
        })

    if not active_matches:
        info["warnings"].append("HubSpot dedupe files had no usable key values.")
        return qualified, info

    if strong_hit_mask is None:
        strong_hit_mask = pl.Series("__strong_hit", [False] * qualified.height, dtype=pl.Boolean)
    if strong_key_present_mask is None:
        strong_key_present_mask = pl.Series("__strong_key_present", [False] * qualified.height, dtype=pl.Boolean)
    if company_hit_mask is None:
        company_hit_mask = pl.Series("__company_hit", [False] * qualified.height, dtype=pl.Boolean)

    # Prefer high-confidence key classes (domain/linkedin/email).
    # Only use company-name matching as a fallback when no strong key is present on the source row.
    remove_mask = strong_hit_mask | ((~strong_key_present_mask) & company_hit_mask)
    keep_mask = ~remove_mask

    deduped = qualified.filter(keep_mask)
    info["removedCount"] = qualified.height - deduped.height
    info["matches"] = active_matches
    if "__row_id" in qualified.columns and info["removedCount"] > 0:
        df_with_idx = qualified.with_row_count("__dedupe_idx")
        removed_rows = df_with_idx.filter(remove_mask).to_dicts()
        detail_by_row_id: dict[int, dict[str, Any]] = {}

        def _build_row_match_detail(row: dict, key_class: str) -> Optional[dict]:
            refs = reference_keys_by_class.get(key_class) or set()
            if not refs:
                return None
            source_cols = source_cols_by_class.get(key_class) or []
            use_fuzzy = bool(use_fuzzy_by_class.get(key_class))
            origins = reference_origin_by_class.get(key_class) or {}
            for source_col in source_cols:
                raw_value = row.get(source_col, "")
                keys = _extract_normalized_keys(raw_value, key_class)
                if not keys:
                    continue
                for key in keys:
                    matched_ref = key if key in refs else (_first_fuzzy_match(key, refs, 90.0) if use_fuzzy else "")
                    if not matched_ref:
                        continue
                    origin = origins.get(matched_ref, {})
                    return {
                        "keyType": key_class,
                        "sourceColumn": source_col,
                        "sourceValue": str(raw_value or "")[:240],
                        "normalizedKey": key,
                        "matchMode": "exact" if matched_ref == key else "fuzzy",
                        "hubspotColumn": origin.get("hubspotColumn", ""),
                        "hubspotValue": origin.get("hubspotValue", ""),
                    }
            return None

        for row in removed_rows:
            row_id_raw = row.get("__row_id")
            if row_id_raw is None:
                continue
            try:
                row_id = int(row_id_raw)
            except Exception:
                continue
            idx = int(row.get("__dedupe_idx", -1))
            if idx < 0:
                continue

            detail: Optional[dict] = None
            if bool(strong_hit_mask[idx]):
                for key_class in ("domain", "linkedin", "email"):
                    detail = _build_row_match_detail(row, key_class)
                    if detail:
                        break
            else:
                detail = _build_row_match_detail(row, "company")

            if detail:
                detail_by_row_id[row_id] = detail
        info["removedDetailsByRowId"] = detail_by_row_id

    if active_matches:
        first_match = active_matches[0]
        info["candidateColumn"] = first_match.get("sourceColumn")
        info["hubspotColumn"] = first_match.get("hubspotColumn")
        info["keyType"] = first_match.get("keyType")
    return deduped, info


def apply_intra_dedupe(
    df: pl.DataFrame,
    key_columns: Optional[list[str]] = None,
    strategy: str = "first",
) -> tuple[pl.DataFrame, dict]:
    """Remove duplicate rows WITHIN a single dataframe based on normalized keys."""
    info: dict[str, Any] = {
        "enabled": True,
        "removedCount": 0,
        "totalChecked": df.height,
        "keyColumns": [],
        "strategy": strategy,
        "warnings": [],
    }
    if df.height == 0:
        return df, info

    effective_cols = list(key_columns or [])
    if not effective_cols:
        detected = guess_key_columns(df.columns)
        for key_class in ("domain", "linkedin", "email", "company"):
            if detected.get(key_class):
                effective_cols = detected[key_class][:1]
                info["autoDetectedKeyClass"] = key_class
                break

    if not effective_cols:
        info["enabled"] = False
        info["warnings"].append("No key columns detected for intra-dataset deduplication.")
        return df, info

    info["keyColumns"] = effective_cols
    key_col = effective_cols[0]
    if key_col not in df.columns:
        info["enabled"] = False
        info["warnings"].append(f"Key column '{key_col}' not found in dataset.")
        return df, info

    col_lower = key_col.lower()
    if "linkedin" in col_lower:
        key_class_for_col = "linkedin"
    elif any(hint in col_lower for hint in ("email", "e-mail", "mail")):
        key_class_for_col = "email"
    elif any(hint in col_lower for hint in ("company", "org", "account", "name")):
        key_class_for_col = "company"
    else:
        key_class_for_col = "domain"

    df_with_id = df.with_row_index("__intra_row_id")

    normalized = df_with_id[key_col].cast(pl.Utf8, strict=False).fill_null("").map_elements(
        lambda v: "|".join(_extract_normalized_keys(v, key_class_for_col)) if v else "",
        return_dtype=pl.Utf8,
    )
    df_keyed = df_with_id.with_columns(normalized.alias("__dedupe_key"))

    has_key = df_keyed.filter(pl.col("__dedupe_key").str.len_chars() > 0)
    no_key = df_keyed.filter(pl.col("__dedupe_key").str.len_chars() == 0)

    if strategy == "merge":
        mv_cols = detect_multivalue_columns(df)
        agg_exprs = [pl.col("__intra_row_id").first()]
        for c in has_key.columns:
            if c in ("__intra_row_id", "__dedupe_key"):
                continue
            if c in mv_cols:
                sep = mv_cols[c]
                agg_exprs.append(
                    pl.col(c).cast(pl.Utf8, strict=False).fill_null("")
                    .map_elements(lambda v, s=sep: [p.strip() for p in v.split(s) if p.strip()] if v else [], return_dtype=pl.List(pl.Utf8))
                    .explode().drop_nulls().unique().sort()
                    .str.concat(sep).alias(c)
                )
            else:
                agg_exprs.append(pl.col(c).first())
        deduped = has_key.group_by("__dedupe_key").agg(agg_exprs)
        merged_count = has_key.n_unique(subset=["__dedupe_key"])
        info["mergedDomains"] = has_key.height - merged_count if merged_count else 0
    else:
        keep = "first" if strategy == "first" else "last"
        deduped = has_key.unique(subset=["__dedupe_key"], keep=keep)

    result = pl.concat([deduped, no_key]).sort("__intra_row_id")
    result = result.drop(["__intra_row_id", "__dedupe_key"])

    info["removedCount"] = df.height - result.height
    return result, info


def compute_lead_scores(df: pl.DataFrame, config: dict) -> pl.DataFrame:
    """Compute a 0-100 lead quality score for each row.

    Components:
    - Data richness (0-25): percentage of non-null fields
    - Multi-value diversity (0-25): count of distinct values in multi-value columns
    - Recency (0-20): newer dates = higher score (configurable date field)
    - Domain quality (0-15): whether domain resolved and homepage was fetched
    - Custom high-signal (0-15): presence of user-defined high-value signals
    """
    if df.height == 0:
        return df.with_columns([
            pl.lit(0).alias("_lead_score"),
            pl.lit("{}").alias("_score_breakdown"),
        ])

    weights = config.get("scoreWeights") or {}
    w_richness = float(weights.get("richness", 25))
    w_diversity = float(weights.get("diversity", 25))
    w_recency = float(weights.get("recency", 20))
    w_domain = float(weights.get("domain", 15))
    w_signal = float(weights.get("signal", 15))
    total_w = max(w_richness + w_diversity + w_recency + w_domain + w_signal, 1)

    # Data columns (excluding internal columns)
    data_cols = [c for c in df.columns if not c.startswith("_")]
    num_cols = max(len(data_cols), 1)

    # Multi-value column detection
    mv_cols = detect_multivalue_columns(df)

    # Date field for recency
    date_field = str(config.get("scoreDateField") or "").strip()
    has_date_field = date_field and date_field in df.columns

    # High-signal config: { column: string, values: string[] }
    signal_config = config.get("scoreHighSignalConfig") or {}
    signal_col = str(signal_config.get("column") or "").strip()
    signal_values = [str(v).strip().lower() for v in (signal_config.get("values") or []) if str(v).strip()]
    has_signal = signal_col and signal_col in df.columns and signal_values

    scores = []
    breakdowns = []

    for row in df.iter_rows(named=True):
        # 1. Data richness: % of non-null, non-empty fields
        filled = sum(1 for c in data_cols if row.get(c) is not None and str(row.get(c, "")).strip())
        richness_pct = filled / num_cols
        richness_pts = richness_pct * w_richness

        # 2. Multi-value diversity: total distinct values across MV columns
        mv_count = 0
        for mc, sep in mv_cols.items():
            val = row.get(mc)
            if val:
                parts = [p.strip() for p in str(val).split(sep) if p.strip()]
                mv_count += len(parts)
        # Cap at 20 distinct values for max score
        diversity_pct = min(mv_count / 20, 1.0) if mv_cols else 0
        diversity_pts = diversity_pct * w_diversity

        # 3. Recency: newer dates score higher
        recency_pts = 0.0
        if has_date_field:
            date_val = row.get(date_field)
            if date_val:
                parsed = _safe_parse_iso_datetime(str(date_val))
                if parsed:
                    now = datetime.now(timezone.utc)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    days_ago = max((now - parsed).days, 0)
                    # Decay: 0 days = 100%, 730 days (2yr) = 0%
                    recency_pct = max(1.0 - (days_ago / 730), 0)
                    recency_pts = recency_pct * w_recency

        # 4. Domain quality: check if domain-related status fields exist
        domain_pts = 0.0
        domain_status = row.get("_domain_status") or row.get("__domain_live")
        if domain_status:
            domain_pts = w_domain  # Full points if domain verified
        elif not has_date_field:
            # Give partial credit if we have no domain data at all
            domain_pts = w_domain * 0.5

        # 5. High-signal indicators
        signal_pts = 0.0
        if has_signal:
            cell_val = str(row.get(signal_col) or "").lower()
            matched = sum(1 for sv in signal_values if sv in cell_val)
            signal_pct = min(matched / max(len(signal_values), 1), 1.0)
            signal_pts = signal_pct * w_signal

        raw_score = richness_pts + diversity_pts + recency_pts + domain_pts + signal_pts
        normalized = round(min(raw_score / total_w * 100, 100))
        scores.append(normalized)
        breakdowns.append(json.dumps({
            "richness": round(richness_pts / max(w_richness, 0.01) * 100),
            "diversity": round(diversity_pts / max(w_diversity, 0.01) * 100),
            "recency": round(recency_pts / max(w_recency, 0.01) * 100),
            "domain": round(domain_pts / max(w_domain, 0.01) * 100),
            "signal": round(signal_pts / max(w_signal, 0.01) * 100),
        }))

    return df.with_columns([
        pl.Series("_lead_score", scores, dtype=pl.Int32),
        pl.Series("_score_breakdown", breakdowns, dtype=pl.Utf8),
    ])


def _build_row_taxonomy(
    df_with_id: pl.DataFrame,
    qualified_ids: set[int],
    removed_filter_ids: set[int],
    removed_filter_reason_by_id: dict[int, str],
    removed_domain_ids: set[int],
    removed_domain_reason_by_id: dict[int, str],
    removed_hubspot_ids: set[int],
    removed_hubspot_detail_by_id: Optional[dict[int, dict]] = None,
) -> list[dict]:
    """Build row-level status taxonomy using vectorized Polars + batch conversion."""
    _internal_cols = {"__row_id", "__domain_key"}
    row_ids = df_with_id["__row_id"].to_list()

    # Build status and reason arrays in bulk using Python lists (faster than per-row dict ops)
    statuses = []
    reasons = []
    for rid in row_ids:
        rid_int = int(rid)
        if rid_int in qualified_ids:
            statuses.append("qualified")
            reasons.append("qualified_passed_all_checks")
        elif rid_int in removed_filter_ids:
            statuses.append("removed_filter")
            reasons.append(str(removed_filter_reason_by_id.get(rid_int, "rule_filter_mismatch")))
        elif rid_int in removed_domain_ids:
            statuses.append("removed_domain")
            detail = removed_domain_reason_by_id.get(rid_int, "unreachable")
            reasons.append(f"domain_{detail}".replace("/", "_").replace(" ", "_").replace(":", "_"))
        elif rid_int in removed_hubspot_ids:
            statuses.append("removed_hubspot")
            reasons.append("hubspot_duplicate_match")
        else:
            statuses.append("removed_filter")
            reasons.append(str(removed_filter_reason_by_id.get(rid_int, "rule_filter_mismatch")))

    # Add status/reason columns, drop internals, then convert to dicts once
    output_df = df_with_id.with_columns([
        pl.Series("_rowId", row_ids).cast(pl.Int64),
        pl.Series("_rowStatus", statuses),
        pl.Series("_rowReasons", [[r] for r in reasons]),
    ]).drop([c for c in _internal_cols if c in df_with_id.columns])

    rows = output_df.to_dicts()
    detail_map = removed_hubspot_detail_by_id or {}
    if detail_map:
        for row in rows:
            rid = int(row.get("_rowId", -1))
            if rid in detail_map:
                row["_dedupeMatch"] = detail_map[rid]
    return rows


async def run_qualification_pipeline(
    df: pl.DataFrame,
    parsed_rules: list[dict],
    domain_check: bool,
    homepage_check: bool,
    domain_field: str,
    website_keywords: list[str],
    website_exclude_keywords: Optional[list[str]] = None,
    exclude_country_tlds: bool = False,
    disallowed_tlds: Optional[set[str]] = None,
    allowed_tlds: Optional[set[str]] = None,
    dedupe_raw: Optional[bytes] = None,
    dedupe_df: Optional[pl.DataFrame] = None,
    include_rows: bool = True,
    include_leads: bool = True,
    blocked_domain_suffixes: Optional[list[str]] = None,
    include_dataframe: bool = False,
    skip_network_checks: bool = False,
    intra_dedupe_enabled: bool = False,
    intra_dedupe_columns: Optional[list[str]] = None,
    intra_dedupe_strategy: str = "first",
    score_config: Optional[dict] = None,
) -> dict:
    """
    Execute filters, domain verification, and dedupe with status tracking.
    Returns output payload parts used by legacy and session endpoints.
    """
    # Intra-dataset deduplication (before rule filtering)
    intra_dedupe_info: dict[str, Any] = {"enabled": False, "removedCount": 0}
    if intra_dedupe_enabled:
        df, intra_dedupe_info = apply_intra_dedupe(df, intra_dedupe_columns, intra_dedupe_strategy)

    # Domain blocklist pre-filter (zero network cost, before everything else)
    blocklist_removed_count = 0
    blocklist_removed_ids: set[int] = set()
    blocklist_reason_by_id: dict[int, str] = {}
    if blocked_domain_suffixes and domain_field and domain_field in df.columns:
        df_temp = df.with_row_count("__bl_row_id")
        domain_vals = df_temp[domain_field].cast(pl.Utf8).to_list()
        keep_mask = []
        for idx, raw_val in enumerate(domain_vals):
            host = normalize_domain_key(str(raw_val or ""))
            match = is_blocked_domain(host, blocked_domain_suffixes)
            if match:
                blocklist_removed_count += 1
                row_id = int(df_temp["__bl_row_id"][idx])
                blocklist_removed_ids.add(row_id)
                # Find which category the match belongs to
                cat_label = "custom"
                for cat_name, cat_domains in BLOCKED_DOMAIN_CATEGORIES.items():
                    if match in cat_domains:
                        cat_label = cat_name
                        break
                blocklist_reason_by_id[row_id] = f"blocked_domain_{cat_label}"
                keep_mask.append(False)
            else:
                keep_mask.append(True)
        df = df_temp.filter(pl.Series(keep_mask)).drop("__bl_row_id")

    df_with_id = df.with_row_count("__row_id")
    warnings = []

    # Rule filtering stage
    after_filters, removed_filter_reason_by_id = apply_rules_with_trace(df_with_id, parsed_rules)
    removed_filter_count = df_with_id.height - after_filters.height
    removed_filter_ids: set[int] = set()
    if include_rows:
        all_ids = set(df_with_id["__row_id"].to_list())
        after_filter_ids = set(after_filters["__row_id"].to_list())
        removed_filter_ids = all_ids - after_filter_ids

    # Domain + homepage stage
    working = after_filters
    domain_results = {}
    homepage_results = {}
    dead_domains = []
    domain_checked_count = 0
    homepage_checked_count = 0
    removed_domain_count = 0
    removed_domain_ids = set()
    removed_domain_reason_by_id: dict[int, str] = {}
    tld_filter_enabled = bool(exclude_country_tlds or disallowed_tlds)
    should_run_dns = bool(domain_check and not skip_network_checks)
    should_run_homepage = bool(homepage_check and not skip_network_checks)

    if skip_network_checks and (domain_check or homepage_check):
        warnings.append(
            "Domain/homepage network checks skipped for preview speed. Run qualification for exact website validation."
        )

    if (should_run_dns or should_run_homepage or tld_filter_enabled) and domain_field and domain_field in working.columns:
        if tld_filter_enabled:
            pre_tld = working
            (
                working,
                tld_removed_count,
                tld_checked_count,
                tld_dead_domains,
                tld_reason_by_row_id,
            ) = _apply_domain_tld_filter(
                df=pre_tld,
                domain_field=domain_field,
                disallowed_tlds=disallowed_tlds,
                allowed_tlds=allowed_tlds,
                exclude_country_tlds=exclude_country_tlds,
            )
            removed_domain_count += tld_removed_count
            domain_checked_count += tld_checked_count
            dead_domains.extend(tld_dead_domains)
            if include_rows and tld_removed_count > 0:
                removed_domain_ids.update(tld_reason_by_row_id.keys())
                removed_domain_reason_by_id.update(tld_reason_by_row_id)

        if should_run_dns:
            domains = working[domain_field].cast(pl.Utf8).to_list()
            unique_domains = _collect_unique_normalized_domains(domains)
            if tld_filter_enabled:
                domain_checked_count = max(domain_checked_count, len(unique_domains))
            else:
                domain_checked_count += len(unique_domains)
            # Use DNS-based validation instead of HTTP (28-42x faster)
            domain_results = await check_domains_dns_batch(unique_domains, concurrency=800)

            pre_domain = _build_resolved_ips_columns(working, domain_field, domain_results)
            df_with_id = _build_resolved_ips_columns(df_with_id, domain_field, domain_results)

            alive_mask = _build_domain_alive_mask(pre_domain, domain_field, domain_results)
            working = pre_domain.filter(alive_mask)
            removed_domain_count += pre_domain.height - working.height
            if include_rows:
                post_domain_ids = set(working["__row_id"].to_list())
                dns_removed_ids = set(pre_domain["__row_id"].to_list()) - post_domain_ids
                removed_domain_ids.update(dns_removed_ids)

            dead_domains.extend([
                {"domain": d, "status": domain_results.get(d, {}).get("status", "unknown")}
                for d in unique_domains
                if not _domain_result_allows_row(domain_results.get(d, {}))
            ])

            if include_rows:
                # Row-id -> reason detail mapping for inspector UX
                removed_df = _normalize_domain_col(pre_domain.filter(~alive_mask), domain_field)
                for item in removed_df.select(["__row_id", "__domain_key"]).to_dicts():
                    status = domain_results.get(item["__domain_key"] or "", {}).get("status", "unreachable")
                    removed_domain_reason_by_id[int(item["__row_id"])] = status

        if should_run_homepage:
            homepage_domains = _collect_unique_normalized_domains(working[domain_field].cast(pl.Utf8).to_list())
            homepage_checked_count = len(homepage_domains)
            if homepage_domains:
                homepage_results = await collect_homepage_signals_batch(
                    homepage_domains,
                    website_keywords=website_keywords,
                    website_exclude_keywords=website_exclude_keywords or [],
                    concurrency=80,
                )
            pre_homepage = _build_homepage_signal_columns(working, domain_field, homepage_results)
            df_with_id = _build_homepage_signal_columns(df_with_id, domain_field, homepage_results)

            homepage_mask = _build_homepage_alive_mask(pre_homepage, domain_field, homepage_results)
            working = pre_homepage.filter(homepage_mask)
            removed_domain_count += pre_homepage.height - working.height
            if include_rows:
                post_homepage_ids = set(working["__row_id"].to_list())
                homepage_removed_ids = set(pre_homepage["__row_id"].to_list()) - post_homepage_ids
                removed_domain_ids.update(homepage_removed_ids)

                removed_hp_df = _normalize_domain_col(pre_homepage.filter(~homepage_mask), domain_field)
                for item in removed_hp_df.select(["__row_id", "__domain_key"]).to_dicts():
                    lookup_key = item["__domain_key"] or ""
                    status = homepage_results.get(lookup_key, {}).get("homepage_status", "homepage_disqualified")
                    removed_domain_reason_by_id[int(item["__row_id"])] = status

            dead_domains.extend([
                {"domain": d, "status": homepage_results.get(d, {}).get("homepage_status", "homepage_disqualified")}
                for d in homepage_domains
                if not _homepage_result_allows_row(homepage_results.get(d, {}))
            ])
    elif domain_check or homepage_check or tld_filter_enabled:
        warnings.append(
            "Domain verification, homepage checks, or TLD filtering was enabled, but the selected website column was unavailable."
        )

    # HubSpot dedupe stage
    pre_dedupe_count = working.height
    deduped, dedupe_info = apply_hubspot_dedupe(working, dedupe_raw=dedupe_raw, dedupe_df=dedupe_df)
    warnings.extend(dedupe_info.get("warnings", []))
    removed_hubspot_count = pre_dedupe_count - deduped.height
    qualified_ids: set[int] = set()
    removed_hubspot_ids: set[int] = set()
    if include_rows:
        pre_dedupe_ids = set(working["__row_id"].to_list())
        qualified_ids = set(deduped["__row_id"].to_list())
        removed_hubspot_ids = pre_dedupe_ids - qualified_ids

    # Build row-level status taxonomy and reasons
    rows = []
    if include_rows:
        rows = _build_row_taxonomy(
            df_with_id, qualified_ids, removed_filter_ids, removed_filter_reason_by_id,
            removed_domain_ids, removed_domain_reason_by_id, removed_hubspot_ids,
            {int(k): v for k, v in dict(dedupe_info.get("removedDetailsByRowId") or {}).items() if str(k).isdigit()},
        )

    _internal_cols = {"__row_id", "__domain_key"}
    qualified_for_return = deduped.drop([c for c in _internal_cols if c in deduped.columns])

    # Lead scoring (post-qualification)
    if score_config and score_config.get("scoreEnabled"):
        qualified_for_return = compute_lead_scores(qualified_for_return, score_config)

    output_columns = [col for col in df_with_id.columns if col not in _internal_cols]
    removed_breakdown = {
        "removedFilter": removed_filter_count,
        "removedDomain": removed_domain_count,
        "removedHubspot": removed_hubspot_count,
        "removedIntraDedupe": intra_dedupe_info.get("removedCount", 0),
        "removedBlocklist": blocklist_removed_count,
    }

    return {
        "rows": rows,
        "leads": qualified_for_return.to_dicts() if include_leads else [],
        "qualifiedDf": qualified_for_return if include_dataframe else None,
        "columns": output_columns,
        "qualifiedCount": qualified_for_return.height,
        "removedCount": df.height - qualified_for_return.height + intra_dedupe_info.get("removedCount", 0) + blocklist_removed_count,
        "removedBreakdown": removed_breakdown,
        "domainResults": {
            "checked": domain_checked_count,
            "homepageChecked": homepage_checked_count,
            "dead": dead_domains,
            "cdnReference": get_cdn_reference_data(),
        },
        "dedupeInfo": dedupe_info,
        "intraDedupe": intra_dedupe_info,
        "warnings": warnings,
    }


def _as_int_set(value: Any) -> set[int]:
    if isinstance(value, set):
        raw_values = value
    elif isinstance(value, list):
        raw_values = value
    else:
        return set()
    out: set[int] = set()
    for item in raw_values:
        try:
            parsed = int(item)
        except Exception:
            continue
        if parsed >= 0:
            out.add(parsed)
    return out


async def _finalize_paused_run(
    session_id: str,
    session: dict,
    run: dict,
    auto_disqualify_unprocessed: bool = True,
) -> dict:
    """
    Finalize a paused run using currently known status sets.
    If requested, rows not yet classified are auto-disqualified as `paused_unprocessed`.
    """
    start_time = time.perf_counter()
    df = _get_session_df(session)
    dedupe_df = _get_session_dedupe_df(session)
    df_with_id = df.with_row_count("__row_id")
    total_rows = int(df.height)
    all_ids = set(df_with_id["__row_id"].to_list())

    qualified_ids = _as_int_set(run.get("qualifiedIds"))
    removed_filter_ids = _as_int_set(run.get("removedFilterIds"))
    removed_domain_ids = _as_int_set(run.get("removedDomainIds"))
    removed_hubspot_ids = _as_int_set(run.get("removedHubspotIds"))
    removed_intra_dedupe_ids = _as_int_set(run.get("removedIntraDedupeIds"))
    removed_filter_reason_by_id = {
        int(k): str(v)
        for k, v in dict(run.get("removedFilterReasonById") or {}).items()
        if str(k).isdigit()
    }
    removed_domain_reason_by_id = {
        int(k): str(v)
        for k, v in dict(run.get("removedDomainReasonById") or {}).items()
        if str(k).isdigit()
    }
    warnings = list(run.get("warnings") or [])

    if auto_disqualify_unprocessed:
        unresolved_ids = all_ids - qualified_ids - removed_filter_ids - removed_domain_ids - removed_hubspot_ids - removed_intra_dedupe_ids
        for row_id in unresolved_ids:
            removed_filter_ids.add(row_id)
            removed_filter_reason_by_id.setdefault(row_id, "paused_unprocessed")
        warnings.append("Qualification was finished from paused state. Unprocessed rows were auto-disqualified.")
    else:
        unresolved_ids = set()

    if qualified_ids:
        working = df_with_id.filter(pl.col("__row_id").is_in(sorted(qualified_ids)))
    else:
        working = df_with_id.head(0)

    deduped, dedupe_info = apply_hubspot_dedupe(working, dedupe_raw=session.get("dedupeRaw"), dedupe_df=dedupe_df)
    warnings.extend(dedupe_info.get("warnings", []))
    removed_hubspot_detail_by_id = {
        int(k): v
        for k, v in dict(dedupe_info.get("removedDetailsByRowId") or {}).items()
        if str(k).isdigit() and isinstance(v, dict)
    }

    pre_dedupe_ids = set(working["__row_id"].to_list())
    qualified_ids = set(deduped["__row_id"].to_list())
    removed_hubspot_ids.update(pre_dedupe_ids - qualified_ids)

    rows = []
    for row in df_with_id.to_dicts():
        row_id = int(row.pop("__row_id"))
        if row_id in qualified_ids:
            row_status = "qualified"
            row_reasons = ["qualified_passed_all_checks"]
        elif row_id in removed_filter_ids:
            row_status = "removed_filter"
            row_reasons = [str(removed_filter_reason_by_id.get(row_id, "rule_filter_mismatch"))]
        elif row_id in removed_domain_ids:
            row_status = "removed_domain"
            domain_detail = removed_domain_reason_by_id.get(row_id, "unreachable")
            row_reasons = [f"domain_{domain_detail}".replace("/", "_").replace(" ", "_").replace(":", "_")]
        elif row_id in removed_hubspot_ids:
            row_status = "removed_hubspot"
            row_reasons = ["hubspot_duplicate_match"]
        elif row_id in unresolved_ids:
            row_status = "removed_filter"
            row_reasons = ["paused_unprocessed"]
        else:
            row_status = "removed_filter"
            row_reasons = [str(removed_filter_reason_by_id.get(row_id, "rule_filter_mismatch"))]

        row_payload = {
            **row,
            "_rowId": row_id,
            "_rowStatus": row_status,
            "_rowReasons": row_reasons,
        }
        dedupe_match = removed_hubspot_detail_by_id.get(row_id)
        if isinstance(dedupe_match, dict):
            row_payload["_dedupeMatch"] = dedupe_match
        rows.append(row_payload)

    _internal_cols_bg = {"__row_id", "__domain_key"}
    qualified_for_return = deduped.drop([c for c in _internal_cols_bg if c in deduped.columns])
    removed_breakdown = {
        "removedFilter": len(removed_filter_ids),
        "removedDomain": len(removed_domain_ids),
        "removedHubspot": len(removed_hubspot_ids),
        "removedIntraDedupe": len(removed_intra_dedupe_ids),
    }
    output_columns = [col for col in df_with_id.columns if col not in _internal_cols_bg]
    processing_ms = int((time.perf_counter() - start_time) * 1000)

    run_config = dict(run.get("runConfig") or {})
    domain_results = dict(run.get("domainResults") or {})
    domain_results.setdefault("checked", 0)
    domain_results.setdefault("homepageChecked", 0)
    domain_results.setdefault("dead", [])
    domain_results.setdefault("cdnReference", get_cdn_reference_data())

    status_sets = {
        "qualifiedIds": qualified_ids,
        "removedFilterIds": removed_filter_ids,
        "removedFilterReasonById": removed_filter_reason_by_id,
        "removedDomainIds": removed_domain_ids,
        "removedHubspotIds": removed_hubspot_ids,
        "removedDomainReasonById": removed_domain_reason_by_id,
        "removedHubspotDetailById": removed_hubspot_detail_by_id,
        "removedIntraDedupeIds": removed_intra_dedupe_ids,
        "removedIntraDedupeReasonById": {},
    }

    result = {
        "sessionId": session_id,
        "totalRows": total_rows,
        "qualifiedCount": int(qualified_for_return.height),
        "removedCount": int(total_rows - qualified_for_return.height),
        "removedBreakdown": removed_breakdown,
        "rows": rows,
        "leads": qualified_for_return.to_dicts(),
        "columns": output_columns,
        "domainResults": domain_results,
        "meta": {
            "processingMs": processing_ms,
            "domainCheckEnabled": bool(run_config.get("domainCheck")),
            "homepageCheckEnabled": bool(run_config.get("homepageCheck")),
            "websiteKeywords": list(run_config.get("websiteKeywords") or []),
            "websiteExcludeKeywords": list(run_config.get("websiteExcludeKeywords") or []),
            "tldFilter": {
                "excludeCountryTlds": bool(run_config.get("excludeCountryTlds")),
                "disallowList": sorted(set(run_config.get("disallowedTlds") or [])),
                "allowList": sorted(set(run_config.get("allowedTlds") or [])),
            },
            "dedupe": dedupe_info,
            "warnings": warnings,
        },
    }

    run.update({
        "status": "done",
        "stage": "complete",
        "progress": 1.0,
        "message": "Qualification complete.",
        "pauseRequested": False,
        "finishOnPause": False,
        "processedRows": total_rows,
        "totalRows": total_rows,
        "qualifiedCount": result["qualifiedCount"],
        "removedCount": result["removedCount"],
        "removedBreakdown": removed_breakdown,
        "warnings": warnings,
        "domainResults": domain_results,
        "error": "",
        "finishedAt": time.time(),
        "result": result,
        **status_sets,
    })
    session["lastRunResult"] = result
    session["lastRunStatus"] = status_sets
    session["updatedAt"] = time.time()
    _persist_session(session_id, session)
    return result


async def _run_session_qualification_job(
    session_id: str,
    run_id: str,
    parsed_rules: list[dict],
    domain_check: bool,
    homepage_check: bool,
    domain_field: str,
    website_keywords: list[str],
    website_exclude_keywords: Optional[list[str]] = None,
    exclude_country_tlds: bool = False,
    disallowed_tlds: Optional[set[str]] = None,
    allowed_tlds: Optional[set[str]] = None,
    intra_dedupe_enabled: bool = False,
    intra_dedupe_columns: Optional[list[str]] = None,
    intra_dedupe_strategy: str = "first",
    blocked_domain_suffixes: Optional[list[str]] = None,
    score_config: Optional[dict] = None,
) -> None:
    start_time = time.perf_counter()

    def _get_current_run() -> tuple[Optional[dict], Optional[dict]]:
        session = SESSION_STORE.get(session_id)
        if not session:
            return None, None
        run = session.get("activeRun")
        if not isinstance(run, dict) or run.get("runId") != run_id:
            return session, None
        return session, run

    def _update_run(persist: bool = False, **kwargs) -> bool:
        session, run = _get_current_run()
        if not session or not run:
            return False
        run.update(kwargs)
        session["updatedAt"] = time.time()
        if persist:
            _persist_session(session_id, session)
        return True

    def _pause_requested() -> bool:
        _, run = _get_current_run()
        return bool(isinstance(run, dict) and run.get("pauseRequested"))

    # Initialize before _handle_pause closure captures it
    removed_intra_dedupe_count = 0

    async def _handle_pause(
        *,
        total_rows: int,
        current_qualified_ids: set[int],
        removed_filter_count: int,
        removed_filter_ids: set[int],
        removed_filter_reason_by_id: dict[int, str],
        removed_domain_ids: set[int],
        removed_domain_reason_by_id: dict[int, str],
        removed_hubspot_ids: set[int],
        removed_hubspot_detail_by_id: dict[int, dict],
        domain_checked_count: int,
        homepage_checked_count: int,
        dead_domains: list[dict],
        warnings: list[str],
    ) -> bool:
        if not _pause_requested():
            return False

        session, run = _get_current_run()
        if not session or not run:
            return True

        removed_breakdown = {
            "removedFilter": removed_filter_count,
            "removedDomain": len(removed_domain_ids),
            "removedHubspot": len(removed_hubspot_ids),
            "removedIntraDedupe": removed_intra_dedupe_count,
        }
        run.update({
            "status": "paused",
            "stage": "paused",
            "progress": float(run.get("progress", 0.0)),
            "message": "Qualification paused. Resume to continue, or finish to auto-disqualify remaining rows.",
            "pauseRequested": False,
            "pausedAt": time.time(),
            "qualifiedIds": set(current_qualified_ids),
            "removedFilterIds": set(removed_filter_ids),
            "removedFilterReasonById": dict(removed_filter_reason_by_id),
            "removedDomainIds": set(removed_domain_ids),
            "removedHubspotIds": set(removed_hubspot_ids),
            "removedHubspotDetailById": dict(removed_hubspot_detail_by_id),
            "removedDomainReasonById": dict(removed_domain_reason_by_id),
            "qualifiedCount": len(current_qualified_ids),
            "removedCount": removed_filter_count + len(removed_domain_ids) + removed_intra_dedupe_count,
            "removedBreakdown": removed_breakdown,
            "domainResults": {
                "checked": int(domain_checked_count),
                "homepageChecked": int(homepage_checked_count),
                "dead": list(dead_domains),
                "cdnReference": get_cdn_reference_data(),
            },
            "warnings": list(warnings),
        })
        session["updatedAt"] = time.time()
        _persist_session(session_id, session)

        if bool(run.get("finishOnPause")):
            run["finishOnPause"] = False
            await _finalize_paused_run(
                session_id=session_id,
                session=session,
                run=run,
                auto_disqualify_unprocessed=True,
            )
        return True

    try:
        session = _touch_session(session_id)
        df = _get_session_df(session)
        dedupe_df = _get_session_dedupe_df(session)
        total_rows = df.height
        warnings: list[str] = []

        # Intra-dataset deduplication (before rule filtering)
        removed_intra_dedupe_count = 0
        removed_intra_dedupe_ids: set[int] = set()
        removed_intra_dedupe_reason_by_id: dict[int, str] = {}
        if intra_dedupe_enabled:
            _update_run(
                stage="intra_dedupe",
                progress=0.04,
                message="Removing intra-dataset duplicates...",
                processedRows=0,
                totalRows=total_rows,
            )
            df_deduped, intra_info = apply_intra_dedupe(df, intra_dedupe_columns, intra_dedupe_strategy)
            removed_intra_dedupe_count = intra_info.get("removedCount", 0)
            if intra_info.get("warnings"):
                warnings.extend(intra_info["warnings"])
            df = df_deduped
            total_rows = df.height
            _update_run(
                stage="intra_dedupe",
                progress=0.07,
                message=f"Intra-dedupe complete. {removed_intra_dedupe_count} duplicates removed.",
                removedBreakdown={
                    "removedFilter": 0,
                    "removedDomain": 0,
                    "removedHubspot": 0,
                    "removedIntraDedupe": removed_intra_dedupe_count,
                },
                removedCount=removed_intra_dedupe_count,
                totalRows=total_rows,
            )

        # Domain blocklist pre-filter (zero network cost)
        blocklist_removed_count = 0
        if blocked_domain_suffixes and domain_field and domain_field in df.columns:
            _update_run(
                stage="blocklist",
                progress=0.07,
                message="Filtering blocked domains...",
                processedRows=0,
                totalRows=total_rows,
            )
            df_bl = df.with_row_count("__bl_row_id")
            domain_vals = df_bl[domain_field].cast(pl.Utf8).to_list()
            keep_mask = []
            for raw_val in domain_vals:
                host = normalize_domain_key(str(raw_val or ""))
                match = is_blocked_domain(host, blocked_domain_suffixes)
                keep_mask.append(match is None)
            blocklist_removed_count = sum(1 for k in keep_mask if not k)
            if blocklist_removed_count > 0:
                df = df_bl.filter(pl.Series(keep_mask)).drop("__bl_row_id")
                total_rows = df.height
                _update_run(
                    stage="blocklist",
                    progress=0.08,
                    message=f"Blocked {blocklist_removed_count} non-company domains.",
                    removedBreakdown={
                        "removedFilter": 0,
                        "removedDomain": 0,
                        "removedHubspot": 0,
                        "removedIntraDedupe": removed_intra_dedupe_count,
                        "removedBlocklist": blocklist_removed_count,
                    },
                    removedCount=removed_intra_dedupe_count + blocklist_removed_count,
                    totalRows=total_rows,
                )

        _update_run(
            stage="filters",
            progress=0.08,
            message="Applying qualification filters...",
            processedRows=0,
            totalRows=total_rows,
        )

        df_with_id = df.with_row_count("__row_id")
        all_ids = set(df_with_id["__row_id"].to_list())

        after_filters, removed_filter_reason_by_id = apply_rules_with_trace(df_with_id, parsed_rules)
        await asyncio.sleep(0)  # yield to serve progress polling
        after_filter_ids = set(after_filters["__row_id"].to_list())
        removed_filter_ids = all_ids - after_filter_ids
        removed_filter_count = len(removed_filter_ids)

        _update_run(
            stage="filters",
            progress=0.32,
            message="Filters applied.",
            removedFilterIds=removed_filter_ids,
            removedFilterReasonById=removed_filter_reason_by_id,
            qualifiedIds=after_filter_ids,
            removedBreakdown={
                "removedFilter": removed_filter_count,
                "removedDomain": 0,
                "removedHubspot": 0,
                "removedIntraDedupe": removed_intra_dedupe_count,
            },
            qualifiedCount=len(after_filter_ids),
            removedCount=removed_filter_count + removed_intra_dedupe_count,
            processedRows=removed_filter_count,
        )

        working = after_filters
        domain_results = {}
        homepage_results = {}
        dead_domains = []
        domain_checked_count = 0
        homepage_checked_count = 0
        removed_domain_count = 0
        removed_domain_ids: set[int] = set()
        removed_domain_reason_by_id: dict[int, str] = {}
        removed_hubspot_ids: set[int] = set()
        removed_hubspot_detail_by_id: dict[int, dict] = {}
        tld_filter_enabled = bool(exclude_country_tlds or disallowed_tlds)

        if await _handle_pause(
            total_rows=total_rows,
            current_qualified_ids=set(after_filter_ids),
            removed_filter_count=removed_filter_count,
            removed_filter_ids=removed_filter_ids,
            removed_filter_reason_by_id=removed_filter_reason_by_id,
                removed_domain_ids=removed_domain_ids,
                removed_domain_reason_by_id=removed_domain_reason_by_id,
                removed_hubspot_ids=removed_hubspot_ids,
                removed_hubspot_detail_by_id=removed_hubspot_detail_by_id,
                domain_checked_count=domain_checked_count,
            homepage_checked_count=homepage_checked_count,
            dead_domains=dead_domains,
            warnings=warnings,
        ):
            return

        if (domain_check or homepage_check or tld_filter_enabled) and domain_field and domain_field in working.columns:
            if tld_filter_enabled:
                _update_run(
                    stage="domain",
                    progress=0.36,
                    message="Applying TLD filters...",
                )
                pre_tld = working
                (
                    working,
                    tld_removed_count,
                    tld_checked_count,
                    tld_dead_domains,
                    tld_reason_by_row_id,
                ) = _apply_domain_tld_filter(
                    df=pre_tld,
                    domain_field=domain_field,
                    disallowed_tlds=disallowed_tlds,
                    allowed_tlds=allowed_tlds,
                    exclude_country_tlds=exclude_country_tlds,
                )
                await asyncio.sleep(0)  # yield to serve progress polling
                removed_domain_count += tld_removed_count
                domain_checked_count += tld_checked_count
                dead_domains.extend(tld_dead_domains)
                if tld_removed_count > 0:
                    removed_domain_ids.update(tld_reason_by_row_id.keys())
                    removed_domain_reason_by_id.update(tld_reason_by_row_id)
                removed_domain_count = len(removed_domain_ids)
                _update_run(
                    stage="domain",
                    message="TLD filters applied.",
                    qualifiedIds=set(working["__row_id"].to_list()),
                    removedDomainIds=set(removed_domain_ids),
                    removedDomainReasonById=dict(removed_domain_reason_by_id),
                    removedBreakdown={
                        "removedFilter": removed_filter_count,
                        "removedDomain": removed_domain_count,
                        "removedHubspot": 0,
                        "removedIntraDedupe": removed_intra_dedupe_count,
                    },
                    qualifiedCount=working.height,
                    removedCount=removed_filter_count + removed_domain_count + removed_intra_dedupe_count,
                    processedRows=removed_filter_count + removed_domain_count,
                )

                if await _handle_pause(
                    total_rows=total_rows,
                    current_qualified_ids=set(working["__row_id"].to_list()),
                    removed_filter_count=removed_filter_count,
                    removed_filter_ids=removed_filter_ids,
                    removed_filter_reason_by_id=removed_filter_reason_by_id,
                    removed_domain_ids=removed_domain_ids,
                    removed_domain_reason_by_id=removed_domain_reason_by_id,
                    removed_hubspot_ids=removed_hubspot_ids,
                    removed_hubspot_detail_by_id=removed_hubspot_detail_by_id,
                    domain_checked_count=domain_checked_count,
                    homepage_checked_count=homepage_checked_count,
                    dead_domains=dead_domains,
                    warnings=warnings,
                ):
                    return

            if domain_check:
                unique_domains = _collect_unique_normalized_domains(working[domain_field].cast(pl.Utf8).to_list())
                if tld_filter_enabled:
                    domain_checked_count = max(domain_checked_count, len(unique_domains))
                else:
                    domain_checked_count += len(unique_domains)

                _update_run(
                    stage="domain",
                    progress=0.44 if tld_filter_enabled else 0.36,
                    message=f"Validating domains (0/{len(unique_domains)})...",
                )

                domain_rows_by_key: dict[str, set[int]] = {}
                for item in working.select(["__row_id", domain_field]).to_dicts():
                    key = normalize_domain_key(str(item.get(domain_field) or ""))
                    if not key:
                        continue
                    domain_rows_by_key.setdefault(key, set()).add(int(item["__row_id"]))
                await asyncio.sleep(0)  # yield to serve progress polling

                domain_live_removed = set(removed_domain_ids)
                domain_live_reason = dict(removed_domain_reason_by_id)
                domain_candidates = set(working["__row_id"].to_list())
                domain_processed_rows: set[int] = set()
                domain_seen_dead: set[str] = set()
                domain_processed = 0
                domain_emit_state = {"ts": 0.0, "processed": 0}

                def _emit_domain_live(force: bool = False):
                    now = time.time()
                    if (
                        not force
                        and (domain_processed - int(domain_emit_state["processed"]) < 200)
                        and (now - float(domain_emit_state["ts"]) < 0.35)
                    ):
                        return
                    live_qualified = domain_processed_rows - domain_live_removed
                    _update_run(
                        qualifiedIds=set(live_qualified),
                        removedDomainIds=set(domain_live_removed),
                        removedDomainReasonById=dict(domain_live_reason),
                        removedBreakdown={
                            "removedFilter": removed_filter_count,
                            "removedDomain": len(domain_live_removed),
                            "removedHubspot": 0,
                            "removedIntraDedupe": removed_intra_dedupe_count,
                        },
                        qualifiedCount=len(live_qualified),
                        removedCount=removed_filter_count + len(domain_live_removed) + removed_intra_dedupe_count,
                        processedRows=removed_filter_count + len(domain_live_removed),
                    )
                    domain_emit_state["ts"] = now
                    domain_emit_state["processed"] = domain_processed

                def _on_domain_progress(processed: int, total: int) -> None:
                    total_safe = max(total, 1)
                    stage_progress = min(processed / total_safe, 1.0)
                    _update_run(
                        stage="domain",
                        progress=0.44 + (0.28 * stage_progress) if tld_filter_enabled else 0.36 + (0.36 * stage_progress),
                        message=f"Validating domains ({processed}/{total})...",
                    )

                def _on_domain_result(domain: str, result: dict) -> None:
                    nonlocal domain_processed
                    key = normalize_domain_key(str(domain or ""))
                    row_ids = domain_rows_by_key.get(key, set())
                    if row_ids:
                        domain_processed_rows.update({int(v) for v in row_ids})
                    if row_ids and not _domain_result_allows_row(result):
                        status = str(result.get("status") or "unreachable")
                        for row_id in row_ids:
                            domain_live_removed.add(int(row_id))
                            domain_live_reason[int(row_id)] = status
                        if key and key not in domain_seen_dead:
                            domain_seen_dead.add(key)
                            dead_domains.append({"domain": key, "status": status})
                    domain_processed += 1
                    _emit_domain_live(force=False)

                domain_results = await check_domains_dns_batch(
                    unique_domains,
                    concurrency=800,
                    progress_callback=_on_domain_progress,
                    should_stop=_pause_requested,
                    result_callback=_on_domain_result,
                )
                _emit_domain_live(force=True)

                removed_domain_ids = set(domain_live_removed)
                removed_domain_reason_by_id = dict(domain_live_reason)
                removed_domain_count = len(removed_domain_ids)

                if await _handle_pause(
                    total_rows=total_rows,
                    current_qualified_ids=(domain_processed_rows - removed_domain_ids),
                    removed_filter_count=removed_filter_count,
                    removed_filter_ids=removed_filter_ids,
                    removed_filter_reason_by_id=removed_filter_reason_by_id,
                    removed_domain_ids=removed_domain_ids,
                    removed_domain_reason_by_id=removed_domain_reason_by_id,
                    removed_hubspot_ids=removed_hubspot_ids,
                    removed_hubspot_detail_by_id=removed_hubspot_detail_by_id,
                    domain_checked_count=domain_checked_count,
                    homepage_checked_count=homepage_checked_count,
                    dead_domains=dead_domains,
                    warnings=warnings,
                ):
                    return

                pre_domain = _build_resolved_ips_columns(working, domain_field, domain_results)
                df_with_id = _build_resolved_ips_columns(df_with_id, domain_field, domain_results)

                alive_mask = _build_domain_alive_mask(pre_domain, domain_field, domain_results)
                working = pre_domain.filter(alive_mask)
                removed_domain_count = len(removed_domain_ids)

                post_domain_ids = set(working["__row_id"].to_list())
                dns_removed_ids = set(pre_domain["__row_id"].to_list()) - post_domain_ids
                removed_domain_ids.update(dns_removed_ids)
                removed_domain_count = len(removed_domain_ids)

                removed_dns_df = _normalize_domain_col(pre_domain.filter(~alive_mask), domain_field)
                for item in removed_dns_df.select(["__row_id", "__domain_key"]).to_dicts():
                    status = domain_results.get(item["__domain_key"] or "", {}).get("status", "unreachable")
                    removed_domain_reason_by_id[int(item["__row_id"])] = status

            if homepage_check:
                homepage_domains = _collect_unique_normalized_domains(working[domain_field].cast(pl.Utf8).to_list())
                homepage_checked_count = len(homepage_domains)

                _update_run(
                    stage="homepage",
                    progress=0.73,
                    message=f"Scraping homepages (0/{homepage_checked_count})...",
                )

                def _on_homepage_progress(processed: int, total: int) -> None:
                    total_safe = max(total, 1)
                    stage_progress = min(processed / total_safe, 1.0)
                    _update_run(
                        stage="homepage",
                        progress=0.73 + (0.12 * stage_progress),
                        message=f"Scraping homepages ({processed}/{total})...",
                    )

                homepage_rows_by_key: dict[str, set[int]] = {}
                for item in working.select(["__row_id", domain_field]).to_dicts():
                    key = normalize_domain_key(str(item.get(domain_field) or ""))
                    if not key:
                        continue
                    homepage_rows_by_key.setdefault(key, set()).add(int(item["__row_id"]))

                homepage_live_removed = set(removed_domain_ids)
                homepage_live_reason = dict(removed_domain_reason_by_id)
                homepage_candidates = set(working["__row_id"].to_list())
                homepage_processed_rows: set[int] = set()
                homepage_seen_dead: set[str] = set()
                homepage_processed = 0
                homepage_emit_state = {"ts": 0.0, "processed": 0}

                def _emit_homepage_live(force: bool = False):
                    now = time.time()
                    if (
                        not force
                        and (homepage_processed - int(homepage_emit_state["processed"]) < 80)
                        and (now - float(homepage_emit_state["ts"]) < 0.35)
                    ):
                        return
                    live_qualified = homepage_processed_rows - homepage_live_removed
                    _update_run(
                        qualifiedIds=set(live_qualified),
                        removedDomainIds=set(homepage_live_removed),
                        removedDomainReasonById=dict(homepage_live_reason),
                        removedBreakdown={
                            "removedFilter": removed_filter_count,
                            "removedDomain": len(homepage_live_removed),
                            "removedHubspot": 0,
                            "removedIntraDedupe": removed_intra_dedupe_count,
                        },
                        qualifiedCount=len(live_qualified),
                        removedCount=removed_filter_count + len(homepage_live_removed) + removed_intra_dedupe_count,
                        processedRows=removed_filter_count + len(homepage_live_removed),
                    )
                    homepage_emit_state["ts"] = now
                    homepage_emit_state["processed"] = homepage_processed

                def _on_homepage_result(domain: str, result: dict) -> None:
                    nonlocal homepage_processed
                    key = normalize_domain_key(str(domain or ""))
                    row_ids = homepage_rows_by_key.get(key, set())
                    if row_ids:
                        homepage_processed_rows.update({int(v) for v in row_ids})
                    if row_ids and not _homepage_result_allows_row(result):
                        status = str(result.get("homepage_status") or "homepage_disqualified")
                        for row_id in row_ids:
                            homepage_live_removed.add(int(row_id))
                            homepage_live_reason[int(row_id)] = status
                        if key and key not in homepage_seen_dead:
                            homepage_seen_dead.add(key)
                            dead_domains.append({"domain": key, "status": status})
                    homepage_processed += 1
                    _emit_homepage_live(force=False)

                if homepage_domains:
                    homepage_results = await collect_homepage_signals_batch(
                        homepage_domains,
                        website_keywords=website_keywords,
                        website_exclude_keywords=website_exclude_keywords or [],
                        concurrency=80,
                        progress_callback=_on_homepage_progress,
                        should_stop=_pause_requested,
                        result_callback=_on_homepage_result,
                    )
                _emit_homepage_live(force=True)

                removed_domain_ids = set(homepage_live_removed)
                removed_domain_reason_by_id = dict(homepage_live_reason)
                removed_domain_count = len(removed_domain_ids)

                if await _handle_pause(
                    total_rows=total_rows,
                    current_qualified_ids=(homepage_processed_rows - removed_domain_ids),
                    removed_filter_count=removed_filter_count,
                    removed_filter_ids=removed_filter_ids,
                    removed_filter_reason_by_id=removed_filter_reason_by_id,
                    removed_domain_ids=removed_domain_ids,
                    removed_domain_reason_by_id=removed_domain_reason_by_id,
                    removed_hubspot_ids=removed_hubspot_ids,
                    removed_hubspot_detail_by_id=removed_hubspot_detail_by_id,
                    domain_checked_count=domain_checked_count,
                    homepage_checked_count=homepage_checked_count,
                    dead_domains=dead_domains,
                    warnings=warnings,
                ):
                    return

                pre_homepage = _build_homepage_signal_columns(working, domain_field, homepage_results)
                df_with_id = _build_homepage_signal_columns(df_with_id, domain_field, homepage_results)

                homepage_mask = _build_homepage_alive_mask(pre_homepage, domain_field, homepage_results)
                working = pre_homepage.filter(homepage_mask)
                removed_domain_count = len(removed_domain_ids)

                post_homepage_ids = set(working["__row_id"].to_list())
                homepage_removed_ids = set(pre_homepage["__row_id"].to_list()) - post_homepage_ids
                removed_domain_ids.update(homepage_removed_ids)
                removed_domain_count = len(removed_domain_ids)

                removed_hp_df = _normalize_domain_col(pre_homepage.filter(~homepage_mask), domain_field)
                for item in removed_hp_df.select(["__row_id", "__domain_key"]).to_dicts():
                    status = homepage_results.get(item["__domain_key"] or "", {}).get("homepage_status", "homepage_disqualified")
                    removed_domain_reason_by_id[int(item["__row_id"])] = status
        elif domain_check or homepage_check or tld_filter_enabled:
            warnings.append(
                "Domain verification, homepage checks, or TLD filtering was enabled, but the selected website column was unavailable."
            )

        _update_run(
            stage="website_checks",
            progress=0.85 if homepage_check else 0.72,
            message="Domain and homepage checks complete." if homepage_check else "Domain checks complete.",
            qualifiedIds=set(working["__row_id"].to_list()),
            removedDomainIds=removed_domain_ids,
            removedDomainReasonById=removed_domain_reason_by_id,
            removedBreakdown={
                "removedFilter": removed_filter_count,
                "removedDomain": len(removed_domain_ids),
                "removedHubspot": 0,
                "removedIntraDedupe": removed_intra_dedupe_count,
            },
            qualifiedCount=working.height,
            removedCount=removed_filter_count + len(removed_domain_ids) + removed_intra_dedupe_count,
        )

        _update_run(
            stage="dedupe",
            progress=0.89 if homepage_check else 0.78,
            message="Applying duplicate guard...",
        )

        pre_dedupe_count = working.height
        deduped, dedupe_info = apply_hubspot_dedupe(working, dedupe_raw=session.get("dedupeRaw"), dedupe_df=dedupe_df)
        warnings.extend(dedupe_info.get("warnings", []))
        removed_hubspot_count = pre_dedupe_count - deduped.height
        pre_dedupe_ids = set(working["__row_id"].to_list())
        qualified_ids = set(deduped["__row_id"].to_list())
        removed_hubspot_ids = pre_dedupe_ids - qualified_ids
        removed_hubspot_detail_by_id = {
            int(k): v
            for k, v in dict(dedupe_info.get("removedDetailsByRowId") or {}).items()
            if str(k).isdigit() and isinstance(v, dict)
        }

        _update_run(
            stage="finalizing",
            progress=0.94 if homepage_check else 0.9,
            message="Finalizing row statuses...",
            qualifiedIds=qualified_ids,
            removedHubspotIds=removed_hubspot_ids,
            removedHubspotDetailById=removed_hubspot_detail_by_id,
            removedBreakdown={
                "removedFilter": removed_filter_count,
                "removedDomain": len(removed_domain_ids),
                "removedHubspot": removed_hubspot_count,
                "removedIntraDedupe": removed_intra_dedupe_count,
            },
            qualifiedCount=len(qualified_ids),
            removedCount=total_rows - len(qualified_ids) + removed_intra_dedupe_count,
        )

        rows = []
        rows = _build_row_taxonomy(
            df_with_id, qualified_ids, removed_filter_ids, removed_filter_reason_by_id,
            removed_domain_ids, removed_domain_reason_by_id, removed_hubspot_ids,
            {int(k): v for k, v in dict(dedupe_info.get("removedDetailsByRowId") or {}).items() if str(k).isdigit()},
        )

        _internal_cols_job = {"__row_id", "__domain_key"}
        qualified_for_return = deduped.drop([c for c in _internal_cols_job if c in deduped.columns])

        # Lead scoring (post-qualification)
        if score_config and score_config.get("scoreEnabled"):
            qualified_for_return = compute_lead_scores(qualified_for_return, score_config)

        output_columns = [col for col in df_with_id.columns if col not in _internal_cols_job]
        removed_breakdown = {
            "removedFilter": removed_filter_count,
            "removedDomain": len(removed_domain_ids),
            "removedHubspot": removed_hubspot_count,
            "removedIntraDedupe": removed_intra_dedupe_count,
        }
        processing_ms = int((time.perf_counter() - start_time) * 1000)

        result = {
            "sessionId": session_id,
            "totalRows": total_rows,
            "qualifiedCount": qualified_for_return.height,
            "removedCount": total_rows - qualified_for_return.height,
            "removedBreakdown": removed_breakdown,
            "rows": rows,
            "leads": qualified_for_return.to_dicts(),
            "columns": output_columns,
            "domainResults": {
                "checked": domain_checked_count,
                "homepageChecked": homepage_checked_count,
                "dead": dead_domains,
                "cdnReference": get_cdn_reference_data(),
            },
            "meta": {
                "processingMs": processing_ms,
                "domainCheckEnabled": domain_check,
                "homepageCheckEnabled": homepage_check,
                "websiteKeywords": website_keywords,
        "websiteExcludeKeywords": website_exclude_keywords,
                "websiteExcludeKeywords": website_exclude_keywords,
                "tldFilter": {
                    "excludeCountryTlds": exclude_country_tlds,
                    "disallowList": sorted(disallowed_tlds),
                    "allowList": sorted(allowed_tlds),
                },
                "dedupe": dedupe_info,
                "warnings": warnings,
            },
        }

        session, run = _get_current_run()
        if session and run:
            status_sets = {
                "qualifiedIds": qualified_ids,
                "removedFilterIds": removed_filter_ids,
                "removedFilterReasonById": removed_filter_reason_by_id,
                "removedDomainIds": removed_domain_ids,
                "removedHubspotIds": removed_hubspot_ids,
                "removedDomainReasonById": removed_domain_reason_by_id,
                "removedHubspotDetailById": {
                    int(k): v
                    for k, v in dict(dedupe_info.get("removedDetailsByRowId") or {}).items()
                    if str(k).isdigit() and isinstance(v, dict)
                },
                "removedIntraDedupeIds": removed_intra_dedupe_ids,
                "removedIntraDedupeReasonById": removed_intra_dedupe_reason_by_id,
            }
            run.update({
                "status": "done",
                "stage": "complete",
                "progress": 1.0,
                "message": "Qualification complete.",
                "pauseRequested": False,
                "finishOnPause": False,
                "pausedAt": None,
                "processedRows": total_rows,
                "totalRows": total_rows,
                "qualifiedCount": result["qualifiedCount"],
                "removedCount": result["removedCount"],
                "removedBreakdown": removed_breakdown,
                "warnings": warnings,
                "domainResults": result["domainResults"],
                "error": "",
                "finishedAt": time.time(),
                "result": result,
                **status_sets,
            })
            session["lastRunResult"] = result
            session["lastRunStatus"] = status_sets
            session["updatedAt"] = time.time()
            _persist_session(session_id, session)

    except Exception as exc:
        session, run = _get_current_run()
        if session and run:
            run.update({
                "status": "error",
                "stage": "error",
                "progress": 1.0,
                "message": "Qualification failed.",
                "pauseRequested": False,
                "finishOnPause": False,
                "error": f"{type(exc).__name__}: {exc}",
                "finishedAt": time.time(),
            })
            session["updatedAt"] = time.time()
            _persist_session(session_id, session)
        traceback.print_exc()


def _resolve_scrape_domain_field(df: pl.DataFrame, preferred: str = "") -> str:
    candidate = str(preferred or "").strip()
    if candidate and candidate in df.columns:
        return candidate
    guessed, key_class = guess_key_column(df.columns, preferred_class="domain")
    if key_class == "domain" and guessed and guessed in df.columns:
        return guessed
    return ""


async def _run_session_scrape_job(
    session_id: str,
    scrape_id: str,
    domain_field: str,
) -> None:
    def _get_current_scrape() -> tuple[Optional[dict], Optional[dict]]:
        session = SESSION_STORE.get(session_id)
        if not session:
            return None, None
        scrape = session.get("activeScrape")
        if not isinstance(scrape, dict) or scrape.get("scrapeId") != scrape_id:
            return session, None
        return session, scrape

    def _update_scrape(persist: bool = False, **kwargs) -> bool:
        session, scrape = _get_current_scrape()
        if not session or not scrape:
            return False
        scrape.update(kwargs)
        session["updatedAt"] = time.time()
        if persist:
            _persist_session(session_id, session)
        return True

    try:
        session = _touch_session(session_id)
        df = _get_session_df(session)
        resolved_domain_field = _resolve_scrape_domain_field(df, domain_field)
        if not resolved_domain_field:
            raise ValueError("Unable to infer a domain column for scraping. Select a website/domain field first.")

        domain_rows_by_key: dict[str, set[int]] = {}
        domain_values = df[resolved_domain_field].cast(pl.Utf8, strict=False).fill_null("").to_list()
        for row_id, raw_value in enumerate(domain_values):
            key = normalize_domain_key(str(raw_value or ""))
            if not key:
                continue
            domain_rows_by_key.setdefault(key, set()).add(int(row_id))

        all_domain_keys = list(domain_rows_by_key.keys())
        total_targets = len(all_domain_keys)
        if total_targets == 0:
            raise ValueError("No valid domains found in the selected domain field.")

        # Check scrape cache for already-scraped domains
        cached_scrape_results: dict[str, dict] = {}
        try:
            cached_scrape_results = await get_cached_scrapes_batch(all_domain_keys)
        except Exception:
            pass  # Cache miss is fine, we'll scrape everything
        cached_count = len(cached_scrape_results)

        uncached_domains = [d for d in all_domain_keys if d not in cached_scrape_results]
        targets = [
            ScrapeTarget(domain=domain_key, url=f"https://{domain_key}")
            for domain_key in uncached_domains
        ]

        output_dir = SCRAPE_JOB_DIR / session_id / scrape_id
        output_dir.mkdir(parents=True, exist_ok=True)
        paths = build_scrape_paths(output_dir)
        warnings: list[str] = []

        if cached_count > 0:
            warnings.append(f"{cached_count} domains loaded from scrape cache.")

        _update_scrape(
            stage="phase1",
            progress=0.04,
            message=f"Phase 1 async scrape starting (0/{len(targets)}, {cached_count} cached)...",
            processed=0,
            total=len(targets),
            ok=0,
            fail=0,
            domainField=resolved_domain_field,
            outputDir=str(output_dir),
            persist=True,
        )

        enriched_df: Optional[pl.DataFrame] = None
        if targets:
            def _on_phase1_progress(payload: dict) -> None:
                processed = int(payload.get("processed", 0))
                total = int(payload.get("total", len(targets)))
                stage_progress = min(processed / max(total, 1), 1.0)
                _update_scrape(
                    stage="phase1",
                    progress=0.04 + (0.60 * stage_progress),
                    message=f"Phase 1 async scrape ({processed}/{total})...",
                    processed=processed,
                    total=total,
                    ok=int(payload.get("ok", 0)),
                    fail=int(payload.get("fail", 0)),
                    ratePerSec=float(payload.get("ratePerSec", 0.0)),
                )

            await run_scrape_phase1_async(
                targets=targets,
                out_path=paths["phase1"],
                failures_path=paths["phase1_failures"],
                state_path=paths["state"],
                concurrency=SCRAPE_PHASE1_CONCURRENCY,
                timeout_seconds=SCRAPE_PHASE1_TIMEOUT,
                retry_count=SCRAPE_PHASE1_RETRY,
                resume=False,
                progress_callback=_on_phase1_progress,
            )

            _update_scrape(
                stage="phase2",
                progress=0.66,
                message="Phase 2 headless fallback starting...",
                persist=True,
            )

            def _on_phase2_progress(payload: dict) -> None:
                processed = int(payload.get("processed", 0))
                total = int(payload.get("total", 0))
                stage_progress = min(processed / max(total, 1), 1.0) if total > 0 else 1.0
                _update_scrape(
                    stage="phase2",
                    progress=0.66 + (0.22 * stage_progress),
                    message=f"Phase 2 fallback ({processed}/{total})...",
                    processed=processed,
                    total=total,
                    ok=int(payload.get("ok", 0)),
                    fail=int(payload.get("fail", 0)),
                    ratePerSec=float(payload.get("ratePerSec", 0.0)),
                )

            try:
                await run_scrape_phase2_fallback(
                    phase1_path=paths["phase1"],
                    failures_path=paths["phase1_failures"],
                    phase2_path=paths["phase2"],
                    merged_path=paths["merged"],
                    state_path=paths["state"],
                    concurrency=SCRAPE_PHASE2_CONCURRENCY,
                    timeout_seconds=SCRAPE_PHASE2_TIMEOUT,
                    progress_callback=_on_phase2_progress,
                )
            except Exception as exc:
                warnings.append(f"Phase 2 fallback skipped: {type(exc).__name__}: {exc}")
                if paths["phase1"].exists() and not paths["merged"].exists():
                    shutil.copy2(paths["phase1"], paths["merged"])

            merged_source = paths["merged"] if paths["merged"].exists() else paths["phase1"]
            if not merged_source.exists():
                raise RuntimeError("Scraper output not found after phase execution.")

            _update_scrape(
                stage="keywords",
                progress=0.90,
                message="Extracting TF-IDF keywords...",
                persist=True,
            )
            extract_scrape_keywords(
                merged_path=merged_source,
                enriched_path=paths["enriched"],
                top_k=SCRAPE_KEYWORD_TOP_K,
                write_csv=True,
            )

            if not paths["enriched"].exists():
                raise RuntimeError("Enriched scraper output was not created.")
            enriched_df = pl.read_parquet(str(paths["enriched"]))
            if "domain" not in enriched_df.columns:
                raise RuntimeError("Enriched scraper output is missing `domain`.")

            # Store freshly scraped results in cache
            try:
                scrape_source_cols = list(SCRAPE_COLUMN_MAP.keys())
                new_cache_entries: dict[str, dict] = {}
                for row in enriched_df.iter_rows(named=True):
                    domain_key = normalize_domain_key(str(row.get("domain") or ""))
                    if not domain_key:
                        continue
                    new_cache_entries[domain_key] = {col: str(row.get(col) or "") for col in scrape_source_cols if col in enriched_df.columns}
                if new_cache_entries:
                    await set_cached_scrapes_batch(new_cache_entries)
            except Exception:
                pass  # Cache write failure is non-fatal
        else:
            _update_scrape(
                stage="cache",
                progress=0.88,
                message=f"All {cached_count} domains loaded from cache, skipping scrape...",
                persist=True,
            )

        # Build cached DataFrame from cache hits and merge with fresh results
        if cached_scrape_results:
            scrape_source_cols = list(SCRAPE_COLUMN_MAP.keys())
            cached_rows = []
            for domain_key, result in cached_scrape_results.items():
                row_data: dict[str, str] = {"domain": domain_key}
                for col in scrape_source_cols:
                    if col != "domain":
                        row_data[col] = str(result.get(col) or "")
                cached_rows.append(row_data)
            cached_df = pl.DataFrame(cached_rows)
            if enriched_df is not None:
                # Merge: fresh results take precedence over cache
                enriched_df = pl.concat([enriched_df, cached_df], how="diagonal_relaxed").unique(subset=["domain"], keep="first")
            else:
                enriched_df = cached_df

        if enriched_df is None or enriched_df.height == 0:
            raise RuntimeError("No scrape results available (neither fresh nor cached).")
        if "domain" not in enriched_df.columns:
            raise RuntimeError("Enriched scraper output is missing `domain`.")

        _update_scrape(
            stage="merging",
            progress=0.95,
            message="Merging scraped fields into session...",
            persist=True,
        )

        available_source_cols = [col for col in SCRAPE_COLUMN_MAP.keys() if col in enriched_df.columns]
        normalized_scrape = (
            enriched_df
            .with_columns(
                pl.col("domain")
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .map_elements(normalize_domain_key, return_dtype=pl.Utf8)
                .alias("__scrape_key")
            )
            .filter(pl.col("__scrape_key").str.len_chars() > 0)
            .select(["__scrape_key"] + available_source_cols)
            .rename({key: SCRAPE_COLUMN_MAP[key] for key in available_source_cols})
            .unique(subset=["__scrape_key"], keep="first")
        )

        source_with_key = (
            df
            .with_columns(
                pl.col(resolved_domain_field)
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .map_elements(normalize_domain_key, return_dtype=pl.Utf8)
                .alias("__scrape_key")
            )
        )
        existing_scrape_cols = [col for col in SCRAPE_ENRICH_COLUMNS if col in source_with_key.columns]
        if existing_scrape_cols:
            source_with_key = source_with_key.drop(existing_scrape_cols)
        merged_df = source_with_key.join(normalized_scrape, on="__scrape_key", how="left").drop("__scrape_key")

        session, scrape = _get_current_scrape()
        if not session or not scrape:
            return

        _replace_session_dataframe(session, merged_df)

        ok_count = int(
            enriched_df
            .filter(pl.col("status").cast(pl.Utf8, strict=False).fill_null("") == "ok")
            .height
        ) if "status" in enriched_df.columns else 0
        fail_count = int(max(0, enriched_df.height - ok_count))
        matched_rows = int(
            merged_df
            .filter(pl.col("scrape_status").cast(pl.Utf8, strict=False).is_not_null())
            .height
        ) if "scrape_status" in merged_df.columns else 0

        scrape.update({
            "status": "done",
            "stage": "complete",
            "progress": 1.0,
            "message": "Homepage scraping complete.",
            "processed": int(enriched_df.height),
            "total": total_targets,
            "ok": ok_count,
            "fail": fail_count,
            "domainField": resolved_domain_field,
            "outputDir": str(output_dir),
            "error": "",
            "finishedAt": time.time(),
            "result": {
                "targets": total_targets,
                "scrapedDomains": int(enriched_df.height),
                "matchedRows": matched_rows,
                "ok": ok_count,
                "fail": fail_count,
                "outputDir": str(output_dir),
                "columnsAdded": SCRAPE_ENRICH_COLUMNS,
                "warnings": warnings,
            },
        })
        session["updatedAt"] = time.time()
        _persist_session(session_id, session)
    except Exception as exc:
        _update_scrape(
            status="error",
            stage="error",
            progress=1.0,
            message="Homepage scraping failed.",
            error=f"{type(exc).__name__}: {exc}",
            finishedAt=time.time(),
            persist=True,
        )
        traceback.print_exc()


def fuzzy_match(value: str, targets: list[str], threshold: float) -> bool:
    """Return True if `value` fuzzy-matches ANY of `targets` above `threshold`."""
    if not value:
        return False
    v = value.strip().lower()
    for t in targets:
        t_clean = t.strip().lower()
        score = fuzz.token_sort_ratio(v, t_clean)
        if score >= threshold:
            return True
        if fuzz.partial_ratio(v, t_clean) >= threshold:
            return True
    return False


# ---------------------------------------------------------------------------
# Domain Liveness Checker (async, concurrent)
# ---------------------------------------------------------------------------

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
# Domains that are known parking / placeholder services → treat as dead
PARKED_INDICATORS = [
    "godaddy", "namecheap", "parked", "buy this domain",
    "domain for sale", "this domain", "squarespace",
    "coming soon", "under construction", "page not found",
]

async def check_single_domain(session: aiohttp.ClientSession, domain: str, timeout: int = 6) -> dict:
    """
    Check if a single domain is alive. Uses HEAD first (fast), falls back to GET.
    Returns {domain, alive: bool, status: str}.
    """
    if not domain or domain.lower() in ("unknown", "n/a", "none", ""):
        return {"domain": domain, "alive": False, "status": "no domain"}

    clean = normalize_domain(domain)
    if not clean or "." not in clean:
        return {"domain": domain, "alive": False, "status": "invalid"}

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    for proto in ["https", "http"]:
        url = f"{proto}://{clean}"
        # Try HEAD first (fast, low bandwidth)
        try:
            async with session.head(url, timeout=aiohttp.ClientTimeout(total=timeout),
                                     ssl=ssl_ctx, allow_redirects=True) as resp:
                if resp.status < 500:
                    return {"domain": domain, "alive": True, "status": str(resp.status)}
        except Exception:
            pass

        # Fall back to GET (some servers reject HEAD)
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout),
                                    ssl=ssl_ctx, allow_redirects=True) as resp:
                if resp.status < 500:
                    # Quick check for parked/dead page content
                    try:
                        body = await resp.text(encoding="utf-8", errors="replace")
                        body_lower = body[:2000].lower()
                        if any(indicator in body_lower for indicator in PARKED_INDICATORS):
                            return {"domain": domain, "alive": False, "status": "parked/placeholder"}
                    except Exception:
                        pass
                    return {"domain": domain, "alive": True, "status": str(resp.status)}
        except Exception:
            continue

    return {"domain": domain, "alive": False, "status": "unreachable"}


async def check_domains_batch(domains: list[str], concurrency: int = 20, timeout: int = 6) -> dict:
    """
    Check many domains concurrently. Returns {domain: {alive, status}} dict.
    Uses a semaphore to limit concurrency and avoid overwhelming the network.
    """
    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency, ttl_dns_cache=300)

    async def bounded_check(session, domain):
        async with sem:
            return await check_single_domain(session, domain, timeout)

    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        tasks = [bounded_check(session, d) for d in domains]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    out = {}
    for r in results:
        if isinstance(r, Exception):
            continue
        out[r["domain"]] = {"alive": r["alive"], "status": r["status"]}
    return out


def _build_contains_group_expr(col_expr, group: dict):
    """Build a Polars expression for a single value group (tags + logic)."""
    tags = [v.strip() for v in group.get("tags", []) if v.strip()]
    logic = group.get("logic", "and").lower()
    if not tags:
        return pl.lit(True)
    if logic == "and":
        expr = pl.lit(True)
        for v in tags:
            expr = expr & col_expr.str.contains(v.lower(), literal=True)
    else:
        expr = pl.lit(False)
        for v in tags:
            expr = expr | col_expr.str.contains(v.lower(), literal=True)
    return expr


def _rule_values_preview(rule: dict, max_items: int = 3) -> str:
    groups = rule.get("groups", [])
    if groups:
        values = [str(v).strip() for g in groups for v in g.get("tags", []) if str(v).strip()]
    else:
        values = [str(v).strip() for v in rule.get("values", []) if str(v).strip()]
    if not values:
        return ""
    shown = values[:max_items]
    text = ", ".join(shown)
    if len(values) > max_items:
        text = f"{text}, +{len(values) - max_items} more"
    return text


def _format_rule_reason(rule: dict, index: int) -> str:
    field = str(rule.get("field") or "field")
    match_type = str(rule.get("matchType") or "").lower()
    values_preview = _rule_values_preview(rule)

    if match_type == "range":
        min_val = str(rule.get("min") or "").strip()
        max_val = str(rule.get("max") or "").strip()
        if min_val and max_val:
            clause = f"{field} between {min_val} and {max_val}"
        elif min_val:
            clause = f"{field} at least {min_val}"
        elif max_val:
            clause = f"{field} at most {max_val}"
        else:
            clause = f"{field} range rule"
        return f"Failed rule {index}: {clause}"

    if match_type == "dates":
        start_date = str(rule.get("startDate") or "").strip()
        end_date = str(rule.get("endDate") or "").strip()
        if start_date and end_date:
            clause = f"{field} between {start_date} and {end_date}"
        elif start_date:
            clause = f"{field} on or after {start_date}"
        elif end_date:
            clause = f"{field} on or before {end_date}"
        else:
            clause = f"{field} date range rule"
        return f"Failed rule {index}: {clause}"

    if match_type == "contains":
        clause = f"{field} contains"
    elif match_type == "not_contains":
        clause = f"{field} does not contain"
    elif match_type == "exact":
        clause = f"{field} equals"
    elif match_type == "not_exact":
        clause = f"{field} is not"
    elif match_type == "fuzzy":
        clause = f"{field} fuzzy match"
    elif match_type == "excludes":
        clause = f"{field} excludes"
    elif match_type == "multivalue_any":
        clause = f"{field} contains any of"
    elif match_type == "multivalue_all":
        clause = f"{field} contains all of"
    elif match_type == "multivalue_exclude":
        clause = f"{field} excludes any of"
    elif match_type == "geo_country":
        clause = f"{field} is in country"
    else:
        clause = f"{field} ({match_type or 'rule'})"

    if values_preview:
        return f"Failed rule {index}: {clause} {values_preview}"
    return f"Failed rule {index}: {clause}"


def apply_rules_with_trace(df: pl.DataFrame, rules: list[dict]) -> tuple[pl.DataFrame, dict[int, str]]:
    """
    Apply rules and return:
    - filtered dataframe
    - row_id -> specific failed rule reason (first rule that removed the row)
    """
    if "__row_id" not in df.columns:
        filtered = apply_rules(df, rules)
        return filtered, {}

    working = df
    removed_reason_by_id: dict[int, str] = {}

    for idx, rule in enumerate(rules, start=1):
        before_height = working.height
        after = apply_rules(working, [rule])

        if after.height < before_height:
            # Use anti_join to find removed rows efficiently (avoids materializing two full sets)
            removed_df = working.join(after.select("__row_id"), on="__row_id", how="anti")
            reason = _format_rule_reason(rule, idx)
            for row_id in removed_df["__row_id"].to_list():
                removed_reason_by_id[int(row_id)] = reason
        working = after

    return working, removed_reason_by_id


def apply_rules(df: pl.DataFrame, rules: list[dict]) -> pl.DataFrame:
    """
    Apply ICP rules to filter the DataFrame.
    Rules are ANDed together. Within a 'contains' rule, groups combine via groupsLogic.
    """
    for rule in rules:
        field = rule["field"]
        match_type = rule["matchType"]

        if field not in df.columns:
            continue

        # Rows with blank/null values pass through — only reject rows
        # that have an actual value failing the rule.
        _text_col = pl.col(field).cast(pl.Utf8, strict=False)
        _is_blank = _text_col.is_null() | (_text_col.str.strip_chars() == "")

        if match_type == "exact":
            # Flatten groups into values, or use legacy values
            groups = rule.get("groups", [])
            if groups:
                values = [v.strip() for g in groups for v in g.get("tags", []) if v.strip()]
            else:
                values = [v.strip() for v in rule.get("values", []) if v.strip()]
            if not values:
                continue
            lower_vals = [v.lower() for v in values]
            mask = _is_blank | df[field].cast(pl.Utf8).str.to_lowercase().is_in(lower_vals)
            df = df.filter(mask)

        elif match_type == "not_exact":
            groups = rule.get("groups", [])
            if groups:
                values = [v.strip() for g in groups for v in g.get("tags", []) if v.strip()]
            else:
                values = [v.strip() for v in rule.get("values", []) if v.strip()]
            if not values:
                continue
            lower_vals = [v.lower() for v in values]
            mask = _is_blank | ~df[field].cast(pl.Utf8).str.to_lowercase().is_in(lower_vals)
            df = df.filter(mask)

        elif match_type == "contains":
            groups = rule.get("groups", [])
            col_expr = df[field].cast(pl.Utf8).str.to_lowercase()

            if groups:
                # New grouped logic
                groups_logic = rule.get("groupsLogic", "or").lower()
                group_exprs = [_build_contains_group_expr(col_expr, g) for g in groups]
                group_exprs = [e for e in group_exprs if e is not None]
                if not group_exprs:
                    continue
                if groups_logic == "and":
                    combined = group_exprs[0]
                    for e in group_exprs[1:]:
                        combined = combined & e
                else:
                    combined = group_exprs[0]
                    for e in group_exprs[1:]:
                        combined = combined | e
                df = df.filter(_is_blank | combined)
            else:
                # Legacy flat values
                values = [v.strip() for v in rule.get("values", []) if v.strip()]
                if not values:
                    continue
                logic = rule.get("logic", "or").lower()
                if logic == "and":
                    combined_expr = pl.lit(True)
                    for v in values:
                        combined_expr = combined_expr & col_expr.str.contains(v.lower(), literal=True)
                else:
                    combined_expr = pl.lit(False)
                    for v in values:
                        combined_expr = combined_expr | col_expr.str.contains(v.lower(), literal=True)
                df = df.filter(_is_blank | combined_expr)

        elif match_type == "not_contains":
            groups = rule.get("groups", [])
            col_expr = df[field].cast(pl.Utf8).str.to_lowercase()

            if groups:
                groups_logic = rule.get("groupsLogic", "or").lower()
                group_exprs = [_build_contains_group_expr(col_expr, g) for g in groups]
                group_exprs = [e for e in group_exprs if e is not None]
                if not group_exprs:
                    continue
                if groups_logic == "and":
                    combined = group_exprs[0]
                    for e in group_exprs[1:]:
                        combined = combined & e
                else:
                    combined = group_exprs[0]
                    for e in group_exprs[1:]:
                        combined = combined | e
                df = df.filter(_is_blank | ~combined)
            else:
                values = [v.strip() for v in rule.get("values", []) if v.strip()]
                if not values:
                    continue
                logic = rule.get("logic", "or").lower()
                if logic == "and":
                    combined_expr = pl.lit(True)
                    for v in values:
                        combined_expr = combined_expr & col_expr.str.contains(v.lower(), literal=True)
                else:
                    combined_expr = pl.lit(False)
                    for v in values:
                        combined_expr = combined_expr | col_expr.str.contains(v.lower(), literal=True)
                df = df.filter(_is_blank | ~combined_expr)

        elif match_type == "fuzzy":
            groups = rule.get("groups", [])
            if groups:
                values = [v.strip() for g in groups for v in g.get("tags", []) if v.strip()]
            else:
                values = [v.strip() for v in rule.get("values", []) if v.strip()]
            if not values:
                continue
            threshold = float(rule.get("threshold", 80))
            mask = _is_blank | (
                df[field]
                .cast(pl.Utf8)
                .map_elements(lambda x: fuzzy_match(x, values, threshold), return_dtype=pl.Boolean)
            )
            df = df.filter(mask)

        elif match_type == "range":
            min_val = rule.get("min")
            max_val = rule.get("max")
            has_min = min_val is not None and str(min_val).strip()
            has_max = max_val is not None and str(max_val).strip()
            if not has_min and not has_max:
                continue
            include_blanks = bool(rule.get("includeBlankValues", False))
            text_expr = pl.col(field).cast(pl.Utf8, strict=False).fill_null("")
            # Extract the first contiguous number (handles "51-200", "$1,000", "500 employees", etc.)
            numeric_expr = (
                text_expr
                .str.replace_all(r",", "")
                .str.extract(r"(\d+\.?\d*)")
                .cast(pl.Float64, strict=False)
            )
            is_blank = numeric_expr.is_null()
            if has_min and has_max:
                in_range = (numeric_expr >= float(min_val)) & (numeric_expr <= float(max_val))
            elif has_min:
                in_range = numeric_expr >= float(min_val)
            else:
                in_range = numeric_expr <= float(max_val)
            range_expr = (is_blank | in_range) if include_blanks else (is_blank.not_() & in_range)
            df = df.filter(range_expr)

        elif match_type == "dates":
            start_raw = str(rule.get("startDate") or "").strip()
            end_raw = str(rule.get("endDate") or "").strip()
            if not start_raw and not end_raw:
                continue
            include_blanks = bool(rule.get("includeBlankValues", False))

            text_expr = pl.col(field).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
            parsed_expr = text_expr.str.strptime(pl.Datetime, strict=False)

            start_dt = _safe_parse_iso_datetime(start_raw) if start_raw else None
            end_dt = _safe_parse_iso_datetime(end_raw) if end_raw else None

            if start_dt or end_dt:
                date_expr = parsed_expr.is_not_null()
                if start_dt:
                    date_expr = date_expr & (parsed_expr >= start_dt)
                elif start_raw:
                    date_expr = date_expr & (text_expr >= start_raw)
                if end_dt:
                    date_expr = date_expr & (parsed_expr <= end_dt)
                elif end_raw:
                    date_expr = date_expr & (text_expr <= end_raw)
            else:
                date_expr = text_expr != ""
                if start_raw:
                    date_expr = date_expr & (text_expr >= start_raw)
                if end_raw:
                    date_expr = date_expr & (text_expr <= end_raw)

            if include_blanks:
                is_blank_date = parsed_expr.is_null() if (start_dt or end_dt) else (text_expr == "")
                date_expr = date_expr | is_blank_date

            df = df.filter(date_expr)

        elif match_type == "excludes":
            groups = rule.get("groups", [])
            if groups:
                values = [v.strip() for g in groups for v in g.get("tags", []) if v.strip()]
            else:
                values = [v.strip() for v in rule.get("values", []) if v.strip()]
            if not values:
                continue
            threshold = float(rule.get("threshold", 80))
            mask = (
                df[field]
                .cast(pl.Utf8)
                .map_elements(lambda x: not fuzzy_match(x, values, threshold), return_dtype=pl.Boolean)
            )
            df = df.filter(mask)

        elif match_type in ("multivalue_any", "multivalue_all", "multivalue_exclude"):
            groups = rule.get("groups", [])
            if groups:
                values = [v.strip().lower() for g in groups for v in g.get("tags", []) if v.strip()]
            else:
                values = [v.strip().lower() for v in rule.get("values", []) if v.strip()]
            if not values:
                continue
            sep = rule.get("separator") or ";"
            values_set = set(values)

            def _mv_check(cell_val, match_type=match_type, vs=values_set, sp=sep):
                if cell_val is None:
                    return match_type != "multivalue_exclude"
                parts = {p.strip().lower() for p in str(cell_val).split(sp) if p.strip()}
                if match_type == "multivalue_any":
                    return bool(parts & vs)
                elif match_type == "multivalue_all":
                    return vs.issubset(parts)
                else:  # multivalue_exclude
                    return not bool(parts & vs)

            mask = df[field].cast(pl.Utf8, strict=False).map_elements(_mv_check, return_dtype=pl.Boolean)
            df = df.filter(mask)

        elif match_type == "geo_country":
            groups = rule.get("groups", [])
            if groups:
                values = [v.strip() for g in groups for v in g.get("tags", []) if v.strip()]
            else:
                values = [v.strip() for v in rule.get("values", []) if v.strip()]
            if not values:
                continue
            normalized_targets = {normalize_country(v) for v in values}
            mask = df[field].cast(pl.Utf8, strict=False).fill_null("").map_elements(
                lambda x, targets=normalized_targets: normalize_country(x) in targets,
                return_dtype=pl.Boolean,
            )
            df = df.filter(mask)

    return df


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/api/upload")
async def upload_csv(
    files: Optional[list[UploadFile]] = File(None),
    file: Optional[UploadFile] = File(None),
):
    """Parse uploaded CSV and return column metadata for rule building."""
    source_files = _resolve_upload_files(files, file)
    source_bundle = await _parse_upload_datasets(source_files, label="source")
    df = source_bundle["df"]
    file_names = source_bundle["fileNames"]

    columns_info, column_profiles = build_columns_info(df)
    preview_rows = df.head(PREVIEW_ROW_LIMIT).to_dicts()

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    tmp.close()
    df.write_csv(tmp.name)

    return {
        "fileName": _summarize_file_names(file_names, "dataset.csv"),
        "fileNames": file_names,
        "fileCount": len(file_names),
        "columns": columns_info,
        "columnProfiles": column_profiles,
        "totalRows": df.height,
        "sourceRows": source_bundle["inputRows"],
        "sourceMappings": source_bundle["mapping"],
        "tempPath": tmp.name,
        "preview": preview_rows,  # backward-compatible legacy field
        "previewRows": preview_rows,
    }


@app.post("/api/session/upload")
async def session_upload(
    files: Optional[list[UploadFile]] = File(None),
    file: Optional[UploadFile] = File(None),
    dedupeFiles: Optional[list[UploadFile]] = File(None),
    dedupeFile: Optional[UploadFile] = File(None),
):
    """
    Sessionized upload endpoint.
    Stores source CSV in-memory and returns session metadata for subsequent preview/qualify/export calls.
    """
    source_files = _resolve_upload_files(files, file)
    source_bundle = await _parse_upload_datasets(source_files, label="source")
    df = source_bundle["df"]
    source_file_names = source_bundle["fileNames"]
    columns_info, column_profiles = build_columns_info(df)
    anomalies = build_column_anomalies(column_profiles, df.height)
    preview_rows = df.head(PREVIEW_ROW_LIMIT).to_dicts()

    dedupe_files = _resolve_upload_files(dedupeFiles, dedupeFile)
    dedupe_bundle = await _parse_upload_datasets(dedupe_files, label="dedupe") if dedupe_files else None
    parsed_dedupe_df = dedupe_bundle["df"] if dedupe_bundle else None
    dedupe_file_names = dedupe_bundle["fileNames"] if dedupe_bundle else []
    dedupe_name = _summarize_file_names(dedupe_file_names, "hubspot.csv") if dedupe_bundle else None
    dedupe_raw = dedupe_bundle["raws"][0] if dedupe_bundle and dedupe_bundle["raws"] else None

    source_name = _summarize_file_names(source_file_names, "dataset.csv")
    sid = _put_session(
        raw_csv=source_bundle["raws"][0],
        file_name=source_name,
        df=df,
        columns_info=columns_info,
        column_profiles=column_profiles,
        preview_rows=preview_rows,
        anomalies=anomalies,
        dedupe_raw=dedupe_raw,
        dedupe_name=dedupe_name,
        dedupe_df=parsed_dedupe_df,
        source_raws=source_bundle["raws"],
        source_file_names=source_file_names,
        source_mapping=source_bundle["mapping"],
        source_rows=source_bundle["inputRows"],
        dedupe_raws=dedupe_bundle["raws"] if dedupe_bundle else [],
        dedupe_file_names=dedupe_file_names,
        dedupe_mapping=dedupe_bundle["mapping"] if dedupe_bundle else [],
        dedupe_source_rows=dedupe_bundle["inputRows"] if dedupe_bundle else 0,
    )
    dedupe_payload = {
        "enabled": bool(dedupe_bundle),
        "fileName": dedupe_name,
        "fileNames": dedupe_file_names,
        "fileCount": len(dedupe_file_names),
    }
    if parsed_dedupe_df is not None:
        inferred_matches = infer_dedupe_matches(df.columns, parsed_dedupe_df.columns)
        primary_match = inferred_matches[0] if inferred_matches else {"sourceColumn": None, "hubspotColumn": None, "keyType": None}
        dedupe_payload.update({
            "columns": parsed_dedupe_df.columns,
            "totalRows": parsed_dedupe_df.height,
            "sourceRows": dedupe_bundle["inputRows"] if dedupe_bundle else parsed_dedupe_df.height,
            "sourceMappings": dedupe_bundle["mapping"] if dedupe_bundle else [],
            "inferredMatch": {
                "sourceColumn": primary_match.get("sourceColumn"),
                "hubspotColumn": primary_match.get("hubspotColumn"),
                "keyType": primary_match.get("keyType"),
            },
            "inferredMatches": inferred_matches,
        })

    return {
        "sessionId": sid,
        "fileName": source_name,
        "fileNames": source_file_names,
        "fileCount": len(source_file_names),
        "columns": columns_info,
        "columnProfiles": column_profiles,
        "previewRows": preview_rows,
        "totalRows": df.height,
        "sourceRows": source_bundle["inputRows"],
        "sourceMappings": source_bundle["mapping"],
        "anomalies": anomalies,
        "dedupe": dedupe_payload,
    }


@app.get("/api/sessions")
async def list_sessions():
    """List all non-expired sessions with metadata."""
    _clean_stale_sessions()
    sessions = []
    for sid, session in SESSION_STORE.items():
        source_file_names = list(session.get("sourceFileNames") or [])
        file_name = session.get("fileName") or _summarize_file_names(source_file_names, "dataset.csv")
        # Use cached sourceRows to avoid deserializing the full DataFrame.
        total_rows = int(session.get("sourceRows") or 0)
        if not total_rows and isinstance(session.get("df"), pl.DataFrame):
            total_rows = session["df"].height
        sessions.append({
            "sessionId": sid,
            "fileName": file_name,
            "fileNames": source_file_names,
            "totalRows": total_rows,
            "createdAt": session.get("createdAt", 0),
            "updatedAt": session.get("updatedAt", 0),
        })
    sessions.sort(key=lambda s: s["updatedAt"], reverse=True)
    return {"sessions": sessions}


@app.post("/api/session/create")
async def create_blank_session(name: str = Form("Untitled")):
    """Create a new blank session with a name."""
    clean_name = str(name or "Untitled").strip() or "Untitled"
    _clean_stale_sessions()
    sid = uuid4().hex
    now = time.time()
    empty_df = pl.DataFrame()
    SESSION_STORE[sid] = {
        "csvRaw": None,
        "dfParquet": None,
        "df": empty_df,
        "fileName": clean_name,
        "sourceFileNames": [],
        "sourceRaws": [],
        "sourceMapping": [],
        "sourceRows": 0,
        "columns": [],
        "columnProfiles": [],
        "previewRows": [],
        "anomalies": {},
        "dedupeRaw": None,
        "dedupeName": None,
        "dedupeFileNames": [],
        "dedupeRaws": [],
        "dedupeMapping": [],
        "dedupeSourceRows": 0,
        "dedupeDf": None,
        "workspaceConfig": {},
        "activeRun": None,
        "activeScrape": None,
        "lastRunResult": None,
        "lastRunStatus": None,
        "createdAt": now,
        "updatedAt": now,
    }
    _persist_session(sid, SESSION_STORE[sid])
    payload = _build_session_payload(sid, SESSION_STORE[sid])
    return payload


@app.post("/api/session/rename")
async def rename_session(sessionId: str = Form(...), name: str = Form(...)):
    """Rename an existing session."""
    session = _touch_session(sessionId)
    clean_name = str(name or "").strip()
    if not clean_name:
        raise HTTPException(status_code=400, detail="Name cannot be empty.")
    session["fileName"] = clean_name
    session["updatedAt"] = time.time()
    _persist_session(sessionId, session)
    return {"sessionId": sessionId, "fileName": clean_name}


@app.delete("/api/session/{sessionId}")
async def delete_session(sessionId: str):
    """Delete a session and its persisted pickle."""
    SESSION_STORE.pop(sessionId, None)
    persist_path = _session_persist_path(sessionId)
    if persist_path.exists():
        persist_path.unlink(missing_ok=True)
    return {"sessionId": sessionId, "deleted": True}


@app.get("/api/session/state")
async def session_state(sessionId: str):
    """Return persisted session metadata so users can restore work from Recent runs."""
    session = _touch_session(sessionId)
    payload = _build_session_payload(sessionId, session)
    _persist_session(sessionId, session)
    return payload


@app.get("/api/session/latest")
async def session_latest():
    """Return most recently updated persisted session payload."""
    _clean_stale_sessions()
    if not SESSION_STORE:
        raise HTTPException(status_code=404, detail="No persisted sessions available.")
    latest_id, latest_session = max(
        SESSION_STORE.items(),
        key=lambda item: float(item[1].get("updatedAt") or 0.0),
    )
    payload = _build_session_payload(latest_id, latest_session)
    _persist_session(latest_id, latest_session)
    return payload


@app.post("/api/session/config")
async def session_set_config(
    sessionId: str = Form(...),
    rules: str = Form("[]"),
    domainCheck: str = Form("false"),
    homepageCheck: str = Form("false"),
    domainField: str = Form(""),
    websiteKeywords: str = Form("[]"),
    websiteExcludeKeywords: str = Form("[]"),
    excludeCountryTlds: str = Form("false"),
    tldDisallowList: str = Form("[]"),
    tldAllowList: str = Form("[]"),
    intraDedupe: str = Form("false"),
    intraDedupeColumns: str = Form("[]"),
    intraDedupeStrategy: str = Form("first"),
    domainBlocklistEnabled: str = Form("false"),
    domainBlocklistCategories: str = Form(""),
    customBlockedDomains: str = Form("[]"),
    scoreEnabled: str = Form("false"),
    scoreWeights: str = Form("{}"),
    scoreDateField: str = Form(""),
    scoreHighSignalConfig: str = Form("{}"),
):
    """Persist workspace config so sessions restore with previous settings."""
    session = _touch_session(sessionId)
    parsed_rules = _parse_rules_payload(rules)
    try:
        intra_dedupe_cols = json.loads(intraDedupeColumns or "[]")
    except (json.JSONDecodeError, TypeError):
        intra_dedupe_cols = []
    try:
        parsed_score_weights = json.loads(scoreWeights) if scoreWeights else {}
    except (json.JSONDecodeError, TypeError):
        parsed_score_weights = {}
    try:
        parsed_score_signal = json.loads(scoreHighSignalConfig) if scoreHighSignalConfig else {}
    except (json.JSONDecodeError, TypeError):
        parsed_score_signal = {}
    config_payload = {
        "rules": parsed_rules,
        "domainCheck": _parse_form_bool(domainCheck),
        "homepageCheck": _parse_form_bool(homepageCheck),
        "domainField": str(domainField or "").strip(),
        "websiteKeywords": _parse_website_keywords_payload(websiteKeywords),
        "websiteExcludeKeywords": _parse_website_keywords_payload(websiteExcludeKeywords),
        "excludeCountryTlds": _parse_form_bool(excludeCountryTlds),
        "disallowedTlds": sorted(_parse_tld_list_payload(tldDisallowList)),
        "allowedTlds": sorted(_parse_tld_list_payload(tldAllowList)),
        "intraDedupe": _parse_form_bool(intraDedupe),
        "intraDedupeColumns": intra_dedupe_cols,
        "domainBlocklistEnabled": _parse_form_bool(domainBlocklistEnabled),
        "domainBlocklistCategories": _parse_blocklist_categories(domainBlocklistCategories),
        "customBlockedDomains": _parse_custom_blocked_domains(customBlockedDomains),
        "intraDedupeStrategy": intraDedupeStrategy if intraDedupeStrategy in ("first", "last", "merge") else "first",
        "scoreEnabled": _parse_form_bool(scoreEnabled),
        "scoreWeights": parsed_score_weights,
        "scoreDateField": scoreDateField,
        "scoreHighSignalConfig": parsed_score_signal,
    }
    session["workspaceConfig"] = config_payload
    session["updatedAt"] = time.time()
    _persist_session(sessionId, session)
    return {"sessionId": sessionId, "workspaceConfig": config_payload}


@app.post("/api/session/dedupe")
async def session_set_dedupe(
    sessionId: str = Form(...),
    dedupeFiles: Optional[list[UploadFile]] = File(None),
    dedupeFile: Optional[UploadFile] = File(None),
):
    """Attach (append) or clear HubSpot dedupe files for an existing session."""
    session = _touch_session(sessionId)
    source_df = _get_session_df(session)
    dedupe_uploads = _resolve_upload_files(dedupeFiles, dedupeFile)
    if not dedupe_uploads:
        _set_session_dedupe(sessionId, None, None, None, dedupe_raws=[], dedupe_file_names=[], dedupe_mapping=[], dedupe_source_rows=0)
        return {"sessionId": sessionId, "dedupe": {"enabled": False, "fileName": None, "fileNames": [], "fileCount": 0, "columns": [], "totalRows": 0}}

    dedupe_bundle = await _parse_upload_datasets(dedupe_uploads, label="dedupe")
    existing_raws = [raw for raw in (session.get("dedupeRaws") or []) if raw]
    existing_names = [str(name or "").strip() for name in (session.get("dedupeFileNames") or []) if str(name or "").strip()]

    dedupe_raws = existing_raws + list(dedupe_bundle["raws"])
    dedupe_file_names = existing_names + list(dedupe_bundle["fileNames"])
    dedupe_df, dedupe_mapping, dedupe_input_rows = _rebuild_dedupe_from_raws(dedupe_raws, dedupe_file_names)
    if dedupe_df is None:
        raise HTTPException(status_code=400, detail="Unable to parse dedupe files.")

    dedupe_name = _summarize_file_names(dedupe_file_names, "hubspot.csv")
    _set_session_dedupe(
        sessionId,
        dedupe_raws[0],
        dedupe_name,
        dedupe_df,
        dedupe_raws=dedupe_raws,
        dedupe_file_names=dedupe_file_names,
        dedupe_mapping=dedupe_mapping,
        dedupe_source_rows=dedupe_input_rows,
    )
    inferred_matches = infer_dedupe_matches(source_df.columns, dedupe_df.columns)
    primary_match = inferred_matches[0] if inferred_matches else {"sourceColumn": None, "hubspotColumn": None, "keyType": None}
    return {
        "sessionId": sessionId,
        "dedupe": {
            "enabled": True,
            "fileName": dedupe_name,
            "fileNames": dedupe_file_names,
            "fileCount": len(dedupe_file_names),
            "columns": dedupe_df.columns,
            "totalRows": dedupe_df.height,
            "sourceRows": dedupe_input_rows,
            "sourceMappings": dedupe_mapping,
            "inferredMatch": {
                "sourceColumn": primary_match.get("sourceColumn"),
                "hubspotColumn": primary_match.get("hubspotColumn"),
                "keyType": primary_match.get("keyType"),
            },
            "inferredMatches": inferred_matches,
        },
    }


@app.post("/api/session/column-values")
async def session_column_values(
    sessionId: str = Form(...),
    column: str = Form(...),
    separator: str = Form(";"),
    limit: int = Form(200),
):
    """Return distinct individual values from a multi-value column, with occurrence counts."""
    session = _touch_session(sessionId)
    df = _get_session_df(session)
    if column not in df.columns:
        raise HTTPException(status_code=400, detail=f"Column '{column}' not found")

    utf = df[column].cast(pl.Utf8, strict=False).drop_nulls()
    # Split each cell by separator, explode, trim, and count
    counts: dict[str, int] = {}
    for cell in utf.to_list():
        for part in str(cell).split(separator):
            v = part.strip()
            if v:
                counts[v] = counts.get(v, 0) + 1

    # Sort by count descending, then alphabetically
    sorted_values = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:limit]
    return {"column": column, "values": [{"value": v, "count": c} for v, c in sorted_values]}


@app.post("/api/session/preview")
async def session_preview(
    sessionId: str = Form(...),
    rules: str = Form("[]"),
    domainCheck: str = Form("false"),
    homepageCheck: str = Form("false"),
    domainField: str = Form(""),
    websiteKeywords: str = Form("[]"),
    websiteExcludeKeywords: str = Form("[]"),
    excludeCountryTlds: str = Form("false"),
    tldDisallowList: str = Form("[]"),
    tldAllowList: str = Form("[]"),
    intraDedupe: str = Form("false"),
    intraDedupeColumns: str = Form("[]"),
    intraDedupeStrategy: str = Form("first"),
    domainBlocklistEnabled: str = Form("false"),
    domainBlocklistCategories: str = Form(""),
    customBlockedDomains: str = Form("[]"),
):
    """Preview estimate using an existing uploaded session."""
    session = _touch_session(sessionId)
    df = _get_session_df(session)
    dedupe_df = _get_session_dedupe_df(session)
    parsed_rules = _parse_rules_payload(rules)
    exclude_country_tlds = _parse_form_bool(excludeCountryTlds)
    disallowed_tlds = _parse_tld_list_payload(tldDisallowList)
    allowed_tlds = _parse_tld_list_payload(tldAllowList)
    website_keywords = _parse_website_keywords_payload(websiteKeywords)
    website_exclude_keywords = _parse_website_keywords_payload(websiteExcludeKeywords)
    intra_dedupe_enabled = _parse_form_bool(intraDedupe)
    blocklist_enabled = _parse_form_bool(domainBlocklistEnabled)
    blocklist_categories = _parse_blocklist_categories(domainBlocklistCategories)
    custom_blocked = _parse_custom_blocked_domains(customBlockedDomains)
    try:
        intra_dedupe_cols = json.loads(intraDedupeColumns or "[]")
    except (json.JSONDecodeError, TypeError):
        intra_dedupe_cols = []
    intra_dedupe_strategy = intraDedupeStrategy if intraDedupeStrategy in ("first", "last", "merge") else "first"
    pipeline = await run_qualification_pipeline(
        df=df,
        parsed_rules=parsed_rules,
        domain_check=_parse_form_bool(domainCheck),
        homepage_check=_parse_form_bool(homepageCheck),
        domain_field=domainField,
        website_keywords=website_keywords,
        website_exclude_keywords=website_exclude_keywords,
        exclude_country_tlds=exclude_country_tlds,
        disallowed_tlds=disallowed_tlds,
        allowed_tlds=allowed_tlds,
        dedupe_raw=session.get("dedupeRaw"),
        dedupe_df=dedupe_df,
        include_rows=False,
        include_leads=False,
        skip_network_checks=True,
        intra_dedupe_enabled=intra_dedupe_enabled,
        intra_dedupe_columns=intra_dedupe_cols if intra_dedupe_cols else None,
        intra_dedupe_strategy=intra_dedupe_strategy,
        blocked_domain_suffixes=build_blocked_suffixes(blocklist_categories, custom_blocked) if blocklist_enabled else None,
    )
    return {
        "sessionId": sessionId,
        "estimatedQualifiedCount": pipeline["qualifiedCount"],
        "estimatedRemovedCount": pipeline["removedCount"],
        "removedBreakdown": pipeline["removedBreakdown"],
        "totalRows": df.height,
        "dedupeMeta": pipeline["dedupeInfo"],
    }


@app.post("/api/session/dedupe/preview")
async def session_dedupe_preview(
    sessionId: str = Form(...),
    intraDedupe: str = Form("false"),
    intraDedupeColumns: str = Form("[]"),
    intraDedupeStrategy: str = Form("first"),
):
    """Preview deduplication without mutating session data."""
    session = _touch_session(sessionId)
    df = _get_session_df(session)
    dedupe_df = _get_session_dedupe_df(session)

    intra_dedupe_enabled = _parse_form_bool(intraDedupe)
    try:
        intra_dedupe_cols = json.loads(intraDedupeColumns or "[]")
    except (json.JSONDecodeError, TypeError):
        intra_dedupe_cols = []
    intra_dedupe_strategy = intraDedupeStrategy if intraDedupeStrategy in ("first", "last", "merge") else "first"

    intra_result: dict = {"enabled": False, "wouldRemove": 0, "sampleMatches": []}
    hubspot_result: dict = {"enabled": False, "wouldRemove": 0, "sampleMatches": []}

    # --- Intra-dataset dedupe preview ---
    if intra_dedupe_enabled and df.height > 0:
        _, intra_info = apply_intra_dedupe(
            df,
            key_columns=intra_dedupe_cols if intra_dedupe_cols else None,
            strategy=intra_dedupe_strategy,
        )
        intra_result["enabled"] = intra_info.get("enabled", False)
        intra_result["wouldRemove"] = intra_info.get("removedCount", 0)
        intra_result["keyColumns"] = intra_info.get("keyColumns", [])
        intra_result["strategy"] = intra_info.get("strategy", "first")
        intra_result["warnings"] = intra_info.get("warnings", [])

    # --- HubSpot dedupe preview ---
    if dedupe_df is not None and df.height > 0:
        _, hubspot_info = apply_hubspot_dedupe(
            df,
            dedupe_df=dedupe_df,
        )
        hubspot_result["enabled"] = hubspot_info.get("enabled", False)
        hubspot_result["wouldRemove"] = hubspot_info.get("removedCount", 0)
        hubspot_result["keyType"] = hubspot_info.get("keyType")
        hubspot_result["matches"] = hubspot_info.get("matches", [])
        hubspot_result["warnings"] = hubspot_info.get("warnings", [])

    return {
        "sessionId": sessionId,
        "intra": intra_result,
        "hubspot": hubspot_result,
        "totalRows": df.height,
    }


@app.post("/api/session/rows/bulk-export")
async def session_bulk_export(
    sessionId: str = Form(...),
    rowIds: str = Form("[]"),
    exportColumns: str = Form("[]"),
):
    """Export a subset of rows as CSV by row IDs."""
    session = _touch_session(sessionId)
    try:
        selected_ids = json.loads(rowIds or "[]")
    except (json.JSONDecodeError, TypeError):
        selected_ids = []
    try:
        col_list = json.loads(exportColumns or "[]")
    except (json.JSONDecodeError, TypeError):
        col_list = []

    active_run = session.get("activeRun")
    if not active_run or not active_run.get("result"):
        raise HTTPException(status_code=400, detail="No completed run to export from.")

    result = active_run["result"]
    rows = result.get("rows") or []
    if not rows:
        raise HTTPException(status_code=400, detail="Run has no rows.")

    selected_set = set()
    for rid in selected_ids:
        if isinstance(rid, (int, float)):
            selected_set.add(int(rid))
        elif isinstance(rid, str):
            try:
                selected_set.add(int(rid))
            except (ValueError, TypeError):
                selected_set.add(rid)

    matched = [row for row in rows if row.get("_rowId") in selected_set or row.get("__row_id") in selected_set]
    if not matched:
        raise HTTPException(status_code=400, detail="No matching rows found for the provided IDs.")

    all_columns = result.get("columns") or list(matched[0].keys())
    effective_cols = [c for c in col_list if c in all_columns] if col_list else [c for c in all_columns if not c.startswith("_")]
    if not effective_cols:
        effective_cols = [c for c in all_columns if not c.startswith("_")]

    export_df = pl.DataFrame([{col: str(row.get(col) or "") for col in effective_cols} for row in matched])
    csv_bytes = export_df.write_csv().encode("utf-8")
    return Response(
        content=csv_bytes,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=selected_rows.csv"},
    )


@app.post("/api/session/rows/bulk-status")
async def session_bulk_status(
    sessionId: str = Form(...),
    rowIds: str = Form("[]"),
    newStatus: str = Form("qualified"),
):
    """Update status annotations for specific rows in the active run."""
    session = _touch_session(sessionId)
    try:
        selected_ids = json.loads(rowIds or "[]")
    except (json.JSONDecodeError, TypeError):
        selected_ids = []

    if newStatus not in ("qualified", "removed_manual"):
        raise HTTPException(status_code=400, detail="newStatus must be 'qualified' or 'removed_manual'.")

    active_run = session.get("activeRun")
    if not active_run or not active_run.get("result"):
        raise HTTPException(status_code=400, detail="No completed run to modify.")

    result = active_run["result"]
    rows = result.get("rows") or []

    selected_set = set()
    for rid in selected_ids:
        if isinstance(rid, (int, float)):
            selected_set.add(int(rid))
        elif isinstance(rid, str):
            try:
                selected_set.add(int(rid))
            except (ValueError, TypeError):
                selected_set.add(rid)

    changed = 0
    for row in rows:
        row_id = row.get("_rowId") or row.get("__row_id")
        if row_id in selected_set:
            row["_rowStatus"] = newStatus
            if newStatus != "removed_hubspot":
                row.pop("_dedupeMatch", None)
            changed += 1

    # Recount
    qualified_count = sum(1 for r in rows if r.get("_rowStatus") == "qualified")
    removed_count = len(rows) - qualified_count
    result["qualifiedCount"] = qualified_count
    result["removedCount"] = removed_count

    session["updatedAt"] = time.time()
    _persist_session(sessionId, session)

    return {
        "sessionId": sessionId,
        "changed": changed,
        "qualifiedCount": qualified_count,
        "removedCount": removed_count,
    }


@app.post("/api/session/rows")
async def session_rows(
    sessionId: str = Form(...),
    page: int = Form(1),
    pageSize: int = Form(100),
    search: str = Form(""),
    sortCol: str = Form(""),
    sortDir: str = Form(""),
    viewFilters: str = Form("[]"),
):
    """
    Paginated table rows for active session datasets.
    Optimized for pre-qualification table browsing across full spreadsheets.
    """
    session = _touch_session(sessionId)
    df = _get_session_df(session)

    safe_size = max(10, min(int(pageSize or 100), 500))
    safe_page = max(1, int(page or 1))

    working = df.with_row_count("__row_id")
    working = _apply_view_filters(working, viewFilters, df.columns)
    query = (search or "").strip().lower()

    if query:
        expr = None
        for col in df.columns:
            col_expr = (
                pl.col(col)
                .cast(pl.Utf8, strict=False)
                .str.to_lowercase()
                .str.contains(query, literal=True)
            )
            expr = col_expr if expr is None else (expr | col_expr)
        if expr is not None:
            working = working.filter(expr)

    if sortCol and sortCol in df.columns and sortDir.lower() in ("asc", "desc"):
        descending = sortDir.lower() == "desc"
        try:
            working = working.sort(sortCol, descending=descending, nulls_last=True)
        except Exception:
            working = working.sort(
                pl.col(sortCol).cast(pl.Utf8, strict=False).str.to_lowercase(),
                descending=descending,
            )

    filtered_rows = working.height
    total_pages = max(1, (filtered_rows + safe_size - 1) // safe_size)
    safe_page = min(safe_page, total_pages)
    start = (safe_page - 1) * safe_size
    page_df = working.slice(start, safe_size)

    rows = []
    active_run = session.get("activeRun")
    running_run = (
        active_run
        if isinstance(active_run, dict) and active_run.get("status") in {"running", "pausing", "paused"}
        else None
    )
    status_source = running_run or session.get("lastRunStatus")

    for item in page_df.to_dicts():
        row_id = int(item.pop("__row_id"))
        row_status, row_reasons, dedupe_match = _resolve_row_annotation(row_id, status_source)
        row_payload = {
            **item,
            "_rowId": row_id,
            "_rowStatus": row_status,
            "_rowReasons": row_reasons,
        }
        if isinstance(dedupe_match, dict):
            row_payload["_dedupeMatch"] = dedupe_match
        rows.append(row_payload)

    return {
        "sessionId": sessionId,
        "rows": rows,
        "totalRows": df.height,
        "filteredRows": filtered_rows,
        "page": safe_page,
        "pageSize": safe_size,
        "qualificationProgress": _serialize_run_snapshot(active_run, include_result=False) if isinstance(active_run, dict) else None,
    }


@app.post("/api/session/scrape/start")
async def session_scrape_start(
    sessionId: str = Form(...),
    domainField: str = Form(""),
):
    """Start an async homepage scraping enrichment job for the current session."""
    session = _touch_session(sessionId)
    running_qualification = session.get("activeRun")
    if isinstance(running_qualification, dict) and running_qualification.get("status") in {"running", "pausing"}:
        raise HTTPException(status_code=409, detail="Qualification is running. Pause or finish it before starting scraper enrichment.")

    active_scrape = session.get("activeScrape")
    if isinstance(active_scrape, dict) and active_scrape.get("status") == "running":
        payload = _serialize_scrape_snapshot(active_scrape, include_result=False)
        payload["sessionId"] = sessionId
        payload["alreadyRunning"] = True
        return payload

    df = _get_session_df(session)
    resolved_domain_field = _resolve_scrape_domain_field(df, domainField or (session.get("workspaceConfig") or {}).get("domainField", ""))
    if not resolved_domain_field:
        raise HTTPException(status_code=400, detail="Select a valid domain/website column before starting scraper enrichment.")

    scrape_id = uuid4().hex
    now = time.time()
    output_dir = SCRAPE_JOB_DIR / sessionId / scrape_id
    session["activeScrape"] = {
        "scrapeId": scrape_id,
        "status": "running",
        "stage": "starting",
        "progress": 0.01,
        "message": "Preparing homepage scraping job...",
        "processed": 0,
        "total": 0,
        "ok": 0,
        "fail": 0,
        "ratePerSec": 0.0,
        "domainField": resolved_domain_field,
        "outputDir": str(output_dir),
        "startedAt": now,
        "finishedAt": None,
        "error": "",
        "result": None,
    }
    session["updatedAt"] = now
    _persist_session(sessionId, session)

    asyncio.create_task(
        _run_session_scrape_job(
            session_id=sessionId,
            scrape_id=scrape_id,
            domain_field=resolved_domain_field,
        )
    )

    payload = _serialize_scrape_snapshot(session.get("activeScrape"), include_result=False)
    payload["sessionId"] = sessionId
    payload["alreadyRunning"] = False
    return payload


@app.get("/api/session/scrape/progress")
async def session_scrape_progress(sessionId: str):
    """Read progress for active scraper enrichment job."""
    session = _touch_session(sessionId)
    payload = _serialize_scrape_snapshot(session.get("activeScrape"), include_result=True)
    payload["sessionId"] = sessionId
    return payload


@app.post("/api/session/qualify/start")
async def session_qualify_start(
    sessionId: str = Form(...),
    rules: str = Form("[]"),
    domainCheck: str = Form("false"),
    homepageCheck: str = Form("false"),
    domainField: str = Form(""),
    websiteKeywords: str = Form("[]"),
    websiteExcludeKeywords: str = Form("[]"),
    excludeCountryTlds: str = Form("false"),
    tldDisallowList: str = Form("[]"),
    tldAllowList: str = Form("[]"),
    intraDedupe: str = Form("false"),
    intraDedupeColumns: str = Form("[]"),
    intraDedupeStrategy: str = Form("first"),
    domainBlocklistEnabled: str = Form("false"),
    domainBlocklistCategories: str = Form(""),
    customBlockedDomains: str = Form("[]"),
    scoreEnabled: str = Form("false"),
    scoreWeights: str = Form("{}"),
    scoreDateField: str = Form(""),
    scoreHighSignalConfig: str = Form("{}"),
):
    """Start an async qualification run for live progress updates."""
    session = _touch_session(sessionId)
    df = _get_session_df(session)
    parsed_rules = _parse_rules_payload(rules)
    domain_check = _parse_form_bool(domainCheck)
    homepage_check = _parse_form_bool(homepageCheck)
    website_keywords = _parse_website_keywords_payload(websiteKeywords)
    website_exclude_keywords = _parse_website_keywords_payload(websiteExcludeKeywords)
    exclude_country_tlds = _parse_form_bool(excludeCountryTlds)
    disallowed_tlds = _parse_tld_list_payload(tldDisallowList)
    allowed_tlds = _parse_tld_list_payload(tldAllowList)
    intra_dedupe_enabled = _parse_form_bool(intraDedupe)
    blocklist_enabled = _parse_form_bool(domainBlocklistEnabled)
    blocklist_categories = _parse_blocklist_categories(domainBlocklistCategories)
    custom_blocked = _parse_custom_blocked_domains(customBlockedDomains)
    try:
        intra_dedupe_cols = json.loads(intraDedupeColumns or "[]")
    except (json.JSONDecodeError, TypeError):
        intra_dedupe_cols = []
    intra_dedupe_strategy = intraDedupeStrategy if intraDedupeStrategy in ("first", "last", "merge") else "first"
    try:
        parsed_score_weights = json.loads(scoreWeights) if scoreWeights else {}
    except (json.JSONDecodeError, TypeError):
        parsed_score_weights = {}
    try:
        parsed_score_signal = json.loads(scoreHighSignalConfig) if scoreHighSignalConfig else {}
    except (json.JSONDecodeError, TypeError):
        parsed_score_signal = {}
    score_config = {
        "scoreEnabled": _parse_form_bool(scoreEnabled),
        "scoreWeights": parsed_score_weights,
        "scoreDateField": scoreDateField,
        "scoreHighSignalConfig": parsed_score_signal,
    } if _parse_form_bool(scoreEnabled) else None
    workspace_config = {
        "rules": parsed_rules,
        "domainCheck": domain_check,
        "homepageCheck": homepage_check,
        "domainField": domainField,
        "websiteKeywords": website_keywords,
        "websiteExcludeKeywords": website_exclude_keywords,
        "excludeCountryTlds": exclude_country_tlds,
        "disallowedTlds": sorted(disallowed_tlds),
        "allowedTlds": sorted(allowed_tlds),
        "intraDedupe": intra_dedupe_enabled,
        "intraDedupeColumns": intra_dedupe_cols,
        "intraDedupeStrategy": intra_dedupe_strategy,
        "domainBlocklistEnabled": blocklist_enabled,
        "domainBlocklistCategories": blocklist_categories,
        "customBlockedDomains": custom_blocked,
        "scoreEnabled": _parse_form_bool(scoreEnabled),
        "scoreWeights": parsed_score_weights,
        "scoreDateField": scoreDateField,
        "scoreHighSignalConfig": parsed_score_signal,
    }
    session["workspaceConfig"] = workspace_config

    active_scrape = session.get("activeScrape")
    if isinstance(active_scrape, dict) and active_scrape.get("status") == "running":
        raise HTTPException(status_code=409, detail="A homepage scrape is currently running. Wait for it to finish before qualification.")

    active = session.get("activeRun")
    if isinstance(active, dict) and active.get("status") in {"running", "pausing"}:
        payload = _serialize_run_snapshot(active, include_result=False)
        payload["sessionId"] = sessionId
        payload["alreadyRunning"] = True
        return payload

    run_id = uuid4().hex
    now = time.time()
    session["activeRun"] = {
        "runId": run_id,
        "status": "running",
        "stage": "starting",
        "progress": 0.02,
        "message": "Preparing qualification run...",
        "processedRows": 0,
        "totalRows": int(df.height),
        "qualifiedCount": 0,
        "removedCount": 0,
        "removedBreakdown": {
            "removedFilter": 0,
            "removedDomain": 0,
            "removedHubspot": 0,
            "removedIntraDedupe": 0,
        },
        "removedFilterIds": set(),
        "removedDomainIds": set(),
        "removedHubspotIds": set(),
        "removedHubspotDetailById": {},
        "removedIntraDedupeIds": set(),
        "qualifiedIds": set(),
        "removedFilterReasonById": {},
        "removedDomainReasonById": {},
        "removedIntraDedupeReasonById": {},
        "pauseRequested": False,
        "finishOnPause": False,
        "pausedAt": None,
        "warnings": [],
        "domainResults": {
            "checked": 0,
            "homepageChecked": 0,
            "dead": [],
            "cdnReference": get_cdn_reference_data(),
        },
        "runConfig": workspace_config,
        "startedAt": now,
        "finishedAt": None,
        "error": "",
        "result": None,
    }
    session["updatedAt"] = now
    _persist_session(sessionId, session)

    asyncio.create_task(
        _run_session_qualification_job(
            session_id=sessionId,
            run_id=run_id,
            parsed_rules=parsed_rules,
            domain_check=domain_check,
            homepage_check=homepage_check,
            domain_field=domainField,
            website_keywords=website_keywords,
        website_exclude_keywords=website_exclude_keywords,
            exclude_country_tlds=exclude_country_tlds,
            disallowed_tlds=disallowed_tlds,
            allowed_tlds=allowed_tlds,
            intra_dedupe_enabled=intra_dedupe_enabled,
            intra_dedupe_columns=intra_dedupe_cols if intra_dedupe_cols else None,
            intra_dedupe_strategy=intra_dedupe_strategy,
            blocked_domain_suffixes=build_blocked_suffixes(blocklist_categories, custom_blocked) if blocklist_enabled else None,
            score_config=score_config,
        )
    )

    payload = _serialize_run_snapshot(session.get("activeRun"), include_result=False)
    payload["sessionId"] = sessionId
    payload["alreadyRunning"] = False
    return payload


@app.get("/api/session/qualify/progress")
async def session_qualify_progress(sessionId: str):
    """Read progress for the active async qualification run."""
    session = _touch_session(sessionId)
    run = session.get("activeRun")
    payload = _serialize_run_snapshot(run, include_result=True)
    payload["sessionId"] = sessionId
    return payload


@app.post("/api/session/qualify/pause")
async def session_qualify_pause(
    sessionId: str = Form(...),
    finishUnprocessed: str = Form("false"),
):
    """Pause a running qualification. Optionally finish by auto-disqualifying unprocessed rows."""
    session = _touch_session(sessionId)
    run = session.get("activeRun")
    if not isinstance(run, dict):
        raise HTTPException(status_code=400, detail="No active qualification run.")

    finish_unprocessed = _parse_form_bool(finishUnprocessed)
    status = str(run.get("status") or "")
    if status in {"done", "error"}:
        payload = _serialize_run_snapshot(run, include_result=True)
        payload["sessionId"] = sessionId
        return payload

    if status == "paused":
        if finish_unprocessed:
            await _finalize_paused_run(
                session_id=sessionId,
                session=session,
                run=run,
                auto_disqualify_unprocessed=True,
            )
        payload = _serialize_run_snapshot(session.get("activeRun"), include_result=True)
        payload["sessionId"] = sessionId
        return payload

    run["pauseRequested"] = True
    run["finishOnPause"] = bool(finish_unprocessed)
    run["status"] = "pausing"
    run["stage"] = "pausing"
    run["message"] = (
        "Pause requested. Finishing current checks and auto-disqualifying unprocessed rows..."
        if finish_unprocessed
        else "Pause requested. Finishing current checks..."
    )
    session["updatedAt"] = time.time()
    _persist_session(sessionId, session)

    payload = _serialize_run_snapshot(run, include_result=False)
    payload["sessionId"] = sessionId
    return payload


@app.post("/api/session/qualify/finish")
async def session_qualify_finish(sessionId: str = Form(...)):
    """Finish a paused run and auto-disqualify unprocessed rows."""
    session = _touch_session(sessionId)
    run = session.get("activeRun")
    if not isinstance(run, dict):
        raise HTTPException(status_code=400, detail="No active qualification run.")

    status = str(run.get("status") or "")
    if status in {"running", "pausing"}:
        run["pauseRequested"] = True
        run["finishOnPause"] = True
        run["status"] = "pausing"
        run["stage"] = "pausing"
        run["message"] = "Finish requested. Completing current checks before finalizing..."
        session["updatedAt"] = time.time()
        _persist_session(sessionId, session)
        payload = _serialize_run_snapshot(run, include_result=False)
        payload["sessionId"] = sessionId
        return payload

    if status == "paused":
        await _finalize_paused_run(
            session_id=sessionId,
            session=session,
            run=run,
            auto_disqualify_unprocessed=True,
        )
        payload = _serialize_run_snapshot(session.get("activeRun"), include_result=True)
        payload["sessionId"] = sessionId
        return payload

    payload = _serialize_run_snapshot(run, include_result=True)
    payload["sessionId"] = sessionId
    return payload


@app.post("/api/session/qualify/resume")
async def session_qualify_resume(sessionId: str = Form(...)):
    """Resume a paused run by starting a new run with stored config."""
    session = _touch_session(sessionId)
    active = session.get("activeRun")
    if isinstance(active, dict) and active.get("status") in {"running", "pausing"}:
        payload = _serialize_run_snapshot(active, include_result=False)
        payload["sessionId"] = sessionId
        payload["alreadyRunning"] = True
        return payload
    active_scrape = session.get("activeScrape")
    if isinstance(active_scrape, dict) and active_scrape.get("status") == "running":
        raise HTTPException(status_code=409, detail="A homepage scrape is currently running. Wait for it to finish before resuming qualification.")

    run_config = dict((active or {}).get("runConfig") or session.get("workspaceConfig") or {})
    parsed_rules = [item for item in (run_config.get("rules") or []) if isinstance(item, dict)]
    domain_check = bool(run_config.get("domainCheck"))
    homepage_check = bool(run_config.get("homepageCheck"))
    domain_field = str(run_config.get("domainField") or "")
    website_keywords = [str(v) for v in (run_config.get("websiteKeywords") or []) if str(v).strip()]
    website_exclude_keywords = [str(v) for v in (run_config.get("websiteExcludeKeywords") or []) if str(v).strip()]
    exclude_country_tlds = bool(run_config.get("excludeCountryTlds"))
    disallowed_tlds = set(str(v) for v in (run_config.get("disallowedTlds") or []) if str(v).strip())
    allowed_tlds = set(str(v) for v in (run_config.get("allowedTlds") or []) if str(v).strip())
    blocklist_enabled = bool(run_config.get("domainBlocklistEnabled"))
    blocklist_categories = run_config.get("domainBlocklistCategories") or {}
    custom_blocked = run_config.get("customBlockedDomains") or []
    df = _get_session_df(session)

    run_id = uuid4().hex
    now = time.time()
    session["activeRun"] = {
        "runId": run_id,
        "status": "running",
        "stage": "starting",
        "progress": 0.02,
        "message": "Resuming qualification run...",
        "processedRows": 0,
        "totalRows": int(df.height),
        "qualifiedCount": 0,
        "removedCount": 0,
        "removedBreakdown": {
            "removedFilter": 0,
            "removedDomain": 0,
            "removedHubspot": 0,
        },
        "removedFilterIds": set(),
        "removedDomainIds": set(),
        "removedHubspotIds": set(),
        "removedHubspotDetailById": {},
        "qualifiedIds": set(),
        "removedFilterReasonById": {},
        "removedDomainReasonById": {},
        "pauseRequested": False,
        "finishOnPause": False,
        "pausedAt": None,
        "warnings": [],
        "domainResults": {
            "checked": 0,
            "homepageChecked": 0,
            "dead": [],
            "cdnReference": get_cdn_reference_data(),
        },
        "runConfig": {
            "rules": parsed_rules,
            "domainCheck": domain_check,
            "homepageCheck": homepage_check,
            "domainField": domain_field,
            "websiteKeywords": website_keywords,
        "websiteExcludeKeywords": website_exclude_keywords,
            "excludeCountryTlds": exclude_country_tlds,
            "disallowedTlds": sorted(disallowed_tlds),
            "allowedTlds": sorted(allowed_tlds),
            "domainBlocklistEnabled": blocklist_enabled,
            "domainBlocklistCategories": blocklist_categories,
            "customBlockedDomains": custom_blocked,
        },
        "startedAt": now,
        "finishedAt": None,
        "error": "",
        "result": None,
    }
    session["updatedAt"] = now
    _persist_session(sessionId, session)

    asyncio.create_task(
        _run_session_qualification_job(
            session_id=sessionId,
            run_id=run_id,
            parsed_rules=parsed_rules,
            domain_check=domain_check,
            homepage_check=homepage_check,
            domain_field=domain_field,
            website_keywords=website_keywords,
        website_exclude_keywords=website_exclude_keywords,
            exclude_country_tlds=exclude_country_tlds,
            disallowed_tlds=disallowed_tlds,
            allowed_tlds=allowed_tlds,
            blocked_domain_suffixes=build_blocked_suffixes(blocklist_categories, custom_blocked) if blocklist_enabled else None,
        )
    )

    payload = _serialize_run_snapshot(session.get("activeRun"), include_result=False)
    payload["sessionId"] = sessionId
    payload["alreadyRunning"] = False
    return payload


@app.post("/api/session/qualify")
async def session_qualify(
    sessionId: str = Form(...),
    rules: str = Form("[]"),
    domainCheck: str = Form("false"),
    homepageCheck: str = Form("false"),
    domainField: str = Form(""),
    websiteKeywords: str = Form("[]"),
    websiteExcludeKeywords: str = Form("[]"),
    excludeCountryTlds: str = Form("false"),
    tldDisallowList: str = Form("[]"),
    tldAllowList: str = Form("[]"),
    domainBlocklistEnabled: str = Form("false"),
    domainBlocklistCategories: str = Form(""),
    customBlockedDomains: str = Form("[]"),
):
    """Run qualification against a stored session dataset."""
    try:
        start_time = time.perf_counter()
        session = _touch_session(sessionId)
        active_scrape = session.get("activeScrape")
        if isinstance(active_scrape, dict) and active_scrape.get("status") == "running":
            raise HTTPException(status_code=409, detail="A homepage scrape is currently running. Wait for it to finish before qualification.")
        df = _get_session_df(session)
        dedupe_df = _get_session_dedupe_df(session)
        parsed_rules = _parse_rules_payload(rules)
        domain_check = _parse_form_bool(domainCheck)
        homepage_check = _parse_form_bool(homepageCheck)
        website_keywords = _parse_website_keywords_payload(websiteKeywords)
        website_exclude_keywords = _parse_website_keywords_payload(websiteExcludeKeywords)
        exclude_country_tlds = _parse_form_bool(excludeCountryTlds)
        disallowed_tlds = _parse_tld_list_payload(tldDisallowList)
        allowed_tlds = _parse_tld_list_payload(tldAllowList)
        blocklist_enabled = _parse_form_bool(domainBlocklistEnabled)
        blocklist_categories = _parse_blocklist_categories(domainBlocklistCategories)
        custom_blocked = _parse_custom_blocked_domains(customBlockedDomains)
        session["workspaceConfig"] = {
            "rules": parsed_rules,
            "domainCheck": domain_check,
            "homepageCheck": homepage_check,
            "domainField": domainField,
            "websiteKeywords": website_keywords,
        "websiteExcludeKeywords": website_exclude_keywords,
            "excludeCountryTlds": exclude_country_tlds,
            "disallowedTlds": sorted(disallowed_tlds),
            "allowedTlds": sorted(allowed_tlds),
            "domainBlocklistEnabled": blocklist_enabled,
            "domainBlocklistCategories": blocklist_categories,
            "customBlockedDomains": custom_blocked,
        }

        pipeline = await run_qualification_pipeline(
            df=df,
            parsed_rules=parsed_rules,
            domain_check=domain_check,
            homepage_check=homepage_check,
            domain_field=domainField,
            website_keywords=website_keywords,
        website_exclude_keywords=website_exclude_keywords,
            exclude_country_tlds=exclude_country_tlds,
            disallowed_tlds=disallowed_tlds,
            allowed_tlds=allowed_tlds,
            dedupe_raw=session.get("dedupeRaw"),
            dedupe_df=dedupe_df,
            blocked_domain_suffixes=build_blocked_suffixes(blocklist_categories, custom_blocked) if blocklist_enabled else None,
        )
        processing_ms = int((time.perf_counter() - start_time) * 1000)

        result = {
            "sessionId": sessionId,
            "totalRows": df.height,
            "qualifiedCount": pipeline["qualifiedCount"],
            "removedCount": pipeline["removedCount"],
            "removedBreakdown": pipeline["removedBreakdown"],
            "rows": pipeline["rows"],
            "leads": pipeline["leads"],
            "columns": pipeline["columns"],
            "domainResults": pipeline["domainResults"],
            "meta": {
                "processingMs": processing_ms,
                "domainCheckEnabled": domain_check,
                "homepageCheckEnabled": homepage_check,
                "websiteKeywords": website_keywords,
        "websiteExcludeKeywords": website_exclude_keywords,
                "websiteExcludeKeywords": website_exclude_keywords,
                "tldFilter": {
                    "excludeCountryTlds": exclude_country_tlds,
                    "disallowList": sorted(disallowed_tlds),
                    "allowList": sorted(allowed_tlds),
                },
                "dedupe": pipeline["dedupeInfo"],
                "warnings": pipeline["warnings"],
            },
        }

        status_sets = _build_status_sets_from_rows(pipeline["rows"])
        session["lastRunResult"] = result
        session["lastRunStatus"] = status_sets
        session["activeRun"] = {
            "runId": uuid4().hex,
            "status": "done",
            "stage": "complete",
            "progress": 1.0,
            "message": "Qualification complete.",
            "processedRows": int(df.height),
            "totalRows": int(df.height),
            "qualifiedCount": int(result["qualifiedCount"]),
            "removedCount": int(result["removedCount"]),
            "removedBreakdown": result["removedBreakdown"],
            "pauseRequested": False,
            "finishOnPause": False,
            "pausedAt": None,
            "startedAt": time.time(),
            "finishedAt": time.time(),
            "error": "",
            "runConfig": dict(session.get("workspaceConfig") or {}),
            "result": result,
            **status_sets,
        }
        session["updatedAt"] = time.time()
        _persist_session(sessionId, session)

        return result
    except HTTPException:
        raise
    except Exception as exc:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Qualification failed: {type(exc).__name__}: {exc}") from exc


@app.post("/api/session/export")
async def session_export(
    sessionId: str = Form(...),
    rules: str = Form("[]"),
    domainCheck: str = Form("false"),
    homepageCheck: str = Form("false"),
    domainField: str = Form(""),
    websiteKeywords: str = Form("[]"),
    websiteExcludeKeywords: str = Form("[]"),
    excludeCountryTlds: str = Form("false"),
    tldDisallowList: str = Form("[]"),
    tldAllowList: str = Form("[]"),
    exportColumns: str = Form(""),
    fileName: str = Form("qualified_leads.csv"),
    intraDedupe: str = Form("false"),
    intraDedupeColumns: str = Form("[]"),
    intraDedupeStrategy: str = Form("first"),
    domainBlocklistEnabled: str = Form("false"),
    domainBlocklistCategories: str = Form(""),
    customBlockedDomains: str = Form("[]"),
    scoreEnabled: str = Form("false"),
    scoreWeights: str = Form("{}"),
    scoreDateField: str = Form(""),
    scoreHighSignalConfig: str = Form("{}"),
):
    """Export CSV from a stored session and current qualification config."""
    session = _touch_session(sessionId)
    df = _get_session_df(session)
    dedupe_df = _get_session_dedupe_df(session)
    parsed_rules = _parse_rules_payload(rules)
    exclude_country_tlds = _parse_form_bool(excludeCountryTlds)
    disallowed_tlds = _parse_tld_list_payload(tldDisallowList)
    allowed_tlds = _parse_tld_list_payload(tldAllowList)
    website_keywords = _parse_website_keywords_payload(websiteKeywords)
    website_exclude_keywords = _parse_website_keywords_payload(websiteExcludeKeywords)
    intra_dedupe_enabled = _parse_form_bool(intraDedupe)
    blocklist_enabled = _parse_form_bool(domainBlocklistEnabled)
    blocklist_categories = _parse_blocklist_categories(domainBlocklistCategories)
    custom_blocked = _parse_custom_blocked_domains(customBlockedDomains)
    try:
        intra_dedupe_cols = json.loads(intraDedupeColumns or "[]")
    except (json.JSONDecodeError, TypeError):
        intra_dedupe_cols = []
    intra_dedupe_strategy = intraDedupeStrategy if intraDedupeStrategy in ("first", "last", "merge") else "first"
    score_enabled = _parse_form_bool(scoreEnabled)
    try:
        parsed_score_weights = json.loads(scoreWeights) if scoreWeights else {}
    except (json.JSONDecodeError, TypeError):
        parsed_score_weights = {}
    try:
        parsed_score_signal = json.loads(scoreHighSignalConfig) if scoreHighSignalConfig else {}
    except (json.JSONDecodeError, TypeError):
        parsed_score_signal = {}
    score_config = {
        "scoreEnabled": score_enabled,
        "scoreWeights": parsed_score_weights,
        "scoreDateField": scoreDateField,
        "scoreHighSignalConfig": parsed_score_signal,
    } if score_enabled else None
    raw_export_columns = str(exportColumns or "").strip()
    selected_export_columns = _parse_export_columns_payload(exportColumns)
    if raw_export_columns and not selected_export_columns:
        raise HTTPException(status_code=400, detail="Select at least one export column.")
    pipeline = await run_qualification_pipeline(
        df=df,
        parsed_rules=parsed_rules,
        domain_check=_parse_form_bool(domainCheck),
        homepage_check=_parse_form_bool(homepageCheck),
        domain_field=domainField,
        website_keywords=website_keywords,
        website_exclude_keywords=website_exclude_keywords,
        exclude_country_tlds=exclude_country_tlds,
        disallowed_tlds=disallowed_tlds,
        allowed_tlds=allowed_tlds,
        dedupe_raw=session.get("dedupeRaw"),
        dedupe_df=dedupe_df,
        include_rows=False,
        include_leads=False,
        include_dataframe=True,
        intra_dedupe_enabled=intra_dedupe_enabled,
        intra_dedupe_columns=intra_dedupe_cols if intra_dedupe_cols else None,
        intra_dedupe_strategy=intra_dedupe_strategy,
        blocked_domain_suffixes=build_blocked_suffixes(blocklist_categories, custom_blocked) if blocklist_enabled else None,
        score_config=score_config,
    )
    out_df = pipeline["qualifiedDf"] if isinstance(pipeline.get("qualifiedDf"), pl.DataFrame) else df.head(0)
    if selected_export_columns:
        out_df = _apply_export_column_selection(out_df, selected_export_columns)
        if out_df.width == 0:
            raise HTTPException(status_code=400, detail="None of the selected export columns are available.")
    buf = io.BytesIO()
    out_df.write_csv(buf)
    buf.seek(0)
    safe_name = (fileName or "qualified_leads.csv").strip() or "qualified_leads.csv"
    if not safe_name.lower().endswith(".csv"):
        safe_name = f"{safe_name}.csv"
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={safe_name}"},
    )


# Apollo/enrichment column mapping
ENRICHMENT_COLUMN_MAP = {
    "domain": "Website URL",
    "website": "Website URL",
    "url": "Website URL",
    "company": "Organization Name",
    "company name": "Organization Name",
    "company_name": "Organization Name",
    "name": "Organization Name",
    "industry": "Industry",
    "employees": "# Employees",
    "employee_count": "# Employees",
    "headcount": "# Employees",
    "country": "Organization Country",
    "city": "Organization City",
    "state": "Organization State",
    "linkedin": "Organization Linkedin Url",
    "linkedin_url": "Organization Linkedin Url",
    "revenue": "Annual Revenue",
    "phone": "Organization Phone",
    "description": "Short Description",
}

def auto_map_enrichment_columns(df_columns: list[str]) -> dict[str, str]:
    """Auto-detect column mappings for enrichment export using name heuristics."""
    mapping = {}
    for col in df_columns:
        col_lower = col.lower().strip().replace(" ", "_")
        for hint, target in ENRICHMENT_COLUMN_MAP.items():
            if hint in col_lower and target not in mapping.values():
                mapping[col] = target
                break
    return mapping


@app.post("/api/session/export/enrichment")
async def session_export_enrichment(
    sessionId: str = Form(...),
    columnMapping: str = Form("{}"),
    fileName: str = Form("enrichment_export.csv"),
):
    """Export qualified leads with columns renamed for enrichment platforms."""
    session = _touch_session(sessionId)
    run = session.get("activeRun")
    if not run or run.get("status") != "done":
        raise HTTPException(status_code=400, detail="No completed qualification run. Run qualification first.")

    result = run.get("result") or {}
    qualified_df = result.get("qualifiedDf")
    if not isinstance(qualified_df, pl.DataFrame) or qualified_df.height == 0:
        raise HTTPException(status_code=400, detail="No qualified leads to export.")

    try:
        mapping = json.loads(columnMapping) if columnMapping else {}
    except (json.JSONDecodeError, TypeError):
        mapping = {}

    if not mapping:
        mapping = auto_map_enrichment_columns(qualified_df.columns)

    # Rename columns according to mapping
    rename_map = {src: tgt for src, tgt in mapping.items() if src in qualified_df.columns}
    out_df = qualified_df.rename(rename_map) if rename_map else qualified_df

    # Remove internal columns
    out_df = out_df.drop([c for c in out_df.columns if c.startswith("_")])

    buf = io.BytesIO()
    out_df.write_csv(buf)
    buf.seek(0)
    safe_name = (fileName or "enrichment_export.csv").strip() or "enrichment_export.csv"
    if not safe_name.lower().endswith(".csv"):
        safe_name = f"{safe_name}.csv"
    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={safe_name}"},
    )


@app.post("/api/preview")
async def preview_qualification(
    files: Optional[list[UploadFile]] = File(None),
    file: Optional[UploadFile] = File(None),
    rules: str = Form("[]"),
    domainCheck: str = Form("false"),
    homepageCheck: str = Form("false"),
    domainField: str = Form(""),
    websiteKeywords: str = Form("[]"),
    websiteExcludeKeywords: str = Form("[]"),
    excludeCountryTlds: str = Form("false"),
    tldDisallowList: str = Form("[]"),
    tldAllowList: str = Form("[]"),
    dedupeFiles: Optional[list[UploadFile]] = File(None),
    dedupeFile: Optional[UploadFile] = File(None),
    domainBlocklistEnabled: str = Form("false"),
    domainBlocklistCategories: str = Form(""),
    customBlockedDomains: str = Form("[]"),
):
    """
    Fast preview endpoint for configure-step feedback.
    Returns estimated counts using current rule set and optional domain check.
    """
    source_files = _resolve_upload_files(files, file)
    source_bundle = await _parse_upload_datasets(source_files, label="source")
    df = source_bundle["df"]
    parsed_rules = _parse_rules_payload(rules)
    exclude_country_tlds = _parse_form_bool(excludeCountryTlds)
    disallowed_tlds = _parse_tld_list_payload(tldDisallowList)
    allowed_tlds = _parse_tld_list_payload(tldAllowList)
    website_keywords = _parse_website_keywords_payload(websiteKeywords)
    website_exclude_keywords = _parse_website_keywords_payload(websiteExcludeKeywords)
    blocklist_enabled = _parse_form_bool(domainBlocklistEnabled)
    blocklist_categories = _parse_blocklist_categories(domainBlocklistCategories)
    custom_blocked = _parse_custom_blocked_domains(customBlockedDomains)
    dedupe_uploads = _resolve_upload_files(dedupeFiles, dedupeFile)
    dedupe_bundle = await _parse_upload_datasets(dedupe_uploads, label="dedupe") if dedupe_uploads else None
    dedupe_raw = dedupe_bundle["raws"][0] if dedupe_bundle and dedupe_bundle["raws"] else None
    dedupe_df = dedupe_bundle["df"] if dedupe_bundle else None
    pipeline = await run_qualification_pipeline(
        df=df,
        parsed_rules=parsed_rules,
        domain_check=_parse_form_bool(domainCheck),
        homepage_check=_parse_form_bool(homepageCheck),
        domain_field=domainField,
        website_keywords=website_keywords,
        website_exclude_keywords=website_exclude_keywords,
        exclude_country_tlds=exclude_country_tlds,
        disallowed_tlds=disallowed_tlds,
        allowed_tlds=allowed_tlds,
        dedupe_raw=dedupe_raw,
        dedupe_df=dedupe_df,
        include_rows=False,
        include_leads=False,
        skip_network_checks=True,
        blocked_domain_suffixes=build_blocked_suffixes(blocklist_categories, custom_blocked) if blocklist_enabled else None,
    )

    return {
        "fileName": _summarize_file_names(source_bundle["fileNames"], "dataset.csv"),
        "fileNames": source_bundle["fileNames"],
        "fileCount": len(source_bundle["fileNames"]),
        "estimatedQualifiedCount": pipeline["qualifiedCount"],
        "estimatedRemovedCount": pipeline["removedCount"],
        "totalRows": df.height,
        "sourceRows": source_bundle["inputRows"],
        "estimatedDuplicatesRemoved": pipeline["dedupeInfo"].get("removedCount", 0),
        "removedBreakdown": pipeline["removedBreakdown"],
    }


@app.post("/api/qualify")
async def qualify_leads(
    files: Optional[list[UploadFile]] = File(None),
    file: Optional[UploadFile] = File(None),
    rules: str = Form("[]"),
    domainCheck: str = Form("false"),
    homepageCheck: str = Form("false"),
    domainField: str = Form(""),
    websiteKeywords: str = Form("[]"),
    websiteExcludeKeywords: str = Form("[]"),
    excludeCountryTlds: str = Form("false"),
    tldDisallowList: str = Form("[]"),
    tldAllowList: str = Form("[]"),
    dedupeFiles: Optional[list[UploadFile]] = File(None),
    dedupeFile: Optional[UploadFile] = File(None),
    domainBlocklistEnabled: str = Form("false"),
    domainBlocklistCategories: str = Form(""),
    customBlockedDomains: str = Form("[]"),
):
    """Apply ICP rules, optionally verify domains, return qualified leads."""
    start_time = time.perf_counter()
    source_files = _resolve_upload_files(files, file)
    source_bundle = await _parse_upload_datasets(source_files, label="source")
    df = source_bundle["df"]
    parsed_rules = _parse_rules_payload(rules)
    domain_check = _parse_form_bool(domainCheck)
    homepage_check = _parse_form_bool(homepageCheck)
    website_keywords = _parse_website_keywords_payload(websiteKeywords)
    website_exclude_keywords = _parse_website_keywords_payload(websiteExcludeKeywords)
    exclude_country_tlds = _parse_form_bool(excludeCountryTlds)
    disallowed_tlds = _parse_tld_list_payload(tldDisallowList)
    allowed_tlds = _parse_tld_list_payload(tldAllowList)
    blocklist_enabled = _parse_form_bool(domainBlocklistEnabled)
    blocklist_categories = _parse_blocklist_categories(domainBlocklistCategories)
    custom_blocked = _parse_custom_blocked_domains(customBlockedDomains)
    dedupe_uploads = _resolve_upload_files(dedupeFiles, dedupeFile)
    dedupe_bundle = await _parse_upload_datasets(dedupe_uploads, label="dedupe") if dedupe_uploads else None
    dedupe_raw = dedupe_bundle["raws"][0] if dedupe_bundle and dedupe_bundle["raws"] else None
    dedupe_df = dedupe_bundle["df"] if dedupe_bundle else None

    pipeline = await run_qualification_pipeline(
        df=df,
        parsed_rules=parsed_rules,
        domain_check=domain_check,
        homepage_check=homepage_check,
        domain_field=domainField,
        website_keywords=website_keywords,
        website_exclude_keywords=website_exclude_keywords,
        exclude_country_tlds=exclude_country_tlds,
        disallowed_tlds=disallowed_tlds,
        allowed_tlds=allowed_tlds,
        dedupe_raw=dedupe_raw,
        dedupe_df=dedupe_df,
        blocked_domain_suffixes=build_blocked_suffixes(blocklist_categories, custom_blocked) if blocklist_enabled else None,
    )
    processing_ms = int((time.perf_counter() - start_time) * 1000)

    return {
        "fileName": _summarize_file_names(source_bundle["fileNames"], "dataset.csv"),
        "fileNames": source_bundle["fileNames"],
        "fileCount": len(source_bundle["fileNames"]),
        "totalRows": df.height,
        "sourceRows": source_bundle["inputRows"],
        "qualifiedCount": pipeline["qualifiedCount"],
        "removedCount": pipeline["removedCount"],
        "removedBreakdown": pipeline["removedBreakdown"],
        "leads": pipeline["leads"],
        "rows": pipeline["rows"],
        "columns": pipeline["columns"],
        "domainResults": pipeline["domainResults"],
        "meta": {
            "processingMs": processing_ms,
            "domainCheckEnabled": domain_check,
            "homepageCheckEnabled": homepage_check,
            "websiteKeywords": website_keywords,
        "websiteExcludeKeywords": website_exclude_keywords,
            "tldFilter": {
                "excludeCountryTlds": exclude_country_tlds,
                "disallowList": sorted(disallowed_tlds),
                "allowList": sorted(allowed_tlds),
            },
            "dedupe": pipeline["dedupeInfo"],
            "warnings": pipeline["warnings"],
        },
    }


@app.post("/api/download")
async def download_qualified(
    files: Optional[list[UploadFile]] = File(None),
    file: Optional[UploadFile] = File(None),
    rules: str = Form("[]"),
    domainCheck: str = Form("false"),
    homepageCheck: str = Form("false"),
    domainField: str = Form(""),
    websiteKeywords: str = Form("[]"),
    websiteExcludeKeywords: str = Form("[]"),
    excludeCountryTlds: str = Form("false"),
    tldDisallowList: str = Form("[]"),
    tldAllowList: str = Form("[]"),
    exportColumns: str = Form(""),
    dedupeFiles: Optional[list[UploadFile]] = File(None),
    dedupeFile: Optional[UploadFile] = File(None),
    domainBlocklistEnabled: str = Form("false"),
    domainBlocklistCategories: str = Form(""),
    customBlockedDomains: str = Form("[]"),
):
    """Apply ICP rules, optionally verify domains, return CSV download."""
    source_files = _resolve_upload_files(files, file)
    source_bundle = await _parse_upload_datasets(source_files, label="source")
    df = source_bundle["df"]
    parsed_rules = _parse_rules_payload(rules)
    exclude_country_tlds = _parse_form_bool(excludeCountryTlds)
    disallowed_tlds = _parse_tld_list_payload(tldDisallowList)
    allowed_tlds = _parse_tld_list_payload(tldAllowList)
    website_keywords = _parse_website_keywords_payload(websiteKeywords)
    website_exclude_keywords = _parse_website_keywords_payload(websiteExcludeKeywords)
    blocklist_enabled = _parse_form_bool(domainBlocklistEnabled)
    blocklist_categories = _parse_blocklist_categories(domainBlocklistCategories)
    custom_blocked = _parse_custom_blocked_domains(customBlockedDomains)
    raw_export_columns = str(exportColumns or "").strip()
    selected_export_columns = _parse_export_columns_payload(exportColumns)
    if raw_export_columns and not selected_export_columns:
        raise HTTPException(status_code=400, detail="Select at least one export column.")
    dedupe_uploads = _resolve_upload_files(dedupeFiles, dedupeFile)
    dedupe_bundle = await _parse_upload_datasets(dedupe_uploads, label="dedupe") if dedupe_uploads else None
    dedupe_raw = dedupe_bundle["raws"][0] if dedupe_bundle and dedupe_bundle["raws"] else None
    dedupe_df = dedupe_bundle["df"] if dedupe_bundle else None
    pipeline = await run_qualification_pipeline(
        df=df,
        parsed_rules=parsed_rules,
        domain_check=_parse_form_bool(domainCheck),
        homepage_check=_parse_form_bool(homepageCheck),
        domain_field=domainField,
        website_keywords=website_keywords,
        website_exclude_keywords=website_exclude_keywords,
        exclude_country_tlds=exclude_country_tlds,
        disallowed_tlds=disallowed_tlds,
        allowed_tlds=allowed_tlds,
        dedupe_raw=dedupe_raw,
        dedupe_df=dedupe_df,
        include_rows=False,
        include_leads=False,
        include_dataframe=True,
        blocked_domain_suffixes=build_blocked_suffixes(blocklist_categories, custom_blocked) if blocklist_enabled else None,
    )
    qualified = pipeline["qualifiedDf"] if isinstance(pipeline.get("qualifiedDf"), pl.DataFrame) else df.head(0)
    if selected_export_columns:
        qualified = _apply_export_column_selection(qualified, selected_export_columns)
        if qualified.width == 0:
            raise HTTPException(status_code=400, detail="None of the selected export columns are available.")

    buf = io.BytesIO()
    qualified.write_csv(buf)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=qualified_leads.csv"},
    )


# Domain cache management endpoints
@app.get("/api/cache/stats")
async def get_domain_cache_stats():
    """Get statistics about the domain validation cache."""
    await init_cache()
    stats = await get_cache_stats()
    return stats


@app.post("/api/cache/clear")
async def clear_domain_cache():
    """Clear all cached domain validation results."""
    await init_cache()
    await clear_all_cache()
    return {"status": "success", "message": "Domain cache cleared"}


@app.post("/api/cache/scrape/clear")
async def clear_scrape_cache_endpoint():
    """Clear all cached scrape enrichment results."""
    await init_cache()
    await clear_scrape_cache()
    return {"status": "success", "message": "Scrape cache cleared"}


@app.get("/api/cache/scrape/stats")
async def get_scrape_cache_stats_endpoint():
    """Get statistics about the scrape enrichment cache."""
    await init_cache()
    stats = await get_scrape_cache_stats()
    return stats


# Mount static files LAST so API routes take priority
app.mount("/fonts", StaticFiles(directory=str(FONTS_DIR)), name="fonts")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Hound Suite backend API server.")
    parser.add_argument("--host", default=os.getenv("HOUND_HOST", "127.0.0.1"), help="Bind host")
    parser.add_argument("--port", type=int, default=int(os.getenv("HOUND_PORT", "8000")), help="Bind port")
    parser.add_argument("--data-dir", default=str(_resolve_data_dir_from_env()), help="Writable data directory")
    parser.add_argument("--log-level", default=os.getenv("HOUND_LOG_LEVEL", "info"), help="uvicorn log level")
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    os.environ["HOUND_DATA_DIR"] = str(args.data_dir)
    _refresh_data_paths_from_env()
    import uvicorn

    uvicorn.run(
        app,
        host=str(args.host),
        port=int(args.port),
        log_level=str(args.log_level).lower(),
        reload=False,
        access_log=False,
    )


if __name__ == "__main__":
    main()
