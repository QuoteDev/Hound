/* ── Shared Utilities, Models & Constants ─────────────────── */

const API = window.location.origin;

const DatasetState = {
    EMPTY: 'empty',
    LOADED: 'loaded',
};

const DrawerState = {
    NONE: 'none',
    FILTERS: 'filters',
    VALIDATION: 'validation',
    EXPORT: 'export',
    ACTIVITY: 'activity',
    ROW_INSPECTOR: 'row_inspector',
};

const VIEW_FILTER_OPS = [
    { value: 'contains', label: 'contains' },
    { value: 'equals', label: 'equals' },
    { value: 'not_equals', label: 'not equals' },
    { value: 'is_empty', label: 'is empty' },
    { value: 'is_not_empty', label: 'is not empty' },
    { value: 'before', label: 'is before' },
    { value: 'after', label: 'is after' },
];

const VIEW_FILTER_OP_LABEL = VIEW_FILTER_OPS.reduce((acc, op) => {
    acc[op.value] = op.label;
    return acc;
}, {});
const FILTER_PRESETS_STORAGE_KEY = 'hound_filter_validation_presets_v1';
const RECENT_RUNS_STORAGE_KEY = 'hound_recent_runs_v1';
const WORKSPACE_RESTORE_STORAGE_KEY = 'hound_workspace_restore_v1';
const MAX_RECENT_RUNS = 40;

function TableViewState(raw = {}) {
    return {
        search: raw.search || '',
        statusFilter: raw.statusFilter || 'all',
        sort: raw.sort || { column: null, direction: null },
        page: raw.page || 1,
        pageSize: raw.pageSize || 100,
        visibleColumns: raw.visibleColumns || null,
    };
}

function HeaderActionState(raw = {}) {
    return {
        canRun: !!raw.canRun,
        isUnsaved: !!raw.isUnsaved,
        isRunning: !!raw.isRunning,
        runMessage: raw.runMessage || '',
    };
}

const RowStatus = {
    PROCESSING: 'processing',
    QUALIFIED: 'qualified',
    REMOVED_FILTER: 'removed_filter',
    REMOVED_DOMAIN: 'removed_domain',
    REMOVED_HUBSPOT: 'removed_hubspot',
    REMOVED_INTRA_DEDUPE: 'removed_intra_dedupe',
    REMOVED_MANUAL: 'removed_manual',
};

/* ── Data Model Factories ─────────────────────────────────── */
function RuleGroup(tags) {
    return { id: Date.now() + Math.random(), tags: tags || [], logic: 'and' };
}

function ViewFilter(raw = {}) {
    return {
        id: raw.id || (Date.now() + Math.random()),
        field: raw.field || '',
        op: raw.op || 'contains',
        value: raw.value || '',
        value2: raw.value2 || '',
    };
}

function Rule(field, cols) {
    const col = cols ? cols.find(c => c.name === field) : null;
    const mt = field ? guessCondition(field, col) : 'contains';
    return {
        id: Date.now() + Math.random(),
        field: field || '',
        matchType: mt,
        groups: [RuleGroup()],
        groupsLogic: 'or',
        threshold: 80,
        min: '',
        max: '',
        startDate: '',
        endDate: '',
        includeBlankValues: false,
        separator: ';',
    };
}

function ColumnProfile(raw) {
    return {
        name: raw.name || '',
        dataType: raw.dataType || '',
        inferredType: raw.inferredType || 'text',
        nullRate: typeof raw.nullRate === 'number' ? raw.nullRate : 0,
        uniqueCount: raw.uniqueCount || 0,
        sampleValues: raw.sampleValues || [],
    };
}

function QualificationMeta(raw) {
    return {
        processingMs: raw?.processingMs || 0,
        domainCheckEnabled: !!raw?.domainCheckEnabled,
        homepageCheckEnabled: !!raw?.homepageCheckEnabled,
        websiteKeywords: Array.isArray(raw?.websiteKeywords) ? raw.websiteKeywords : [],
        websiteExcludeKeywords: Array.isArray(raw?.websiteExcludeKeywords) ? raw.websiteExcludeKeywords : [],
        tldFilter: raw?.tldFilter || {
            excludeCountryTlds: false,
            disallowList: [],
            allowList: [],
        },
        dedupe: raw?.dedupe || {
            enabled: false,
            removedCount: 0,
            checkedCount: 0,
            candidateColumn: null,
            hubspotColumn: null,
            keyType: null,
            matches: [],
            warnings: [],
        },
        warnings: Array.isArray(raw?.warnings) ? raw.warnings : [],
    };
}

function WorkspaceSession(raw) {
    const sourceFileNames = Array.isArray(raw?.fileNames)
        ? raw.fileNames
        : (raw?.fileName ? [raw.fileName] : []);
    const dedupeRaw = raw?.dedupe || {};
    const dedupeFileNames = Array.isArray(dedupeRaw.fileNames)
        ? dedupeRaw.fileNames
        : (dedupeRaw.fileName ? [dedupeRaw.fileName] : []);
    return {
        sessionId: raw?.sessionId || '',
        fileName: raw?.fileName || '',
        fileNames: sourceFileNames,
        fileCount: raw?.fileCount || sourceFileNames.length,
        columns: raw?.columns || [],
        columnProfiles: (raw?.columnProfiles || []).map(ColumnProfile),
        previewRows: raw?.previewRows || [],
        totalRows: raw?.totalRows || 0,
        anomalies: raw?.anomalies || { emptyHeavyColumns: [], duplicateHeavyColumns: [] },
        sourceRows: raw?.sourceRows || (raw?.totalRows || 0),
        sourceMappings: raw?.sourceMappings || [],
        dedupe: {
            ...dedupeRaw,
            enabled: !!dedupeRaw.enabled,
            fileName: dedupeRaw.fileName || null,
            fileNames: dedupeFileNames,
            fileCount: dedupeRaw.fileCount || dedupeFileNames.length,
        },
    };
}

function RowReason(code) {
    const decodeHomepageReason = (token) => {
        const value = String(token || '').trim().toLowerCase();
        if (!value) return '';
        if (value === 'html_lang_not_en') return 'homepage language is non-English';
        if (value === 'non_usd_currency_without_usd') return 'non-USD currency with weak US signals';
        if (value === 'website_keywords_no_match') return 'no homepage keyword match';
        if (value === 'limited_b2b_signals') return 'limited B2B evidence';
        if (value.startsWith('consumer_signal_')) {
            return `consumer/ecommerce signal (${value.replace('consumer_signal_', '').replaceAll('_', ' ')})`;
        }
        if (value.startsWith('exclude_keyword_')) {
            return `matched exclude keyword (${value.replace('exclude_keyword_', '').replaceAll('_', ' ')})`;
        }
        return value.replaceAll('_', ' ');
    };

    const map = {
        qualified_passed_all_checks: 'Passed all checks',
        rule_filter_mismatch: 'Did not match filter rules',
        hubspot_duplicate_match: 'Found duplicate in attached dedupe files',
        intra_dedupe_duplicate: 'Removed as intra-dataset duplicate',
        manual_exclusion: 'Manually excluded by user',
        preview_only: 'Preview row (qualification not run yet)',
        qualification_in_progress: 'Qualification currently running for this row.',
        qualification_paused_pending: 'Qualification paused before this row was processed.',
        paused_unprocessed: 'Qualification finished from paused state; row was auto-disqualified.',
    };
    if (!code) return 'No reason available';
    if (map[code]) return map[code];
    if (code.startsWith('blocked_domain_')) {
        const cat = code.replace('blocked_domain_', '');
        const catLabel = BLOCKED_DOMAIN_CATEGORY_LABELS[cat] || cat;
        return `Blocked non-company domain (${catLabel})`;
    }
    if (code.startsWith('domain_')) {
        const detail = code.replace('domain_', '').toLowerCase();
        if (detail.startsWith('inconclusive_fetch_failed')) {
            return 'Homepage fetch inconclusive (not disqualified)';
        }
        if (detail.startsWith('disallowed_tld_')) {
            const tld = `.${detail.replace('disallowed_tld_', '').replaceAll('_', '.')}`;
            return `Domain disqualified due to blocked TLD (${tld})`;
        }
        if (detail.startsWith('non_us_country')) {
            return 'Domain disqualified as non-US location';
        }
        if (detail.startsWith('dns_timeout')) {
            return 'Domain disqualified due to DNS timeout';
        }
        if (detail.startsWith('dns_unresolved') || detail.startsWith('nxdomain') || detail.startsWith('no_a_record')) {
            return 'Domain disqualified because DNS did not resolve';
        }
        if (detail.startsWith('disqualified_missing_homepage_signals')) {
            return 'Homepage signals missing or insufficient';
        }
        if (detail.startsWith('disqualified_soft_strikes_')) {
            const match = detail.match(/^disqualified_soft_strikes_(\d+)_?(.*)$/);
            const strikeCount = match?.[1] || '?';
            const tail = match?.[2] || '';
            const decoded = tail
                .split(',')
                .map(decodeHomepageReason)
                .filter(Boolean);
            if (decoded.length > 0) {
                return `Homepage disqualified after ${strikeCount} soft-signal strikes (${decoded.join('; ')})`;
            }
            return `Homepage disqualified after ${strikeCount} soft-signal strikes`;
        }
        if (detail.startsWith('disqualified')) {
            return `Homepage disqualified (${detail.replace('disqualified_', '').replaceAll('_', ' ').replaceAll(',', ', ')})`;
        }
        return `Domain check failed (${detail.replaceAll('_', ' ')})`;
    }
    return code.replaceAll('_', ' ');
}

function RowStatusLabel(status) {
    const map = {
        processing: 'Needs review',
        qualified: 'Qualified',
        removed_filter: 'Excluded',
        removed_domain: 'Excluded',
        removed_hubspot: 'Excluded',
        removed_intra_dedupe: 'Excluded',
        removed_manual: 'Excluded',
        error: 'Error',
    };
    return map[status] || String(status || '').replaceAll('_', ' ');
}

function RunSummary(raw) {
    return {
        sessionId: raw?.sessionId || '',
        totalRows: raw?.totalRows || 0,
        qualifiedCount: raw?.qualifiedCount || 0,
        removedCount: raw?.removedCount || 0,
        removedBreakdown: raw?.removedBreakdown || {
            removedFilter: 0,
            removedDomain: 0,
            removedHubspot: 0,
            removedIntraDedupe: 0,
            removedBlocklist: 0,
        },
        rows: raw?.rows || [],
        leads: raw?.leads || [],
        columns: raw?.columns || [],
        domainResults: raw?.domainResults || { checked: 0, dead: [] },
        meta: QualificationMeta(raw?.meta),
    };
}

/* ── Field Type Badge Component ───────────────────────────── */
function FieldType({ name, inferredType }) {
    const forced = (inferredType || '').toLowerCase();
    if (forced === 'link') return <span className="ft ft-link"><I.link /></span>;
    if (forced === 'date') return <span className="ft ft-date">D</span>;
    if (forced === 'email') return <span className="ft ft-mail">@</span>;
    if (forced === 'number') return <span className="ft ft-num"><I.hash /></span>;
    if (forced === 'boolean') return <span className="ft ft-bool">B</span>;

    const n = (name || '').toLowerCase();
    if (n.includes('url') || n.includes('website') || n.includes('domain') || n.includes('link')) {
        return <span className="ft ft-link"><I.link /></span>;
    }
    if (n.includes('date') || n.includes('time') || n.includes('created')) {
        return <span className="ft ft-date">D</span>;
    }
    if (n.includes('email') || n.includes('mail')) {
        return <span className="ft ft-mail">@</span>;
    }
    if (n.includes('phone') || n.includes('tel')) {
        return <span className="ft ft-phone">#</span>;
    }
    if (n.includes('count') || n.includes('amount') || n.includes('size') || n.includes('num') || n.includes('revenue') || n.includes('funding')) {
        return <span className="ft ft-num"><I.hash /></span>;
    }
    return <span className="ft ft-text">T</span>;
}

/* ── Match Type Options ───────────────────────────────────── */
const MATCH_TYPES = [
    { value: 'contains', label: 'Contains' },
    { value: 'not_contains', label: 'Does not contain' },
    { value: 'exact', label: 'Exact match' },
    { value: 'not_exact', label: 'Not exact' },
    { value: 'fuzzy', label: 'Fuzzy match' },
    { value: 'range', label: 'Numeric range' },
    { value: 'dates', label: 'Date range' },
    { value: 'excludes', label: 'Excludes (fuzzy)' },
    { value: 'multivalue_any', label: 'Contains any value' },
    { value: 'multivalue_all', label: 'Contains all values' },
    { value: 'multivalue_exclude', label: 'Excludes value' },
    { value: 'geo_country', label: 'Country filter' },
];

/* ── Quick Filter Presets ─────────────────────────────────── */
const QUICK_FILTER_PRESETS = [
    {
        id: 'enterprise_exclude',
        name: 'Enterprise Exclusion',
        description: 'Exclude companies using enterprise-grade tools',
        matchType: 'multivalue_exclude',
        tags: ['Marketo', 'Pardot', 'Eloqua', 'Salesforce', 'Adobe Analytics', 'Adobe Experience Platform', 'Oracle', 'SAP'],
    },
    {
        id: 'b2b_signals',
        name: 'B2B Signals',
        description: 'Keep companies using popular B2B SaaS tools',
        matchType: 'multivalue_any',
        tags: ['Stripe', 'Segment', 'Amplitude', 'Intercom', 'Calendly', 'HubSpot', 'Zendesk'],
    },
    {
        id: 'dev_tools',
        name: 'Developer Tools',
        description: 'Companies with developer documentation tools',
        matchType: 'multivalue_any',
        tags: ['Mintlify', 'GitBook', 'ReadMe', 'Swagger', 'Postman'],
    },
];

const COUNTRY_PRESETS = {
    'United States': ['US', 'United States', 'USA'],
    'US + Canada': ['US', 'CA', 'United States', 'Canada', 'USA'],
    'English-speaking': ['US', 'CA', 'GB', 'AU', 'NZ', 'IE', 'United States', 'Canada', 'United Kingdom', 'Australia', 'New Zealand', 'Ireland'],
    'EU': ['DE', 'FR', 'ES', 'IT', 'NL', 'SE', 'NO', 'DK', 'FI', 'BE', 'PT', 'PL', 'CZ', 'AT', 'IE', 'Germany', 'France', 'Spain', 'Italy', 'Netherlands', 'Sweden'],
};

/* ── Domain Column Guessing ───────────────────────────────── */
const DOMAIN_HINTS = ['website', 'domain', 'url', 'site', 'web', 'homepage', 'link'];
const DEFAULT_TLD_ALLOWLIST = ['.com', '.io', '.ai', '.dev', '.co', '.org', '.net', '.app', '.tech', '.so', '.gg'];

function guessDomainColumn(cols) {
    for (const h of DOMAIN_HINTS) {
        const c = cols.find(c => c.name.toLowerCase().includes(h));
        if (c) return c.name;
    }
    return '';
}

/* ── Smart Condition Auto-Detection ───────────────────────── */
function guessCondition(colName, col) {
    const n = (colName || '').toLowerCase();
    if (col && col.isMultiValue) return 'multivalue_any';
    if (n.includes('country') || n.includes('nation') || n.includes('geo') || n.includes('region') || n.includes('location')) return 'geo_country';
    if (n.includes('date') || n.includes('time') || n.includes('created') || n.includes('updated') ||
        n.includes('founded') || n.includes('launched') || n.includes('timestamp')) return 'dates';
    if (n.includes('revenue') || n.includes('funding') || n.includes('amount') ||
        n.includes('size') || n.includes('count') || n.includes('employee') ||
        n.includes('headcount') || n.includes('salary') || n.includes('price') ||
        n.includes('age')) return 'range';
    if (col) {
        const samples = col.sampleValues || [];
        const dateLike = samples.filter(v => !Number.isNaN(Date.parse(String(v || '').trim())));
        if (samples.length > 0 && dateLike.length > samples.length * 0.6) return 'dates';
        const nums = (col.sampleValues || []).filter(v => !isNaN(parseFloat(String(v).replace(/[,$%]/g, ''))));
        if (nums.length > (col.sampleValues || []).length * 0.6) return 'range';
    }
    if (n.includes('email') || n.includes('mail') || n.includes('url') ||
        n.includes('website') || n.includes('domain') || n.includes('link')) return 'exact';
    if (col && col.uniqueCount && col.uniqueCount <= 20) return 'contains';
    return 'fuzzy';
}

/* ── API Helpers ──────────────────────────────────────────── */
async function parseApiError(response) {
    const rawBody = await response.text();
    let detail = rawBody || '';

    if (rawBody) {
        try {
            const payload = JSON.parse(rawBody);
            detail = payload?.detail || payload?.message || JSON.stringify(payload);
        } catch (e) {
            detail = rawBody;
        }
    }

    const msg = detail && detail.length < 400 ? detail : `Request failed with status ${response.status}`;
    return new Error(msg);
}

async function requestJSON(url, options) {
    const res = await fetch(url, options);
    if (!res.ok) throw await parseApiError(res);
    return res.json();
}

async function requestBlob(url, options) {
    const res = await fetch(url, options);
    if (!res.ok) throw await parseApiError(res);
    return res.blob();
}

function isCsvFile(file) {
    const lower = (file?.name || '').toLowerCase();
    return lower.endsWith('.csv') || lower.endsWith('.tsv') || lower.endsWith('.txt');
}

function toReadableFileSize(bytes) {
    if (!bytes || bytes <= 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB'];
    let size = bytes;
    let unit = 0;
    while (size >= 1024 && unit < units.length - 1) {
        size /= 1024;
        unit += 1;
    }
    return `${size.toFixed(size >= 10 || unit === 0 ? 0 : 1)} ${units[unit]}`;
}

function normalizeTldToken(raw) {
    let token = String(raw || '').trim().toLowerCase();
    if (!token) return '';
    token = token.replace(/^\*\./, '').replace(/^\.+/, '').replace(/\.+$/, '');
    token = token.replace(/[^a-z0-9.-]/g, '');
    if (!token) return '';
    return `.${token}`;
}

function normalizeTldList(values) {
    if (!Array.isArray(values)) return [];
    const out = [];
    const seen = new Set();
    values.forEach(value => {
        const token = normalizeTldToken(value);
        if (!token || seen.has(token)) return;
        seen.add(token);
        out.push(token);
    });
    return out;
}

function parseTldListInput(raw) {
    const parts = String(raw || '').split(/[\n,]+/g);
    return normalizeTldList(parts);
}

function formatTldListInput(values) {
    return normalizeTldList(values).join(', ');
}

function normalizeKeywordToken(raw) {
    const token = String(raw || '').trim().toLowerCase().replace(/\s+/g, ' ');
    return token;
}

function normalizeKeywordList(values) {
    if (!Array.isArray(values)) return [];
    const out = [];
    const seen = new Set();
    values.forEach(value => {
        const token = normalizeKeywordToken(value);
        if (!token || seen.has(token)) return;
        seen.add(token);
        out.push(token);
    });
    return out;
}

function parseKeywordListInput(raw) {
    const parts = String(raw || '').split(/[\n,]+/g);
    return normalizeKeywordList(parts);
}

function formatKeywordListInput(values) {
    return normalizeKeywordList(values).join(', ');
}

function ruleSignature(
    rules,
    domChk,
    homepageChk,
    domField,
    websiteKeywords,
    dedupeSig,
    tldCountryChk,
    tldDisallow,
    tldAllow,
    intraDedupe,
    intraDedupeCol,
    intraDedupeStrategy,
    websiteExcludeKeywords,
    domainBlocklistEnabled,
    domainBlocklistCategories,
    customBlockedDomains,
    scoreEnabled,
    scoreWeights,
    scoreDateField,
    scoreHighSignalConfig
) {
    return JSON.stringify({
        rules,
        domChk,
        homepageChk: !!homepageChk,
        domField,
        websiteKeywords: normalizeKeywordList(websiteKeywords),
        websiteExcludeKeywords: normalizeKeywordList(websiteExcludeKeywords),
        dedupeSig,
        tldCountryChk: !!tldCountryChk,
        tldDisallow: normalizeTldList(tldDisallow),
        tldAllow: normalizeTldList(tldAllow),
        intraDedupe: !!intraDedupe,
        intraDedupeCol: intraDedupeCol || '',
        intraDedupeStrategy: intraDedupeStrategy || 'first',
        domainBlocklistEnabled: !!domainBlocklistEnabled,
        domainBlocklistCategories: domainBlocklistCategories || {},
        customBlockedDomains: customBlockedDomains || [],
        scoreEnabled: !!scoreEnabled,
        scoreWeights: scoreWeights || {},
        scoreDateField: scoreDateField || '',
        scoreHighSignalConfig: scoreHighSignalConfig || {},
    });
}

const BLOCKED_DOMAIN_CATEGORY_LABELS = {
    blogs: 'Blog platforms',
    dev_hosting: 'Dev hosting',
    social: 'Social media',
    parked: 'Parked / test',
    email: 'Email providers',
    marketplaces: 'Marketplaces',
};

const DEFAULT_BLOCKLIST_CATEGORIES = {
    blogs: true,
    dev_hosting: true,
    social: true,
    parked: true,
    email: true,
    marketplaces: true,
};

function defaultWorkspaceConfig(columns) {
    const firstField = columns?.[0]?.name || '';
    return {
        rules: [Rule(firstField, columns || [])],
        domChk: false,
        homepageChk: false,
        domField: '',
        websiteKeywords: [],
        websiteKeywordsText: '',
        websiteExcludeKeywords: [],
        websiteExcludeKeywordsText: '',
        tldCountryChk: false,
        tldDisallow: [],
        tldAllow: [...DEFAULT_TLD_ALLOWLIST],
        intraDedupe: false,
        intraDedupeCol: '',
        intraDedupeStrategy: 'first',
        domainBlocklistEnabled: false,
        domainBlocklistCategories: { ...DEFAULT_BLOCKLIST_CATEGORIES },
        customBlockedDomains: [],
        customBlockedDomainsText: '',
        scoreEnabled: false,
        scoreWeights: { richness: 25, diversity: 25, recency: 20, domain: 15, signal: 15 },
        scoreDateField: '',
        scoreHighSignalConfig: { column: '', values: [] },
    };
}

function appendConfigFormData(fd, config) {
    fd.append('rules', JSON.stringify(buildPayloadFromRules(config.rules || [])));
    fd.append('domainCheck', config.domChk ? 'true' : 'false');
    fd.append('homepageCheck', config.homepageChk ? 'true' : 'false');
    fd.append('domainField', config.domField || '');
    fd.append('websiteKeywords', JSON.stringify(normalizeKeywordList(config.websiteKeywords || [])));
    fd.append('websiteExcludeKeywords', JSON.stringify(normalizeKeywordList(config.websiteExcludeKeywords || [])));
    fd.append('excludeCountryTlds', config.tldCountryChk ? 'true' : 'false');
    fd.append('tldDisallowList', JSON.stringify(normalizeTldList(config.tldDisallow || [])));
    fd.append('tldAllowList', JSON.stringify(normalizeTldList(config.tldAllow || [])));
    fd.append('intraDedupe', config.intraDedupe ? 'true' : 'false');
    fd.append('intraDedupeColumns', JSON.stringify(config.intraDedupeCol ? [config.intraDedupeCol] : []));
    fd.append('intraDedupeStrategy', config.intraDedupeStrategy || 'first');
    fd.append('domainBlocklistEnabled', config.domainBlocklistEnabled ? 'true' : 'false');
    fd.append('domainBlocklistCategories', JSON.stringify(config.domainBlocklistCategories || DEFAULT_BLOCKLIST_CATEGORIES));
    fd.append('customBlockedDomains', JSON.stringify(config.customBlockedDomains || []));
    fd.append('scoreEnabled', config.scoreEnabled ? 'true' : 'false');
    fd.append('scoreWeights', JSON.stringify(config.scoreWeights || {}));
    fd.append('scoreDateField', config.scoreDateField || '');
    fd.append('scoreHighSignalConfig', JSON.stringify(config.scoreHighSignalConfig || {}));
}

function _makePresetId() {
    const rnd = Math.random().toString(16).slice(2);
    return `preset_${Date.now()}_${rnd}`;
}

function _isBrowserStorageAvailable() {
    return typeof window !== 'undefined' && typeof window.localStorage !== 'undefined';
}

function loadFilterPresets() {
    if (!_isBrowserStorageAvailable()) return [];
    try {
        const raw = window.localStorage.getItem(FILTER_PRESETS_STORAGE_KEY);
        if (!raw) return [];
        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed)) return [];
        return parsed
            .filter(item => item && typeof item === 'object' && item.id && item.name && item.config)
            .sort((a, b) => String(b.updatedAt || '').localeCompare(String(a.updatedAt || '')));
    } catch (_e) {
        return [];
    }
}

function _persistFilterPresets(presets) {
    if (!_isBrowserStorageAvailable()) return false;
    try {
        window.localStorage.setItem(FILTER_PRESETS_STORAGE_KEY, JSON.stringify(presets || []));
        return true;
    } catch (_e) {
        return false;
    }
}

function _sanitizeRecentRun(raw) {
    if (!raw || typeof raw !== 'object') return null;
    const completedAt = String(raw.completedAt || raw.createdAt || '');
    const runId = String(raw.runId || '');
    const sessionId = String(raw.sessionId || '');
    const fileName = String(raw.fileName || '').trim();
    if (!completedAt || !fileName) return null;
    return {
        id: String(raw.id || `${sessionId || 'session'}:${runId || completedAt}`),
        runId,
        sessionId,
        fileName,
        fileNames: Array.isArray(raw.fileNames) ? raw.fileNames.map(name => String(name || '').trim()).filter(Boolean) : [],
        totalRows: Number(raw.totalRows || 0),
        qualifiedCount: Number(raw.qualifiedCount || 0),
        removedCount: Number(raw.removedCount || 0),
        removedBreakdown: raw.removedBreakdown || {
            removedFilter: 0,
            removedDomain: 0,
            removedHubspot: 0,
            removedIntraDedupe: 0,
        },
        completedAt,
        processingMs: Number(raw.processingMs || 0),
        meta: raw.meta && typeof raw.meta === 'object' ? raw.meta : {},
    };
}

function loadRecentRuns() {
    if (!_isBrowserStorageAvailable()) return [];
    try {
        const raw = window.localStorage.getItem(RECENT_RUNS_STORAGE_KEY);
        if (!raw) return [];
        const parsed = JSON.parse(raw);
        if (!Array.isArray(parsed)) return [];
        return parsed
            .map(_sanitizeRecentRun)
            .filter(Boolean)
            .sort((a, b) => String(b.completedAt || '').localeCompare(String(a.completedAt || '')))
            .slice(0, MAX_RECENT_RUNS);
    } catch (_e) {
        return [];
    }
}

function _persistRecentRuns(runs) {
    if (!_isBrowserStorageAvailable()) return false;
    try {
        window.localStorage.setItem(RECENT_RUNS_STORAGE_KEY, JSON.stringify(runs || []));
        return true;
    } catch (_e) {
        return false;
    }
}

function saveWorkspaceRestoreState(payload) {
    if (!_isBrowserStorageAvailable()) return false;
    try {
        window.localStorage.setItem(WORKSPACE_RESTORE_STORAGE_KEY, JSON.stringify(payload || {}));
        return true;
    } catch (_e) {
        return false;
    }
}

function loadWorkspaceRestoreState() {
    if (!_isBrowserStorageAvailable()) return null;
    try {
        const raw = window.localStorage.getItem(WORKSPACE_RESTORE_STORAGE_KEY);
        if (!raw) return null;
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== 'object') return null;
        return parsed;
    } catch (_e) {
        return null;
    }
}

function appendRecentRun(rawEntry) {
    const entry = _sanitizeRecentRun(rawEntry);
    if (!entry) return { ok: false, runs: loadRecentRuns(), saved: null };

    const current = loadRecentRuns();
    const next = [entry, ...current.filter(item => item.id !== entry.id && item.runId !== entry.runId)];
    const sorted = next
        .sort((a, b) => String(b.completedAt || '').localeCompare(String(a.completedAt || '')))
        .slice(0, MAX_RECENT_RUNS);
    const ok = _persistRecentRuns(sorted);
    return { ok, runs: sorted, saved: entry };
}

function exportConfigPreset(config) {
    return {
        rules: (config?.rules || []).map(rule => ({
            field: rule?.field || '',
            matchType: rule?.matchType || 'contains',
            groups: (rule?.groups || []).map(group => ({
                tags: (group?.tags || []).map(tag => String(tag || '')).filter(Boolean),
                logic: group?.logic === 'and' ? 'and' : 'or',
            })),
            groupsLogic: rule?.groupsLogic === 'and' ? 'and' : 'or',
            threshold: Number(rule?.threshold ?? 80),
            min: String(rule?.min ?? ''),
            max: String(rule?.max ?? ''),
            startDate: String(rule?.startDate ?? ''),
            endDate: String(rule?.endDate ?? ''),
            includeBlankValues: !!rule?.includeBlankValues,
        })),
        domChk: !!config?.domChk,
        homepageChk: !!config?.homepageChk,
        domField: config?.domField || '',
        websiteKeywords: normalizeKeywordList(config?.websiteKeywords || []),
        websiteKeywordsText: String(config?.websiteKeywordsText ?? formatKeywordListInput(config?.websiteKeywords || [])),
        websiteExcludeKeywords: normalizeKeywordList(config?.websiteExcludeKeywords || []),
        websiteExcludeKeywordsText: String(config?.websiteExcludeKeywordsText ?? formatKeywordListInput(config?.websiteExcludeKeywords || [])),
        tldCountryChk: !!config?.tldCountryChk,
        tldDisallow: normalizeTldList(config?.tldDisallow || []),
        tldAllow: normalizeTldList(config?.tldAllow || []),
        intraDedupe: !!config?.intraDedupe,
        intraDedupeCol: config?.intraDedupeCol || '',
        intraDedupeStrategy: config?.intraDedupeStrategy || 'first',
        domainBlocklistEnabled: !!config?.domainBlocklistEnabled,
        domainBlocklistCategories: config?.domainBlocklistCategories || { ...DEFAULT_BLOCKLIST_CATEGORIES },
        customBlockedDomains: config?.customBlockedDomains || [],
        customBlockedDomainsText: String(config?.customBlockedDomainsText ?? (config?.customBlockedDomains || []).join(', ')),
        scoreEnabled: !!config?.scoreEnabled,
        scoreWeights: config?.scoreWeights || { richness: 25, diversity: 25, recency: 20, domain: 15, signal: 15 },
        scoreDateField: config?.scoreDateField || '',
        scoreHighSignalConfig: config?.scoreHighSignalConfig || { column: '', values: [] },
    };
}

function _buildRuleFromPreset(rawRule, columns) {
    const availableColumns = columns || [];
    const byName = new Set(availableColumns.map(col => col?.name).filter(Boolean));
    const rawField = String(rawRule?.field || '');
    const field = byName.size > 0 && !byName.has(rawField) ? '' : rawField;
    const base = Rule(field || availableColumns?.[0]?.name || '', availableColumns);
    const groups = Array.isArray(rawRule?.groups) && rawRule.groups.length > 0
        ? rawRule.groups
        : [{ tags: Array.isArray(rawRule?.values) ? rawRule.values : [], logic: 'or' }];
    return {
        ...base,
        field,
        matchType: rawRule?.matchType || base.matchType,
        groupsLogic: rawRule?.groupsLogic === 'and' ? 'and' : 'or',
        groups: groups.map(group => ({
            id: Date.now() + Math.random(),
            tags: (group?.tags || []).map(tag => String(tag || '').trim()).filter(Boolean),
            logic: group?.logic === 'and' ? 'and' : 'or',
        })),
        threshold: Number.isFinite(Number(rawRule?.threshold)) ? Number(rawRule.threshold) : 80,
        min: String(rawRule?.min ?? ''),
        max: String(rawRule?.max ?? ''),
        startDate: String(rawRule?.startDate ?? ''),
        endDate: String(rawRule?.endDate ?? ''),
        includeBlankValues: !!rawRule?.includeBlankValues,
    };
}

function importConfigPreset(presetConfig, columns) {
    const defaults = defaultWorkspaceConfig(columns || []);
    const rulesRaw = Array.isArray(presetConfig?.rules) ? presetConfig.rules : [];
    const builtRules = rulesRaw.map(rule => _buildRuleFromPreset(rule, columns || []));
    const nonEmptyRules = builtRules.filter(rule => !!rule.field);
    const rules = nonEmptyRules.length > 0 ? nonEmptyRules : defaults.rules;

    const validColumnNames = new Set((columns || []).map(col => col?.name).filter(Boolean));
    const domFieldRaw = String(presetConfig?.domField || '');
    const domField = validColumnNames.size > 0 && !validColumnNames.has(domFieldRaw) ? '' : domFieldRaw;
    const websiteKeywords = normalizeKeywordList(
        presetConfig?.websiteKeywords || parseKeywordListInput(presetConfig?.websiteKeywordsText || '')
    );
    const websiteExcludeKeywords = normalizeKeywordList(
        presetConfig?.websiteExcludeKeywords || parseKeywordListInput(presetConfig?.websiteExcludeKeywordsText || '')
    );

    return {
        ...defaults,
        rules,
        domChk: !!presetConfig?.domChk,
        homepageChk: !!presetConfig?.homepageChk,
        domField,
        websiteKeywords,
        websiteKeywordsText: String(presetConfig?.websiteKeywordsText ?? formatKeywordListInput(websiteKeywords)),
        websiteExcludeKeywords,
        websiteExcludeKeywordsText: String(presetConfig?.websiteExcludeKeywordsText ?? formatKeywordListInput(websiteExcludeKeywords)),
        tldCountryChk: !!presetConfig?.tldCountryChk,
        tldDisallow: normalizeTldList(presetConfig?.tldDisallow || []),
        tldAllow: normalizeTldList((presetConfig?.tldAllow || []).length ? presetConfig.tldAllow : DEFAULT_TLD_ALLOWLIST),
        intraDedupe: !!presetConfig?.intraDedupe,
        intraDedupeCol: presetConfig?.intraDedupeCol
            || (Array.isArray(presetConfig?.intraDedupeColumns) ? presetConfig.intraDedupeColumns[0] || '' : ''),
        intraDedupeStrategy: presetConfig?.intraDedupeStrategy || 'first',
        domainBlocklistEnabled: !!presetConfig?.domainBlocklistEnabled,
        domainBlocklistCategories: presetConfig?.domainBlocklistCategories || { ...DEFAULT_BLOCKLIST_CATEGORIES },
        customBlockedDomains: presetConfig?.customBlockedDomains || [],
        customBlockedDomainsText: String(presetConfig?.customBlockedDomainsText ?? (presetConfig?.customBlockedDomains || []).join(', ')),
        scoreEnabled: !!presetConfig?.scoreEnabled,
        scoreWeights: presetConfig?.scoreWeights || { richness: 25, diversity: 25, recency: 20, domain: 15, signal: 15 },
        scoreDateField: presetConfig?.scoreDateField || '',
        scoreHighSignalConfig: presetConfig?.scoreHighSignalConfig || { column: '', values: [] },
    };
}

function saveFilterPreset({ name, config, presetId = '' }) {
    const cleanName = String(name || '').trim();
    if (!cleanName) return { ok: false, presets: loadFilterPresets(), saved: null };

    const existing = loadFilterPresets();
    const now = new Date().toISOString();
    const exportPayload = exportConfigPreset(config || {});
    let next = [...existing];
    let saved = null;

    const byIdIndex = presetId ? next.findIndex(item => item.id === presetId) : -1;
    const byNameIndex = byIdIndex < 0
        ? next.findIndex(item => String(item.name || '').toLowerCase() === cleanName.toLowerCase())
        : -1;
    const targetIndex = byIdIndex >= 0 ? byIdIndex : byNameIndex;

    if (targetIndex >= 0) {
        const current = next[targetIndex];
        saved = {
            ...current,
            name: cleanName,
            config: exportPayload,
            updatedAt: now,
        };
        next[targetIndex] = saved;
    } else {
        saved = {
            id: _makePresetId(),
            name: cleanName,
            config: exportPayload,
            createdAt: now,
            updatedAt: now,
        };
        next.unshift(saved);
    }

    next = next.sort((a, b) => String(b.updatedAt || '').localeCompare(String(a.updatedAt || '')));
    const ok = _persistFilterPresets(next);
    return { ok, presets: next, saved };
}

function deleteFilterPreset(presetId) {
    const current = loadFilterPresets();
    const next = current.filter(item => item.id !== presetId);
    const ok = _persistFilterPresets(next);
    return { ok, presets: next };
}

function renameFilterPreset(presetId, newName) {
    const cleanName = String(newName || '').trim();
    if (!cleanName) return { ok: false, presets: loadFilterPresets(), renamed: null };
    const current = loadFilterPresets();
    const now = new Date().toISOString();
    const next = current.map(item =>
        item.id === presetId ? { ...item, name: cleanName, updatedAt: now } : item
    );
    const ok = _persistFilterPresets(next);
    const renamed = next.find(item => item.id === presetId) || null;
    return { ok, presets: next, renamed };
}

function _normalizeHeaderName(value) {
    return String(value || '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function _findColumnsByHints(columns, hints) {
    const normalizedHints = (hints || []).map(_normalizeHeaderName).filter(Boolean);
    if (!normalizedHints.length) return [];
    return (columns || []).filter(col => {
        const name = _normalizeHeaderName(col?.name || col);
        return normalizedHints.some(hint => name.includes(hint));
    });
}

function _coverageForColumns(columnMatches, profileByName) {
    if (!columnMatches?.length) return 0;
    let best = 0;
    columnMatches.forEach(col => {
        const name = col?.name || col;
        const profile = profileByName[name];
        const coverage = profile && typeof profile.nullRate === 'number'
            ? Math.max(0, Math.min(1, 1 - profile.nullRate))
            : 0;
        best = Math.max(best, coverage);
    });
    return best;
}

function _coveragePercentLabel(value) {
    return `${Math.round(Math.max(0, Math.min(1, value)) * 100)}%`;
}

function _icpStatusFromCoverage(hasColumns, coverage, readyThreshold = 0.7, partialThreshold = 0.35) {
    if (!hasColumns) return 'gap';
    if (coverage >= readyThreshold) return 'ready';
    if (coverage >= partialThreshold) return 'partial';
    return 'gap';
}

function _icpCriterion(label, status, columns, coverage, gapPlan) {
    return {
        label,
        status,
        columns: (columns || []).map(col => col?.name || col).filter(Boolean),
        coverageLabel: _coveragePercentLabel(coverage),
        gapPlan,
    };
}

function buildIcpGapReport(session, config = {}) {
    const columns = session?.columns || [];
    const profiles = session?.columnProfiles || [];
    const profileByName = {};
    profiles.forEach(profile => {
        if (profile?.name) profileByName[profile.name] = profile;
    });

    const employeeCols = _findColumnsByHints(columns, ['employee', 'headcount', 'team size', 'staff', 'fte']);
    const stageCols = _findColumnsByHints(columns, ['series', 'funding stage', 'funding round', 'stage', 'total funding', 'raised', 'investment']);
    const activeCols = _findColumnsByHints(columns, ['last found', 'last seen', 'last indexed', 'status', 'active', 'updated']);
    const growthCols = _findColumnsByHints(columns, ['growth', 'employee growth', 'hiring', 'tranco', 'page rank', 'cloudflare rank', 'traffic', 'social']);
    const locationCols = _findColumnsByHints(columns, ['country', 'state', 'city', 'location', 'region', 'hq', 'headquarters']);
    const b2bCols = _findColumnsByHints(columns, ['vertical', 'industry', 'category', 'segment', 'description', 'keywords', 'business model']);
    const foundedCols = _findColumnsByHints(columns, ['founded', 'founding', 'year founded', 'incorporated', 'launch date']);
    const foundedProxyCols = _findColumnsByHints(columns, ['first detected', 'first indexed', 'created', 'discovered']);

    const employeeCoverage = _coverageForColumns(employeeCols, profileByName);
    const stageCoverage = _coverageForColumns(stageCols, profileByName);
    const activeCoverage = _coverageForColumns(activeCols, profileByName);
    const growthCoverage = _coverageForColumns(growthCols, profileByName);
    const locationCoverage = _coverageForColumns(locationCols, profileByName);
    const b2bCoverage = _coverageForColumns(b2bCols, profileByName);
    const foundedCoverage = _coverageForColumns(foundedCols, profileByName);
    const foundedProxyCoverage = _coverageForColumns(foundedProxyCols, profileByName);

    const domainSignalsEnabled = !!(config?.domField && (config?.domChk || config?.homepageChk || config?.tldCountryChk || (config?.tldDisallow || []).length));
    const homepageSignalsEnabled = !!(config?.homepageChk && config?.domField);
    const keywordCount = normalizeKeywordList(config?.websiteKeywords || []).length;

    const employeeStatus = _icpStatusFromCoverage(employeeCols.length > 0, employeeCoverage);
    const stageStatus = _icpStatusFromCoverage(stageCols.length > 0, stageCoverage);

    const hasActiveSignals = activeCols.length > 0;
    const hasGrowthSignals = growthCols.length > 0;
    let activeGrowthStatus = 'gap';
    if (hasActiveSignals && hasGrowthSignals && Math.max(activeCoverage, growthCoverage) >= 0.45) activeGrowthStatus = 'ready';
    else if (hasActiveSignals || hasGrowthSignals || homepageSignalsEnabled) activeGrowthStatus = 'partial';

    let locationStatus = _icpStatusFromCoverage(locationCols.length > 0, locationCoverage);
    if (locationStatus === 'gap' && domainSignalsEnabled) locationStatus = 'partial';

    let b2bStatus = _icpStatusFromCoverage(b2bCols.length > 0, b2bCoverage);
    if (b2bStatus !== 'ready' && homepageSignalsEnabled) b2bStatus = b2bStatus === 'gap' ? 'partial' : b2bStatus;

    let foundedStatus = _icpStatusFromCoverage(foundedCols.length > 0, foundedCoverage);
    if (foundedStatus === 'gap' && foundedProxyCols.length > 0) foundedStatus = foundedProxyCoverage >= 0.6 ? 'partial' : 'gap';

    const criteria = [
        _icpCriterion(
            '1-50 employees',
            employeeStatus,
            employeeCols,
            employeeCoverage,
            employeeStatus === 'ready'
                ? 'Use a numeric range rule (min 1, max 50) and keep min populated to avoid blank employee values passing.'
                : 'Add/standardize one employee-count field in every source (aliases: employees, headcount, team_size).'
        ),
        _icpCriterion(
            'Series A or below',
            stageStatus,
            stageCols,
            stageCoverage,
            stageStatus === 'ready'
                ? 'Normalize stage labels to a single taxonomy (pre-seed, seed, series_a, etc.) before qualification.'
                : 'Enrich each company with funding stage/last round from a startup enrichment source, then map it to one canonical column.'
        ),
        _icpCriterion(
            'Still active and fast growing',
            activeGrowthStatus,
            [...activeCols, ...growthCols],
            Math.max(activeCoverage, growthCoverage),
            activeGrowthStatus === 'ready'
                ? 'Use recency rules on activity columns and add growth thresholds from ranking/traffic deltas.'
                : 'Track snapshots over time (weekly/monthly) so growth is computed as a delta, not a single static value.'
        ),
        _icpCriterion(
            'Based in the US',
            locationStatus,
            locationCols,
            locationCoverage,
            locationStatus === 'ready'
                ? 'Prioritize explicit country/state fields, then use domain geo checks as a secondary validator.'
                : domainSignalsEnabled
                    ? 'Keep domain geo checks on, but add explicit country/state data to reduce inconclusive CDN passes.'
                    : 'Add a normalized country column (ISO2 like US) and turn on domain checks for backup validation.'
        ),
        _icpCriterion(
            'B2B Tech',
            b2bStatus,
            b2bCols,
            b2bCoverage,
            b2bStatus === 'ready'
                ? 'Combine vertical/industry rules with homepage checks for higher precision.'
                : homepageSignalsEnabled
                    ? `Homepage checks are enabled${keywordCount ? ` with ${keywordCount} keyword${keywordCount === 1 ? '' : 's'}` : ''}; add stable industry/segment columns for stronger classification.`
                    : 'Enable homepage checks and add targeted website keywords, then backfill a canonical industry/vertical field.'
        ),
        _icpCriterion(
            'Founded in last two years',
            foundedStatus,
            foundedCols.length ? foundedCols : foundedProxyCols,
            foundedCols.length ? foundedCoverage : foundedProxyCoverage,
            foundedCols.length
                ? 'Apply a strict founded-date cutoff in qualification using the Date range rule.'
                : foundedProxyCols.length
                    ? 'Use first-detected dates only as a temporary proxy; add real founded-date enrichment for production filtering.'
                    : 'Add a founded-date field via enrichment and normalize to ISO dates for deterministic filtering.'
        ),
    ];

    const summary = {
        ready: criteria.filter(item => item.status === 'ready').length,
        partial: criteria.filter(item => item.status === 'partial').length,
        gap: criteria.filter(item => item.status === 'gap').length,
        total: criteria.length,
    };

    return { criteria, summary };
}

function buildPayloadFromRules(rules) {
    return rules.filter(r => r.field).map(r => {
        if (r.matchType === 'range') {
            return { field: r.field, matchType: r.matchType, values: [], threshold: r.threshold, min: r.min, max: r.max, includeBlankValues: !!r.includeBlankValues };
        }
        if (r.matchType === 'dates') {
            return {
                field: r.field,
                matchType: r.matchType,
                values: [],
                threshold: r.threshold,
                startDate: r.startDate,
                endDate: r.endDate,
                includeBlankValues: !!r.includeBlankValues,
            };
        }
        const isContains = r.matchType === 'contains' || r.matchType === 'not_contains';
        if (isContains && r.groups) {
            return {
                field: r.field,
                matchType: r.matchType,
                groups: r.groups.map(g => ({ tags: g.tags, logic: g.logic || 'and' })),
                groupsLogic: r.groupsLogic || 'or',
                threshold: r.threshold,
            };
        }
        if (r.matchType?.startsWith('multivalue_')) {
            const allTags = (r.groups || []).flatMap(g => g.tags || []);
            return {
                field: r.field,
                matchType: r.matchType,
                values: allTags,
                logic: 'or',
                threshold: r.threshold,
                separator: r.separator || ';',
            };
        }
        const allTags = (r.groups || []).flatMap(g => g.tags || []);
        return {
            field: r.field,
            matchType: r.matchType,
            values: allTags,
            logic: 'or',
            threshold: r.threshold,
        };
    });
}

function normalizeLink(value, columnName = '') {
    const raw = String(value || '').trim();
    if (!raw) return '';
    const lower = raw.toLowerCase();
    const col = String(columnName || '').toLowerCase();
    const isLinkColumn = col.includes('domain') || col.includes('website') || col.includes('url') || col.includes('linkedin') || col.includes('link');
    const looksLikeUrl = lower.startsWith('http://') || lower.startsWith('https://') || lower.startsWith('www.');
    const looksLikeDomain = /^[a-z0-9.-]+\.[a-z]{2,}(\/.*)?$/i.test(raw);
    const looksLikeLinkedInHandle = lower.includes('linkedin.com');

    if (!(isLinkColumn || looksLikeUrl || looksLikeDomain || looksLikeLinkedInHandle)) return '';
    if (looksLikeUrl) return raw;
    return `https://${raw}`;
}

function renderTableCell(columnName, value, format = 'auto') {
    if (value === null || value === undefined || value === '') return '';
    const raw = String(value || '').trim();
    const col = String(columnName || '').toLowerCase();
    const normalizedFormat = String(format || 'auto').toLowerCase();

    if (normalizedFormat === 'text') {
        return String(value);
    }

    if (normalizedFormat === 'number' || normalizedFormat === 'currency') {
        const parsed = parseFloat(raw.replace(/[^0-9.\-]/g, ''));
        if (!Number.isNaN(parsed)) {
            if (normalizedFormat === 'currency') {
                return parsed.toLocaleString(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
            }
            return parsed.toLocaleString();
        }
    }

    if (normalizedFormat === 'url') {
        const href = normalizeLink(value, columnName);
        if (href) {
            return (
                <a className="cell-link" href={href} target="_blank" rel="noopener noreferrer" onClick={(e) => e.stopPropagation()}>
                    {String(value)}
                </a>
            );
        }
    }

    if (raw.toLowerCase() === 'true' || raw.toLowerCase() === 'false') {
        return (
            <span className={`bool-chip ${raw.toLowerCase() === 'true' ? 'is-true' : 'is-false'}`}>
                {raw.toLowerCase() === 'true' ? <I.check /> : ''}
            </span>
        );
    }

    if (col.includes('role') || col.includes('title') || col.includes('position')) {
        const toneIdx = Math.abs(raw.split('').reduce((acc, ch) => acc + ch.charCodeAt(0), 0)) % 4;
        return <span className={`cell-tag tone-${toneIdx}`}>{raw}</span>;
    }

    if (col.includes('skill') && /^\d+$/.test(raw)) {
        const count = Math.max(0, Math.min(parseInt(raw, 10), 5));
        return (
            <span className="cell-stars" aria-label={`${count} stars`}>
                {'★'.repeat(count)}
                {'☆'.repeat(5 - count)}
            </span>
        );
    }

    const href = normalizeLink(value, columnName);
    if (!href) return String(value);
    return (
        <a className="cell-link" href={href} target="_blank" rel="noopener noreferrer" onClick={(e) => e.stopPropagation()}>
            {String(value)}
        </a>
    );
}
