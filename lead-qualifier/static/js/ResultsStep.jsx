/* ── Results Step ──────────────────────────────────────────── */
function ResultsStep({ results, error, dead, domChecked, loading, onEdit, onNew, onDl, isDirty }) {
    const [sortCol, setSortCol] = useState(null);
    const [sortDir, setSortDir] = useState(null);
    const [search, setSearch] = useState('');
    const [pageSize, setPageSize] = useState(100);
    const [page, setPage] = useState(1);
    const [viewMode, setViewMode] = useState('all'); // all | qualified | removed

    const searchLower = search.toLowerCase().trim();
    const tableRows = results?.rows || results?.leads || [];
    const statusAwareRows = useMemo(() => {
        return tableRows.map(row => ({
            ...row,
            _rowStatus: row._rowStatus || 'qualified',
        }));
    }, [tableRows]);

    const modeRows = useMemo(() => {
        if (viewMode === 'qualified') return statusAwareRows.filter(row => row._rowStatus === 'qualified');
        if (viewMode === 'removed') return statusAwareRows.filter(row => row._rowStatus === 'removed');
        return statusAwareRows;
    }, [statusAwareRows, viewMode]);

    const searched = useMemo(() => {
        if (!results || !modeRows) return [];
        if (!searchLower) return modeRows;
        return modeRows.filter(row =>
            results.columns.some(col => String(row[col] || '').toLowerCase().includes(searchLower))
        );
    }, [results, searchLower, modeRows]);

    const sorted = useMemo(() => {
        if (!sortCol || !sortDir) return searched;
        const copy = [...searched];
        copy.sort((a, b) => {
            let va = a[sortCol];
            let vb = b[sortCol];
            const na = parseFloat(String(va).replace(/[^0-9.\-]/g, ''));
            const nb = parseFloat(String(vb).replace(/[^0-9.\-]/g, ''));
            if (!isNaN(na) && !isNaN(nb)) return sortDir === 'asc' ? na - nb : nb - na;

            va = String(va || '').toLowerCase();
            vb = String(vb || '').toLowerCase();
            if (va < vb) return sortDir === 'asc' ? -1 : 1;
            if (va > vb) return sortDir === 'asc' ? 1 : -1;
            return 0;
        });
        return copy;
    }, [searched, sortCol, sortDir]);

    useEffect(() => setPage(1), [search, pageSize, sortCol, sortDir, viewMode]);

    const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize));
    const safePage = Math.min(page, totalPages);
    const pageStart = (safePage - 1) * pageSize;
    const currentRows = sorted.slice(pageStart, pageStart + pageSize);

    const handleHeaderClick = (col) => {
        if (sortCol === col) {
            if (sortDir === 'asc') setSortDir('desc');
            else if (sortDir === 'desc') { setSortCol(null); setSortDir(null); }
        } else {
            setSortCol(col);
            setSortDir('asc');
        }
    };

    const resetTableView = () => {
        setSearch('');
        setSortCol(null);
        setSortDir(null);
        setPage(1);
    };

    const SortIcon = ({ col }) => {
        if (sortCol !== col) return <span className="sort-arrow sort-idle"><I.chevDown /></span>;
        if (sortDir === 'asc') return <span className="sort-arrow sort-active"><I.arrowUp /></span>;
        return <span className="sort-arrow sort-active"><I.arrowDown /></span>;
    };

    const processingMs = results?.meta?.processingMs || 0;
    const qualifiedInView = statusAwareRows.filter(row => row._rowStatus === 'qualified').length;
    const removedInView = statusAwareRows.filter(row => row._rowStatus === 'removed').length;
    const dedupeMeta = results?.meta?.dedupe;

    return (
        <section>
            <div className="pg-head">
                <h1>Qualification results</h1>
                <p>Leads filtered against your ICP criteria{domChecked > 0 ? ' and domain verification' : ''}.</p>
            </div>

            {isDirty && (
                <div className="inline-msg warn">
                    <I.info /> Results reflect an older configuration. Re-run qualification to refresh.
                </div>
            )}

            {error && (
                <div className="pnl">
                    <div className="empty">
                        <I.alertTri style={{ width: 28, height: 28 }} />
                        <h3>Unable to load qualification results</h3>
                        <p>{error}</p>
                    </div>
                </div>
            )}

            {!error && results && (
                <>
                    <div className="pnl">
                        <div className="stats">
                            <div className="stat">
                                <div className="stat-v">{results.totalRows.toLocaleString()}</div>
                                <div className="stat-l">Total Leads</div>
                            </div>
                            <div className="stat">
                                <div className="stat-v s-ok">{results.qualifiedCount.toLocaleString()}</div>
                                <div className="stat-l">Qualified</div>
                            </div>
                            <div className="stat">
                                <div className="stat-v s-rm">{results.removedCount.toLocaleString()}</div>
                                <div className="stat-l">Removed</div>
                            </div>
                            <div className="stat">
                                <div className="stat-v">{(processingMs / 1000).toFixed(1)}s</div>
                                <div className="stat-l">Processing Time</div>
                            </div>
                        </div>
                        {(dedupeMeta?.enabled || domChecked > 0) && (
                            <div className="pnl-foot">
                                <div className="tb-item">
                                    {dedupeMeta?.enabled && (
                                        <span>
                                            HubSpot dedupe removed {dedupeMeta.removedCount.toLocaleString()} duplicates
                                            {dedupeMeta.keyType ? ` using ${dedupeMeta.keyType} matching` : ''}
                                        </span>
                                    )}
                                </div>
                                {domChecked > 0 && (
                                    <button type="button" className="badge badge-m dead-jump" onClick={() => {
                                        const panel = document.getElementById('dead-domains-panel');
                                        if (panel) panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
                                    }}>
                                        <I.alertTri /> Dead domains: {dead.length}
                                    </button>
                                )}
                            </div>
                        )}
                    </div>

                    {dead.length > 0 && <DeadPanel domains={dead} total={domChecked} />}

                    <div className="pnl">
                        <div className="command-bar">
                            <div className="command-left">
                                <div className="search-wrap">
                                    <I.search />
                                    <input
                                        className="search-input"
                                        type="text"
                                        value={search}
                                        onChange={e => setSearch(e.target.value)}
                                        placeholder="Search rows..."
                                        aria-label="Search rows"
                                    />
                                    {search && <button type="button" className="search-clear" aria-label="Clear search" onClick={() => setSearch('')}><I.x /></button>}
                                </div>
                                <div className="segmented" role="tablist" aria-label="Row visibility">
                                    <button
                                        type="button"
                                        role="tab"
                                        aria-selected={viewMode === 'all'}
                                        className={`seg-opt ${viewMode === 'all' ? 'active' : ''}`}
                                        onClick={() => setViewMode('all')}
                                    >
                                        All ({statusAwareRows.length.toLocaleString()})
                                    </button>
                                    <button
                                        type="button"
                                        role="tab"
                                        aria-selected={viewMode === 'qualified'}
                                        className={`seg-opt ${viewMode === 'qualified' ? 'active' : ''}`}
                                        onClick={() => setViewMode('qualified')}
                                    >
                                        Qualified ({qualifiedInView.toLocaleString()})
                                    </button>
                                    <button
                                        type="button"
                                        role="tab"
                                        aria-selected={viewMode === 'removed'}
                                        className={`seg-opt ${viewMode === 'removed' ? 'active' : ''}`}
                                        onClick={() => setViewMode('removed')}
                                    >
                                        Removed ({removedInView.toLocaleString()})
                                    </button>
                                </div>
                                <span className="tb-item">{sorted.length.toLocaleString()} rows shown</span>
                                {(sortCol && sortDir) && <span className="tb-item">Sorted by {sortCol} ({sortDir})</span>}
                            </div>
                            <div className="command-right">
                                <label className="table-size">
                                    <span>Page size</span>
                                    <select value={pageSize} onChange={e => setPageSize(parseInt(e.target.value, 10))}>
                                        <option value={50}>50</option>
                                        <option value={100}>100</option>
                                        <option value={250}>250</option>
                                    </select>
                                </label>
                                <button type="button" className="btn btn-g" onClick={resetTableView}>Reset table view</button>
                                <button type="button" className="btn btn-p" onClick={onDl} disabled={loading || results.qualifiedCount === 0}>
                                    {loading && <span className="spinner" />} <I.download /> Export CSV
                                </button>
                            </div>
                        </div>

                        {sorted.length > 0 ? (
                            <>
                                <div className="tw results-table-wrap">
                                    <table>
                                        <thead>
                                            <tr>
                                                <th style={{ width: '44px' }}>#</th>
                                                {results.columns.map(col => (
                                                    <th key={col} className="th-sort" onClick={() => handleHeaderClick(col)} tabIndex={0}
                                                        onKeyDown={(event) => { if (event.key === 'Enter' || event.key === ' ') handleHeaderClick(col); }}>
                                                        <FieldType name={col} /> {col} <SortIcon col={col} />
                                                    </th>
                                                ))}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {currentRows.map((row, index) => (
                                                <tr key={`${safePage}-${index}`} className={row._rowStatus === 'removed' ? 'row-removed' : 'row-qualified'}>
                                                    <td className="rn">{pageStart + index + 1}</td>
                                                    {results.columns.map(col => (
                                                        <td key={col} title={row[col]}>{renderTableCell(col, row[col])}</td>
                                                    ))}
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                                <div className="pnl-foot">
                                    <span className="tb-item">Page {safePage} of {totalPages}</span>
                                    <div className="btn-row">
                                        <button type="button" className="btn btn-g" disabled={safePage <= 1} onClick={() => setPage(safePage - 1)}>Previous</button>
                                        <button type="button" className="btn btn-g" disabled={safePage >= totalPages} onClick={() => setPage(safePage + 1)}>Next</button>
                                    </div>
                                </div>
                            </>
                        ) : (
                            <div className="empty">
                                <I.search style={{ width: 28, height: 28 }} />
                                <h3>No rows match the current table search</h3>
                                <p>Try clearing search or switch to another row view.</p>
                            </div>
                        )}
                    </div>
                </>
            )}

            <div className="action-rail">
                <div className="action-rail-meta">
                    <span>Need a different output? Update rules and run again.</span>
                </div>
                <div className="btn-row">
                    <button className="btn btn-g" onClick={onEdit}><I.arrowL /> Edit filters</button>
                    <button className="btn btn-g" onClick={onNew}><I.refresh /> New import</button>
                </div>
            </div>
        </section>
    );
}

function DeadPanel({ domains, total }) {
    const [open, setOpen] = useState(false);
    return (
        <div className="pnl" id="dead-domains-panel">
            <button type="button" className="coll-head" onClick={() => setOpen(!open)}>
                <h3 style={{ color: 'var(--amber)' }}><I.alertTri /> Inactive domains removed ({domains.length})</h3>
                <span className={`chv ${open ? 'open' : ''}`}><I.chevDown /></span>
            </button>
            {open && (
                <div className="coll-body">
                    <div className="dead-sum">{total} domains verified · {domains.length} found inactive and excluded</div>
                    <div className="dead-list">
                        {domains.map((domain, index) => (
                            <div key={index} className="dead-item">
                                <span className="dn">{domain.domain}</span>
                                <span className="ds">{domain.status}</span>
                            </div>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}
