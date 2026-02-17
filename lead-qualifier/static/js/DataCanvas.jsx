/* ── Data Canvas ───────────────────────────────────────────── */
function DataCanvas({
    session,
    runSummary,
    runProgress,
    viewSearch,
    viewSort,
    onViewSort,
    viewFilters,
    selectedRowId,
    onSelectRow,
}) {
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(100);
    const [selectedCell, setSelectedCell] = useState(null);

    const [previewRows, setPreviewRows] = useState([]);
    const [previewMeta, setPreviewMeta] = useState({
        totalRows: session?.totalRows || 0,
        filteredRows: 0,
        loading: false,
        error: '',
    });

    const isReviewData = !!runSummary;
    const runProgressSignal = `${runProgress?.status || ''}:${runProgress?.stage || ''}:${runProgress?.processedRows || 0}:${runProgress?.qualifiedCount || 0}:${runProgress?.removedCount || 0}`;
    const columns = isReviewData ? (runSummary.columns || []) : (session?.columns || []).map(c => c.name);

    const profileByName = useMemo(() => {
        const out = {};
        for (const profile of (session?.columnProfiles || [])) {
            if (profile?.name) out[profile.name] = profile;
        }
        return out;
    }, [session?.columnProfiles]);

    useEffect(() => {
        setPage(1);
    }, [viewSearch, JSON.stringify(viewFilters), viewSort?.column, viewSort?.direction, pageSize]);

    useEffect(() => {
        if (isReviewData || !session?.sessionId || columns.length === 0) return;

        let cancelled = false;
        const timer = setTimeout(async () => {
            try {
                setPreviewMeta(prev => ({ ...prev, loading: true, error: '' }));
                const fd = new FormData();
                fd.append('sessionId', session.sessionId);
                fd.append('page', String(page));
                fd.append('pageSize', String(pageSize));
                fd.append('search', viewSearch || '');
                fd.append('sortCol', viewSort?.column || '');
                fd.append('sortDir', viewSort?.direction || '');
                fd.append('viewFilters', JSON.stringify(viewFilters || []));
                const data = await requestJSON(`${API}/api/session/rows`, { method: 'POST', body: fd });
                if (cancelled) return;

                setPreviewRows(data.rows || []);
                setPreviewMeta({
                    totalRows: data.totalRows || 0,
                    filteredRows: data.filteredRows || 0,
                    loading: false,
                    error: '',
                });
                if ((data.page || page) !== page) setPage(data.page || 1);
            } catch (e) {
                if (cancelled) return;
                setPreviewRows([]);
                setPreviewMeta(prev => ({
                    ...prev,
                    loading: false,
                    error: e?.message || 'Failed to load rows for this page.',
                }));
            }
        }, 160);

        return () => {
            cancelled = true;
            clearTimeout(timer);
        };
    }, [isReviewData, session?.sessionId, session?.totalRows, columns.length, page, pageSize, viewSearch, viewSort?.column, viewSort?.direction, JSON.stringify(viewFilters), runProgressSignal]);

    const matchesViewFilter = (row, filter) => {
        const raw = row?.[filter.field];
        const value = raw === null || raw === undefined ? '' : String(raw);
        const v = value.toLowerCase();
        const target = String(filter.value || '').toLowerCase();

        if (filter.op === 'contains') return target ? v.includes(target) : true;
        if (filter.op === 'equals') return v === target;
        if (filter.op === 'not_equals') return v !== target;
        if (filter.op === 'is_empty') return !String(value || '').trim();
        if (filter.op === 'is_not_empty') return !!String(value || '').trim();

        if (filter.op === 'before' || filter.op === 'after') {
            const leftTs = Date.parse(value);
            const rightTs = Date.parse(String(filter.value || ''));
            if (!Number.isNaN(leftTs) && !Number.isNaN(rightTs)) {
                return filter.op === 'before' ? leftTs < rightTs : leftTs > rightTs;
            }
            if (filter.op === 'before') return value < String(filter.value || '');
            return value > String(filter.value || '');
        }

        return true;
    };

    const reviewRowsFiltered = useMemo(() => {
        if (!isReviewData) return [];
        const searchLower = String(viewSearch || '').toLowerCase().trim();
        const base = runSummary?.rows || [];

        return base.filter(row => {
            const passesSearch = !searchLower || columns.some(col => String(row[col] || '').toLowerCase().includes(searchLower));
            const passesViewFilters = (viewFilters || []).every(filter => matchesViewFilter(row, filter));
            return passesSearch && passesViewFilters;
        });
    }, [isReviewData, runSummary?.rows, columns, viewSearch, JSON.stringify(viewFilters)]);

    const reviewRowsSorted = useMemo(() => {
        if (!isReviewData) return [];
        const sortCol = viewSort?.column;
        const sortDir = viewSort?.direction;
        if (!sortCol || !sortDir) return reviewRowsFiltered;

        const copy = [...reviewRowsFiltered];
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
    }, [isReviewData, reviewRowsFiltered, viewSort?.column, viewSort?.direction]);

    let totalPages;
    let safePage;
    let pageStart;
    let currentRows;
    let rowsShownCount;
    let totalRowsCount;

    if (isReviewData) {
        rowsShownCount = reviewRowsSorted.length;
        totalRowsCount = runSummary?.rows?.length || 0;
        totalPages = Math.max(1, Math.ceil(reviewRowsSorted.length / pageSize));
        safePage = Math.min(page, totalPages);
        pageStart = (safePage - 1) * pageSize;
        currentRows = reviewRowsSorted.slice(pageStart, pageStart + pageSize);
    } else {
        totalRowsCount = previewMeta.totalRows || session?.totalRows || 0;
        rowsShownCount = previewMeta.filteredRows || 0;
        totalPages = Math.max(1, Math.ceil(rowsShownCount / pageSize));
        safePage = Math.min(page, totalPages);
        pageStart = (safePage - 1) * pageSize;
        currentRows = previewRows;
    }

    useEffect(() => {
        if (page > totalPages) setPage(totalPages);
    }, [page, totalPages]);

    useEffect(() => {
        setSelectedCell(null);
    }, [viewSearch, JSON.stringify(viewFilters), viewSort?.column, viewSort?.direction, page, pageSize]);

    const toggleSort = (col) => {
        const activeCol = viewSort?.column;
        const activeDir = viewSort?.direction;
        if (activeCol === col) {
            if (activeDir === 'asc') onViewSort({ column: col, direction: 'desc' });
            else if (activeDir === 'desc') onViewSort({ column: null, direction: null });
            else onViewSort({ column: col, direction: 'asc' });
        } else {
            onViewSort({ column: col, direction: 'asc' });
        }
    };

    const SortIcon = ({ col }) => {
        if (viewSort?.column !== col || !viewSort?.direction) return <span className="sort-arrow sort-idle"><I.chevDown /></span>;
        return <span className="sort-arrow sort-active">{viewSort.direction === 'asc' ? <I.arrowUp /> : <I.arrowDown />}</span>;
    };

    const statusLabels = {
        processing: 'Processing',
        qualified: 'Qualified',
        removed_filter: 'Removed · filter',
        removed_domain: 'Removed · domain',
        removed_hubspot: 'Removed · hubspot',
    };

    return (
        <div className="dc-shell">
            <div className="dc-header-row">
                <span className="tb-item">{rowsShownCount.toLocaleString()} shown</span>
                <span className="tb-item">{totalRowsCount.toLocaleString()} total</span>
                {!isReviewData && previewMeta.loading && <span className="tb-item muted">Loading…</span>}
                {!isReviewData && runProgress?.status === 'running' && (
                    <span className="tb-item tb-item-accent">
                        Running qualification {Math.max(0, Math.min(100, Math.round((runProgress?.progress || 0) * 100)))}%
                    </span>
                )}
                <label className="table-size">
                    <span>Page size</span>
                    <select value={pageSize} onChange={e => setPageSize(parseInt(e.target.value, 10))}>
                        <option value={50}>50</option>
                        <option value={100}>100</option>
                        <option value={250}>250</option>
                    </select>
                </label>
            </div>

            {!isReviewData && previewMeta.error && (
                <div className="inline-msg err"><I.alertTri /> {previewMeta.error}</div>
            )}

            {columns.length === 0 ? (
                <div className="empty">
                    <I.file style={{ width: 28, height: 28 }} />
                    <h3>No dataset loaded</h3>
                    <p>Import a dataset to begin qualification and table exploration.</p>
                </div>
            ) : (
                <>
                    <div className="tw results-table-wrap">
                        <table>
                            <thead>
                                <tr>
                                    <th style={{ width: '56px' }}>#</th>
                                    <th style={{ width: '138px' }}>Status</th>
                                    {columns.map(col => (
                                        <th key={col} className="th-sort">
                                            <button type="button" className="th-btn" onClick={() => toggleSort(col)} aria-label={`Sort by ${col}`}>
                                                <FieldType name={col} inferredType={profileByName[col]?.inferredType} /> {col} <SortIcon col={col} />
                                            </button>
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {currentRows.map((row, idx) => {
                                    const status = row._rowStatus || RowStatus.QUALIFIED;
                                    const rowId = row._rowId ?? `${safePage}-${idx}`;
                                    return (
                                        <tr
                                            key={rowId}
                                            className={`row-${status} ${String(status || '').startsWith('removed_') ? 'row-removed' : ''} ${selectedRowId === rowId ? 'row-selected' : ''}`}
                                            onClick={() => onSelectRow({ ...row, _rowId: rowId })}
                                            onKeyDown={(event) => {
                                                if (event.key === 'Enter' || event.key === ' ') {
                                                    event.preventDefault();
                                                    onSelectRow({ ...row, _rowId: rowId });
                                                }
                                            }}
                                            tabIndex={0}
                                        >
                                            <td className="rn">{pageStart + idx + 1}</td>
                                            <td><span className={`status-chip ${status}`}>{statusLabels[status] || status.replaceAll('_', ' ')}</span></td>
                                            {columns.map(col => (
                                                <td
                                                    key={col}
                                                    title={row[col]}
                                                    className={selectedCell?.rowId === rowId && selectedCell?.column === col ? 'cell-selected' : ''}
                                                    onClick={(event) => {
                                                        event.stopPropagation();
                                                        setSelectedCell({ rowId, column: col });
                                                        onSelectRow({ ...row, _rowId: rowId });
                                                    }}
                                                >
                                                    {renderTableCell(col, row[col])}
                                                </td>
                                            ))}
                                        </tr>
                                    );
                                })}
                            </tbody>
                        </table>
                    </div>
                    <div className="pnl-foot">
                        <span className="tb-item">Page {safePage} of {totalPages}</span>
                        <div className="btn-row">
                            <button className="btn btn-g" disabled={safePage <= 1} onClick={() => setPage(safePage - 1)}>Previous</button>
                            <button className="btn btn-g" disabled={safePage >= totalPages} onClick={() => setPage(safePage + 1)}>Next</button>
                        </div>
                    </div>
                </>
            )}
        </div>
    );
}
