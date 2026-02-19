/* ── Data Canvas ───────────────────────────────────────────── */
function DataCanvas({
    session,
    runSummary,
    runProgress,
    viewSearch,
    viewSort,
    onViewSort,
    viewFilters,
    onAddViewFilter,
    selectedRowId,
    onSelectRow,
    columnPrefs,
    onHideColumn,
    onShowAllColumns,
    onRenameColumn,
    onFormatColumn,
    onBulkExport,
    onBulkStatus,
}) {
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(100);
    const [selectedCell, setSelectedCell] = useState(null);
    const [checkedRows, setCheckedRows] = useState({});
    const [focusedRowIndex, setFocusedRowIndex] = useState(-1);
    const [showShortcutHelp, setShowShortcutHelp] = useState(false);
    const headerCheckboxRef = useRef(null);
    const tableRef = useRef(null);

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
    const hiddenColumns = columnPrefs?.hidden || {};
    const columnLabels = columnPrefs?.labels || {};
    const columnFormats = columnPrefs?.formats || {};
    const visibleColumns = columns.filter(col => !hiddenColumns[col] && col !== '_score_breakdown');
    const hiddenCount = columns.length - visibleColumns.length;

    const profileByName = useMemo(() => {
        const out = {};
        for (const profile of (session?.columnProfiles || [])) {
            if (profile?.name) out[profile.name] = profile;
        }
        return out;
    }, [session?.columnProfiles]);

    useEffect(() => {
        setPage(1);
    }, [viewSearch, viewFilters, viewSort?.column, viewSort?.direction, pageSize]);

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
    }, [isReviewData, session?.sessionId, session?.totalRows, columns.length, page, pageSize, viewSearch, viewSort?.column, viewSort?.direction, viewFilters, runProgressSignal]);

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

    useEffect(() => {
        setCheckedRows({});
    }, [viewSearch, JSON.stringify(viewFilters), viewSort?.column, viewSort?.direction, page, pageSize, isReviewData]);

    useEffect(() => {
        const handler = (e) => {
            // Don't intercept when typing in inputs
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA' || e.target.tagName === 'SELECT') return;

            if (e.key === '?') {
                e.preventDefault();
                setShowShortcutHelp(prev => !prev);
                return;
            }

            if (!currentRows.length) return;

            if (e.key === 'ArrowDown' || e.key === 'j') {
                e.preventDefault();
                setFocusedRowIndex(prev => Math.min(prev + 1, currentRows.length - 1));
            } else if (e.key === 'ArrowUp' || e.key === 'k') {
                e.preventDefault();
                setFocusedRowIndex(prev => Math.max(prev - 1, 0));
            } else if (e.key === 'Enter' && focusedRowIndex >= 0 && focusedRowIndex < currentRows.length) {
                e.preventDefault();
                const row = currentRows[focusedRowIndex];
                const rowId = row._rowId ?? `${safePage}-${focusedRowIndex}`;
                onSelectRow({ ...row, _rowId: rowId }, { openInspector: true });
            } else if (e.key === 'q' && focusedRowIndex >= 0 && focusedRowIndex < currentRows.length) {
                const row = currentRows[focusedRowIndex];
                const rowId = row._rowId;
                if (rowId != null && onBulkStatus) onBulkStatus([rowId], 'qualified');
            } else if (e.key === 'x' && focusedRowIndex >= 0 && focusedRowIndex < currentRows.length) {
                const row = currentRows[focusedRowIndex];
                const rowId = row._rowId;
                if (rowId != null && onBulkStatus) onBulkStatus([rowId], 'removed_manual');
            }
        };
        window.addEventListener('keydown', handler);
        return () => window.removeEventListener('keydown', handler);
    }, [currentRows, focusedRowIndex, safePage, onSelectRow, onBulkStatus]);

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

    const displayColumnName = (col) => columnLabels[col] || col;
    const getColumnFormat = (col) => columnFormats[col] || 'auto';

    const handleHeaderFilter = (columnName) => {
        if (typeof onAddViewFilter !== 'function') return;
        const promptValue = typeof window !== 'undefined'
            ? window.prompt(`Filter "${displayColumnName(columnName)}" contains:`)
            : '';
        if (promptValue === null) return;
        onAddViewFilter({
            field: columnName,
            op: 'contains',
            value: String(promptValue || '').trim(),
        });
    };

    const handleHeaderHide = (columnName) => {
        if (typeof onHideColumn !== 'function') return;
        onHideColumn(columnName);
    };

    const handleHeaderRename = (columnName) => {
        if (typeof onRenameColumn !== 'function') return;
        const current = displayColumnName(columnName);
        const next = typeof window !== 'undefined'
            ? window.prompt(`Rename column "${current}"`, current)
            : current;
        if (next === null) return;
        onRenameColumn(columnName, next);
    };

    const handleFormatChange = (columnName, format) => {
        if (typeof onFormatColumn !== 'function') return;
        onFormatColumn(columnName, format);
    };

    const handleShowAllColumns = () => {
        if (typeof onShowAllColumns === 'function') onShowAllColumns();
    };

    const SortIcon = ({ col }) => {
        if (viewSort?.column !== col || !viewSort?.direction) return <span className="sort-arrow sort-idle"><I.chevDown /></span>;
        return <span className="sort-arrow sort-active">{viewSort.direction === 'asc' ? <I.arrowUp /> : <I.arrowDown />}</span>;
    };

    const statusMap = {
        processing: { label: 'Needs review', tone: 'review', icon: <I.info /> },
        qualified: { label: 'Qualified', tone: 'qualified', icon: <I.check /> },
        removed_filter: { label: 'Excluded', tone: 'excluded', icon: <I.x /> },
        removed_domain: { label: 'Excluded', tone: 'excluded', icon: <I.x /> },
        removed_hubspot: { label: 'Excluded', tone: 'excluded', icon: <I.x /> },
        removed_intra_dedupe: { label: 'Duplicate', tone: 'excluded', icon: <I.x /> },
        removed_manual: { label: 'Excluded', tone: 'excluded', icon: <I.x /> },
    };

    const rowIdsOnPage = currentRows.map((row, idx) => row._rowId ?? `${safePage}-${idx}`);
    const allOnPageChecked = rowIdsOnPage.length > 0 && rowIdsOnPage.every(rowId => !!checkedRows[rowId]);
    const someOnPageChecked = rowIdsOnPage.some(rowId => !!checkedRows[rowId]);

    useEffect(() => {
        if (!headerCheckboxRef.current) return;
        headerCheckboxRef.current.indeterminate = !allOnPageChecked && someOnPageChecked;
    }, [allOnPageChecked, someOnPageChecked]);

    const toggleRowChecked = (rowId, checked) => {
        setCheckedRows(curr => {
            const next = { ...curr };
            if (checked) next[rowId] = true;
            else delete next[rowId];
            return next;
        });
    };

    const togglePageChecked = (checked) => {
        setCheckedRows(curr => {
            const next = { ...curr };
            rowIdsOnPage.forEach(rowId => {
                if (checked) next[rowId] = true;
                else delete next[rowId];
            });
            return next;
        });
    };

    const checkedRowIds = Object.keys(checkedRows).filter(k => checkedRows[k]);
    const checkedCount = checkedRowIds.length;

    const handleBulkExport = () => {
        if (typeof onBulkExport === 'function') onBulkExport(checkedRowIds);
    };
    const handleBulkQualify = () => {
        if (typeof onBulkStatus === 'function') onBulkStatus(checkedRowIds, 'qualified');
        setCheckedRows({});
    };
    const handleBulkExclude = () => {
        if (typeof onBulkStatus === 'function') onBulkStatus(checkedRowIds, 'removed_manual');
        setCheckedRows({});
    };

    return (
        <div className="dc-shell">
            <div className="dc-header-row">
                <span className="tb-item">{rowsShownCount.toLocaleString()} shown</span>
                <span className="tb-item">{totalRowsCount.toLocaleString()} total</span>
                {hiddenCount > 0 && (
                    <button className="btn btn-t" type="button" onClick={handleShowAllColumns}>
                        Show {hiddenCount} hidden {hiddenCount === 1 ? 'column' : 'columns'}
                    </button>
                )}
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

            {someOnPageChecked && (
                <div className="bulk-action-bar">
                    <span className="bulk-count">{checkedCount} selected</span>
                    {isReviewData && <button className="btn btn-g" type="button" onClick={handleBulkExport}>Export selected</button>}
                    {isReviewData && <button className="btn btn-g" type="button" onClick={handleBulkQualify}>Mark qualified</button>}
                    {isReviewData && <button className="btn btn-g" type="button" onClick={handleBulkExclude}>Exclude selected</button>}
                    <button className="btn btn-t" type="button" onClick={() => setCheckedRows({})}>Clear selection</button>
                </div>
            )}

            {!isReviewData && previewMeta.error && (
                <div className="inline-msg err"><I.alertTri /> {previewMeta.error}</div>
            )}

            {columns.length === 0 ? (
                <div className="empty">
                    <I.file style={{ width: 28, height: 28 }} />
                    <h3>No dataset loaded</h3>
                    <p>Import a dataset to begin qualification and table exploration.</p>
                </div>
            ) : visibleColumns.length === 0 ? (
                <div className="empty">
                    <I.columns style={{ width: 28, height: 28 }} />
                    <h3>All columns are hidden</h3>
                    <p>Use Show columns to restore the table layout.</p>
                    <button className="btn btn-g" type="button" onClick={handleShowAllColumns}>Show columns</button>
                </div>
            ) : (
                <>
                    <div className="tw results-table-wrap">
                        <table>
                            <thead>
                                <tr>
                                    <th style={{ width: '36px' }} className="th-check">
                                        <input
                                            ref={headerCheckboxRef}
                                            type="checkbox"
                                            checked={allOnPageChecked}
                                            onChange={e => togglePageChecked(e.target.checked)}
                                            aria-label="Select all rows on page"
                                        />
                                    </th>
                                    <th style={{ width: '56px' }}>#</th>
                                    <th style={{ width: '108px' }}>Status</th>
                                    {visibleColumns.map(col => (
                                        <th key={col} className="th-sort">
                                            <div className="th-head">
                                                <button type="button" className="th-btn" onClick={() => toggleSort(col)} aria-label={`Sort by ${displayColumnName(col)}`}>
                                                    <span className="th-label"><FieldType name={col} inferredType={profileByName[col]?.inferredType} /> {displayColumnName(col)}</span>
                                                    <SortIcon col={col} />
                                                </button>
                                                <details className="menu-wrap th-menu-wrap">
                                                    <summary className="th-menu-btn" aria-label={`Column menu for ${displayColumnName(col)}`}>
                                                        <I.moreH />
                                                    </summary>
                                                    <div className="menu-panel th-menu-panel">
                                                        <button className="menu-item" type="button" onClick={() => onViewSort({ column: col, direction: 'asc' })}>
                                                            <I.arrowUp /> Sort ascending
                                                        </button>
                                                        <button className="menu-item" type="button" onClick={() => onViewSort({ column: col, direction: 'desc' })}>
                                                            <I.arrowDown /> Sort descending
                                                        </button>
                                                        <button className="menu-item" type="button" onClick={() => handleHeaderFilter(col)}>
                                                            <I.filter /> Filter values
                                                        </button>
                                                        <button className="menu-item" type="button" onClick={() => handleHeaderHide(col)}>
                                                            <I.x /> Hide column
                                                        </button>
                                                        <button className="menu-item" type="button" onClick={() => handleHeaderRename(col)}>
                                                            <I.edit /> Rename column
                                                        </button>
                                                        <label className="th-menu-format">
                                                            <span>Format</span>
                                                            <select value={getColumnFormat(col)} onChange={e => handleFormatChange(col, e.target.value)}>
                                                                <option value="auto">Auto</option>
                                                                <option value="text">Text</option>
                                                                <option value="number">Number</option>
                                                                <option value="currency">Currency</option>
                                                                <option value="url">URL</option>
                                                            </select>
                                                        </label>
                                                    </div>
                                                </details>
                                            </div>
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                <tr className="meta-row" aria-hidden="true">
                                    <td className="row-check" />
                                    <td className="rn rn-meta">%</td>
                                    <td>
                                        <span className="meta-status-chip">
                                            <I.check />
                                            <span>Up to date</span>
                                        </span>
                                    </td>
                                    {visibleColumns.map(col => (
                                        <td key={`${col}-meta`}>
                                            <span className="meta-col-check"><I.check /></span>
                                        </td>
                                    ))}
                                </tr>
                                {currentRows.map((row, idx) => {
                                    const status = row._rowStatus || RowStatus.QUALIFIED;
                                    const rowId = row._rowId ?? `${safePage}-${idx}`;
                                    const statusMeta = statusMap[status] || { label: 'Error', tone: 'error', icon: <I.alertTri /> };
                                    return (
                                        <tr
                                            key={rowId}
                                            className={`row-${status} ${String(status || '').startsWith('removed_') ? 'row-removed' : ''} ${selectedRowId === rowId ? 'row-selected' : ''} ${focusedRowIndex === idx ? 'row-focused' : ''}`}
                                            onClick={() => onSelectRow({ ...row, _rowId: rowId }, { openInspector: false })}
                                            onDoubleClick={() => onSelectRow({ ...row, _rowId: rowId }, { openInspector: true })}
                                            onKeyDown={(event) => {
                                                if (event.key === 'Enter' || event.key === ' ') {
                                                    event.preventDefault();
                                                    onSelectRow({ ...row, _rowId: rowId }, { openInspector: false });
                                                }
                                            }}
                                            tabIndex={0}
                                        >
                                            <td className="row-check">
                                                <input
                                                    type="checkbox"
                                                    checked={!!checkedRows[rowId]}
                                                    onClick={event => event.stopPropagation()}
                                                    onChange={e => toggleRowChecked(rowId, e.target.checked)}
                                                    aria-label={`Select row ${pageStart + idx + 1}`}
                                                />
                                            </td>
                                            <td className="rn">{pageStart + idx + 1}</td>
                                            <td>
                                                <span className={`status-chip status-${statusMeta.tone}`}>
                                                    <span className="status-chip-icon">{statusMeta.icon}</span>
                                                    <span>{statusMeta.label}</span>
                                                </span>
                                            </td>
                                            {visibleColumns.map(col => {
                                                if (col === '_lead_score') {
                                                    const scoreVal = parseFloat(row[col]);
                                                    const hasScore = !isNaN(scoreVal);
                                                    const scoreStyle = hasScore
                                                        ? scoreVal >= 70
                                                            ? { background: '#e6f9e6', color: '#1a7a1a' }
                                                            : scoreVal >= 40
                                                                ? { background: '#fff8e1', color: '#8a6d00' }
                                                                : { background: '#fde8e8', color: '#c62828' }
                                                        : {};
                                                    return (
                                                        <td
                                                            key={col}
                                                            title={row[col]}
                                                            className={selectedCell?.rowId === rowId && selectedCell?.column === col ? 'cell-selected' : ''}
                                                            onClick={(event) => {
                                                                event.stopPropagation();
                                                                setSelectedCell({ rowId, column: col });
                                                                onSelectRow({ ...row, _rowId: rowId }, { openInspector: false });
                                                            }}
                                                        >
                                                            {hasScore
                                                                ? <span className="score-pill" style={{ ...scoreStyle, padding: '2px 8px', borderRadius: '10px', fontWeight: 600, fontSize: '12px', display: 'inline-block' }}>{Math.round(scoreVal)}</span>
                                                                : String(row[col] ?? '')}
                                                        </td>
                                                    );
                                                }
                                                return (
                                                <td
                                                    key={col}
                                                    title={row[col]}
                                                    className={`${selectedCell?.rowId === rowId && selectedCell?.column === col ? 'cell-selected' : ''} ${((getColumnFormat(col) === 'number' || getColumnFormat(col) === 'currency') || (getColumnFormat(col) === 'auto' && profileByName[col]?.inferredType === 'number')) ? 'cell-num' : ''}`}
                                                    onClick={(event) => {
                                                        event.stopPropagation();
                                                        setSelectedCell({ rowId, column: col });
                                                        onSelectRow({ ...row, _rowId: rowId }, { openInspector: false });
                                                    }}
                                                >
                                                    {renderTableCell(col, row[col], getColumnFormat(col))}
                                                </td>
                                                );
                                            })}
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
                    <div className="sheet-strip">
                        <div className="sheet-strip-left">
                            <button className="sheet-link" type="button">Overview</button>
                            <button className="sheet-tab active" type="button">{session?.fileName || 'Table'}</button>
                            <button className="sheet-add" type="button"><I.plus /> Add</button>
                        </div>
                        <div className="sheet-strip-right">
                            <span>{Math.max(1, Math.round(((pageStart + currentRows.length) / Math.max(1, rowsShownCount || totalRowsCount)) * 100))}% of table completed</span>
                            <button className="sheet-history" type="button">History</button>
                        </div>
                    </div>
                </>
            )}
            {showShortcutHelp && (
                <div className="shortcut-help-overlay" onClick={() => setShowShortcutHelp(false)}>
                    <div className="shortcut-help-card" onClick={e => e.stopPropagation()}>
                        <div className="shortcut-help-title">Keyboard Shortcuts</div>
                        <div className="shortcut-row"><kbd>↑</kbd> / <kbd>k</kbd> Move up</div>
                        <div className="shortcut-row"><kbd>↓</kbd> / <kbd>j</kbd> Move down</div>
                        <div className="shortcut-row"><kbd>Enter</kbd> Inspect row</div>
                        <div className="shortcut-row"><kbd>q</kbd> Qualify row</div>
                        <div className="shortcut-row"><kbd>x</kbd> Exclude row</div>
                        <div className="shortcut-row"><kbd>?</kbd> Toggle shortcuts</div>
                        <button className="btn btn-t mt12" onClick={() => setShowShortcutHelp(false)}>Close</button>
                    </div>
                </div>
            )}
        </div>
    );
}
