/* ── Command Header (Hybrid view filters + actions) ───────── */
function CommandHeader({
    session,
    runSummary,
    estimate,
    drawerState,
    canRun,
    disabledReason,
    isUnsaved,
    loading,
    loadMsg,
    runProgress,
    scrapeProgress,
    onOpenDrawer,
    onCloseDrawer,
    onRun,
    onPauseRun,
    onResumeRun,
    onFinishRun,
    onExport,
    onOpenImportModal,
    viewSearch,
    onViewSearch,
    viewFilters,
    onAddViewFilter,
    onUpdateViewFilter,
    onRemoveViewFilter,
    onClearViewFilters,
    onPromoteViewFilters,
    viewSort,
    onViewSort,
    savedViews,
    activeViewId,
    onSelectView,
    onCreateView,
    onSaveActiveView,
    onDeleteActiveView,
    totalColumnCount,
    visibleColumnCount,
}) {
    const hasDataset = !!session?.sessionId;
    const hasResults = !!runSummary;
    const runStatus = String(runProgress?.status || '');
    const scrapeStatus = String(scrapeProgress?.status || '');
    const isRunningLike = runStatus === 'running' || runStatus === 'pausing';
    const isPaused = runStatus === 'paused';
    const isScraping = scrapeStatus === 'running';
    const useProgressCounts = (isRunningLike || isPaused || (!hasResults && runStatus && runStatus !== 'idle'));

    const statusQualified = useProgressCounts
        ? (runProgress?.qualifiedCount || 0)
        : hasResults
        ? (runSummary?.qualifiedCount || 0)
        : (estimate?.estimatedQualifiedCount || 0);
    const statusRemoved = useProgressCounts
        ? (runProgress?.removedCount || 0)
        : hasResults
        ? (runSummary?.removedCount || 0)
        : (estimate?.estimatedRemovedCount || 0);

    const [pendingField, setPendingField] = useState('');
    const [pendingOp, setPendingOp] = useState('contains');
    const [pendingValue, setPendingValue] = useState('');

    const columns = session?.columns || [];
    const customViews = savedViews || [];
    const hasCustomActiveView = !!activeViewId && activeViewId !== 'default';
    const activeViewLabel = hasCustomActiveView
        ? (customViews.find(view => view.id === activeViewId)?.name || 'Saved view')
        : 'Default view';
    const columnCount = totalColumnCount || session?.columns?.length || 0;
    const shownColumns = Number.isFinite(visibleColumnCount) ? visibleColumnCount : columnCount;
    const columnButtonLabel = (columnCount > 0 && shownColumns > 0 && shownColumns !== columnCount)
        ? `${shownColumns}/${columnCount} columns`
        : `${columnCount} columns`;
    const filterSummaryLabel = !viewFilters.length ? 'No filters' : `${viewFilters.length} filters`;

    const canAddPending = !!pendingField && (
        pendingOp === 'is_empty' || pendingOp === 'is_not_empty' || String(pendingValue || '').trim()
    );

    const submitPendingFilter = () => {
        if (!canAddPending) return;
        onAddViewFilter({
            field: pendingField,
            op: pendingOp,
            value: String(pendingValue || '').trim(),
        });
        setPendingField('');
        setPendingOp('contains');
        setPendingValue('');
    };

    const filterLabel = (filter) => {
        const op = VIEW_FILTER_OP_LABEL[filter.op] || filter.op;
        const showValue = !(filter.op === 'is_empty' || filter.op === 'is_not_empty');
        return `${filter.field} ${op}${showValue && filter.value ? ` ${filter.value}` : ''}`;
    };

    const editFilter = (filter) => {
        if (typeof onUpdateViewFilter !== 'function') return;
        const field = typeof window !== 'undefined'
            ? window.prompt('Edit filter field', String(filter.field || ''))
            : String(filter.field || '');
        if (field === null) return;
        const op = typeof window !== 'undefined'
            ? window.prompt('Edit operator (contains, equals, not_equals, is_empty, is_not_empty, before, after)', String(filter.op || 'contains'))
            : String(filter.op || 'contains');
        if (op === null) return;
        const normalizedOp = String(op || '').trim() || 'contains';
        let value = String(filter.value || '');
        if (!['is_empty', 'is_not_empty'].includes(normalizedOp)) {
            const nextValue = typeof window !== 'undefined'
                ? window.prompt('Edit filter value', value)
                : value;
            if (nextValue === null) return;
            value = String(nextValue || '');
        } else {
            value = '';
        }
        onUpdateViewFilter(filter.id, {
            field: String(field || '').trim(),
            op: normalizedOp,
            value,
        });
    };

    const toggleDrawer = (panelKey) => {
        if (!hasDataset && panelKey !== DrawerState.ACTIVITY) return;
        if (drawerState === panelKey) onCloseDrawer();
        else onOpenDrawer(panelKey);
    };

    const clearSort = () => onViewSort({ column: null, direction: null });
    const setSortColumn = (column) => onViewSort({
        column: column || null,
        direction: column ? (viewSort?.direction || 'asc') : null,
    });
    const setSortDirection = (direction) => onViewSort({
        column: viewSort?.column || null,
        direction: viewSort?.column ? (direction || 'asc') : null,
    });

    const closeMenu = (event) => {
        const details = event?.currentTarget?.closest('details');
        if (details) details.open = false;
    };

    const selectViewFromMenu = (viewId, event) => {
        onSelectView(viewId);
        closeMenu(event);
    };

    const saveViewFromMenu = (event) => {
        onSaveActiveView();
        closeMenu(event);
    };

    const createViewFromMenu = (event) => {
        onCreateView();
        closeMenu(event);
    };

    const deleteViewFromMenu = (event) => {
        onDeleteActiveView();
        closeMenu(event);
    };

    return (
        <div className="cmd-shell">
            <div className="cmd-breadcrumbs" aria-label="Workspace breadcrumb">
                <div className="cmd-breadcrumbs-left">
                    <span>All Files</span>
                    <I.chevRight />
                    <span>Untitled workbook</span>
                    <I.chevRight />
                    <span className="crumb-current">{hasDataset ? (session?.fileName || 'Current table') : 'Table'}</span>
                    {hasDataset && (
                        <div className="crumb-metrics" aria-label="Table metadata">
                            <span className="crumb-metric-item">{(session?.totalRows || 0).toLocaleString()} rows</span>
                            <span className="crumb-metric-item">{columnCount.toLocaleString()} columns</span>
                            <span className="crumb-metric-item metric-good">{statusQualified.toLocaleString()} qualified</span>
                            <span className="crumb-metric-item metric-bad">{statusRemoved.toLocaleString()} removed</span>
                        </div>
                    )}
                </div>
                <div className="cmd-breadcrumbs-right">
                    <button className="btn btn-t btn-header-chip" type="button">Credits</button>
                    <button className="profile-pill" type="button">
                        <span className="profile-avatar">U</span>
                        <span className="profile-copy">
                            <strong>Workspace user</strong>
                            <small>Kennel</small>
                        </span>
                    </button>
                </div>
            </div>

            <div className="cmd-row cmd-row-toolbar">
                <div className="cmd-view-controls">
                    <button className="btn btn-g btn-auto toolbar-pill toolbar-pill-strong" type="button" disabled={!hasDataset}>
                        <I.refresh /> Auto-run
                    </button>

                    <details className="menu-wrap view-menu">
                        <summary className="btn btn-g toolbar-pill view-menu-trigger" aria-label="View settings and saved views" role="button">
                            <span className="view-menu-label">{activeViewLabel}</span>
                            <I.chevDown />
                        </summary>
                        <div className="menu-panel view-menu-panel">
                            <div className="view-menu-list">
                                <button
                                    className={`menu-item view-option ${!hasCustomActiveView ? 'is-active' : ''}`}
                                    type="button"
                                    onClick={e => selectViewFromMenu('default', e)}
                                >
                                    Default view
                                </button>
                                {customViews.map(view => (
                                    <button
                                        key={view.id}
                                        className={`menu-item view-option ${activeViewId === view.id ? 'is-active' : ''}`}
                                        type="button"
                                        onClick={e => selectViewFromMenu(view.id, e)}
                                    >
                                        {view.name}
                                    </button>
                                ))}
                            </div>
                            <div className="view-menu-sep" />
                            <button className="menu-item" type="button" onClick={saveViewFromMenu} disabled={!hasDataset}>
                                <I.check /> Save current view
                            </button>
                            <button className="menu-item" type="button" onClick={createViewFromMenu} disabled={!hasDataset}>
                                <I.plus /> Save as new view
                            </button>
                            <button className="menu-item" type="button" onClick={deleteViewFromMenu} disabled={!hasDataset || !hasCustomActiveView}>
                                <I.x /> Delete current view
                            </button>
                        </div>
                    </details>

                    <button className="btn btn-g toolbar-pill" type="button" onClick={() => toggleDrawer(DrawerState.EXPORT)} disabled={!hasDataset}>
                        <I.columns /> {columnButtonLabel}
                    </button>

                    <details className="menu-wrap filter-builder" onToggle={(event) => {
                        if (event.currentTarget.open && !pendingField && columns[0]?.name) setPendingField(columns[0].name);
                    }}>
                        <summary className="btn btn-g toolbar-pill" aria-label="Filters" role="button">
                            <I.filter /> {filterSummaryLabel}
                        </summary>
                        <div className="menu-panel filter-panel">
                            <label className="filter-form-field">
                                <span>Field</span>
                                <select value={pendingField} onChange={e => setPendingField(e.target.value)}>
                                    <option value="">Select field...</option>
                                    {columns.map(col => <option key={col.name} value={col.name}>{col.name}</option>)}
                                </select>
                            </label>
                            <label className="filter-form-field">
                                <span>Operator</span>
                                <select value={pendingOp} onChange={e => setPendingOp(e.target.value)}>
                                    {VIEW_FILTER_OPS.map(op => <option key={op.value} value={op.value}>{op.label}</option>)}
                                </select>
                            </label>
                            {!['is_empty', 'is_not_empty'].includes(pendingOp) && (
                                <label className="filter-form-field">
                                    <span>Value</span>
                                    <input
                                        type={pendingOp === 'before' || pendingOp === 'after' ? 'date' : 'text'}
                                        value={pendingValue}
                                        onChange={e => setPendingValue(e.target.value)}
                                        placeholder="Value"
                                    />
                                </label>
                            )}
                            <button type="button" className="btn btn-p" onClick={submitPendingFilter} disabled={!hasDataset || !canAddPending}>
                                Add filter
                            </button>
                        </div>
                    </details>

                    <details className="menu-wrap sort-builder">
                        <summary className="btn btn-g toolbar-pill" aria-label="Sort rows" role="button">
                            <I.arrowDown /> Sort
                        </summary>
                        <div className="menu-panel filter-panel">
                            <label className="filter-form-field">
                                <span>Column</span>
                                <select value={viewSort?.column || ''} onChange={e => setSortColumn(e.target.value)} disabled={!hasDataset}>
                                    <option value="">No sort</option>
                                    {columns.map(col => <option key={col.name} value={col.name}>{col.name}</option>)}
                                </select>
                            </label>
                            <label className="filter-form-field">
                                <span>Direction</span>
                                <select value={viewSort?.direction || 'asc'} onChange={e => setSortDirection(e.target.value)} disabled={!hasDataset || !viewSort?.column}>
                                    <option value="asc">Ascending</option>
                                    <option value="desc">Descending</option>
                                </select>
                            </label>
                            <button type="button" className="btn btn-t" onClick={clearSort} disabled={!viewSort?.column}>
                                Clear sort
                            </button>
                        </div>
                    </details>
                    <div className="search-wrap cmd-search toolbar-pill">
                        <I.search />
                        <input
                            type="text"
                            className="search-input"
                            value={viewSearch}
                            onChange={e => onViewSearch(e.target.value)}
                            placeholder="Search"
                            aria-label="Search"
                            disabled={!hasDataset}
                        />
                    </div>
                </div>

                <div className="cmd-right-actions">
                    <button className={`btn btn-g toolbar-pill ${drawerState === DrawerState.FILTERS ? 'is-active' : ''}`} type="button" onClick={() => toggleDrawer(DrawerState.FILTERS)} disabled={!hasDataset}>
                        <I.filter /> Qualification
                    </button>
                    <button className={`btn btn-g toolbar-pill ${drawerState === DrawerState.VALIDATION ? 'is-active' : ''}`} type="button" onClick={() => toggleDrawer(DrawerState.VALIDATION)} disabled={!hasDataset}>
                        <I.globe /> Validation
                    </button>
                    <button className={`btn btn-g toolbar-pill ${drawerState === DrawerState.EXPORT ? 'is-active' : ''}`} type="button" onClick={() => toggleDrawer(DrawerState.EXPORT)} disabled={!hasDataset}>
                        <I.download /> Export
                    </button>

                    <details className="menu-wrap action-builder">
                        <summary className="btn btn-g toolbar-pill" aria-label="Actions menu" role="button">
                            <I.settings /> Actions
                        </summary>
                        <div className="menu-panel action-panel">
                            <button className="menu-item" type="button" onClick={onOpenImportModal}>
                                <I.upload /> {hasDataset ? 'Replace data' : 'Import data'}
                            </button>
                            <button className="menu-item" type="button" onClick={() => toggleDrawer(DrawerState.EXPORT)} disabled={!hasDataset}>
                                <I.columns /> Manage columns
                            </button>
                            <button className="menu-item" type="button" onClick={onExport} disabled={!hasDataset || loading}>
                                <I.download /> Export now
                            </button>
                            <button
                                className="menu-item"
                                type="button"
                                onClick={onPromoteViewFilters}
                                disabled={!hasDataset || !viewFilters.length}
                            >
                                <I.filter /> Add view filters to qualification rules
                            </button>
                            <button className="menu-item" type="button" onClick={() => toggleDrawer(DrawerState.ROW_INSPECTOR)} disabled={!hasDataset}>
                                <I.table /> Row inspector
                            </button>
                            <button className="menu-item" type="button" onClick={() => toggleDrawer(DrawerState.ACTIVITY)}>
                                <I.table /> Activity
                            </button>
                        </div>
                    </details>

                    <button className="btn btn-p" type="button" onClick={onRun} disabled={!canRun || isRunningLike || isScraping}>
                        {isRunningLike && <span className="spinner" />} Run
                    </button>
                    {isRunningLike && (
                        <button className="btn btn-g" type="button" onClick={onPauseRun}>
                            Pause
                        </button>
                    )}
                    {isPaused && (
                        <>
                            <button className="btn btn-g" type="button" onClick={onResumeRun}>
                                Resume
                            </button>
                            <button className="btn btn-g" type="button" onClick={onFinishRun}>
                                Finish
                            </button>
                        </>
                    )}
                </div>
            </div>

            {!!viewFilters.length && (
                <div className="cmd-row cmd-row-chips">
                    <div className="chips-block" aria-label="View filters">
                        <div className="chips-wrap">
                            {viewFilters.map(filter => (
                                <span key={filter.id} className="chip">
                                    <span>{filterLabel(filter)}</span>
                                    <button type="button" onClick={() => editFilter(filter)} aria-label="Edit view filter">
                                        <I.edit />
                                    </button>
                                    <button type="button" onClick={() => onRemoveViewFilter(filter.id)} aria-label="Remove view filter">
                                        <I.x />
                                    </button>
                                </span>
                            ))}
                        </div>
                    </div>
                    <div className="cmd-row-actions-end">
                        <button className="btn btn-t" type="button" onClick={onClearViewFilters}>Clear filters</button>
                    </div>
                </div>
            )}

            {!canRun && hasDataset && !loading && <div className="cmd-note warn"><I.info /> {disabledReason}</div>}
            {isUnsaved && hasDataset && <div className="cmd-note"><I.info /> Unrun rule changes in qualification settings.</div>}
            {isRunningLike && <div className="cmd-note"><span className="spinner" /> {runProgress?.message || 'Running qualification rules...'}</div>}
            {isPaused && <div className="cmd-note warn"><I.info /> {runProgress?.message || 'Run is paused.'}</div>}
            {isScraping && <div className="cmd-note"><span className="spinner" /> {scrapeProgress?.message || 'Scraping homepages...'}</div>}
            {loading && loadMsg && <div className="cmd-note"><span className="spinner" /> {loadMsg}</div>}
        </div>
    );
}
