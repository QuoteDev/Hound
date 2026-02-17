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
    onOpenDrawer,
    onCloseDrawer,
    onRun,
    onExport,
    onOpenImportModal,
    viewSearch,
    onViewSearch,
    viewFilters,
    onAddViewFilter,
    onRemoveViewFilter,
    onClearViewFilters,
    onPromoteViewFilters,
}) {
    const hasDataset = !!session?.sessionId;
    const hasResults = !!runSummary;

    const statusQualified = hasResults
        ? (runSummary?.qualifiedCount || 0)
        : (estimate?.estimatedQualifiedCount || 0);
    const statusRemoved = hasResults
        ? (runSummary?.removedCount || 0)
        : (estimate?.estimatedRemovedCount || 0);

    const panelOptions = [
        { key: DrawerState.FILTERS, label: 'Qualification' },
        { key: DrawerState.VALIDATION, label: 'Validation' },
        { key: DrawerState.EXPORT, label: 'Export' },
    ];

    const [pendingField, setPendingField] = useState('');
    const [pendingOp, setPendingOp] = useState('contains');
    const [pendingValue, setPendingValue] = useState('');

    const columns = session?.columns || [];

    const onPanelClick = (panelKey) => {
        if (!hasDataset) return;
        if (drawerState === panelKey) onCloseDrawer();
        else onOpenDrawer(panelKey);
    };

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

    return (
        <div className="cmd-shell">
            <div className="cmd-row cmd-row-top">
                <div className="cmd-dataset">
                    <div className="cmd-overline">{hasResults ? 'Review mode' : 'Live workspace'}</div>
                    <div className="cmd-title-row">
                        <h1 className="cmd-title">{hasDataset ? (session?.fileName || 'Dataset loaded') : 'Import required'}</h1>
                        <span className="cmd-mode">{hasDataset ? (hasResults ? 'Post-run' : 'Preview') : 'Import required'}</span>
                    </div>
                    {hasDataset && (
                        <div className="cmd-meta">
                            <span><I.rows /> {(session?.totalRows || 0).toLocaleString()} rows</span>
                            <span><I.columns /> {(session?.columns?.length || 0).toLocaleString()} columns</span>
                            <span className="metric-good">{statusQualified.toLocaleString()} qualified</span>
                            <span className="metric-bad">{statusRemoved.toLocaleString()} removed</span>
                        </div>
                    )}
                </div>

                <div className="cmd-toolbar">
                    <button type="button" className="btn btn-t" aria-label="More workspace actions"><I.settings /></button>
                </div>
            </div>

            <div className="cmd-row cmd-row-filters">
                <div className="cmd-view-controls">
                    <div className="view-select-wrap">
                        <select className="view-select" defaultValue="default" aria-label="Saved view">
                            <option value="default">Default view</option>
                        </select>
                    </div>

                    <div className="search-wrap cmd-search">
                        <I.search />
                        <input
                            type="text"
                            className="search-input"
                            value={viewSearch}
                            onChange={e => onViewSearch(e.target.value)}
                            placeholder="Search rows"
                            aria-label="Search rows"
                            disabled={!hasDataset}
                        />
                    </div>

                    <details className="menu-wrap filter-builder" onToggle={(event) => {
                        if (event.currentTarget.open && !pendingField && columns[0]?.name) setPendingField(columns[0].name);
                    }}>
                        <summary className="btn btn-g" aria-label="Add view filter" role="button">
                            <I.plus /> Add filter
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
                                Add view filter
                            </button>
                        </div>
                    </details>
                </div>

                <div className="cmd-right-actions">
                    <button
                        className="btn btn-g"
                        type="button"
                        onClick={onPromoteViewFilters}
                        disabled={!hasDataset || !viewFilters.length}
                    >
                        Save to qualification
                    </button>
                    <button className="btn btn-g" type="button" onClick={onOpenImportModal}>
                        <I.upload /> {hasDataset ? 'Replace dataset' : 'Import dataset'}
                    </button>
                    <button className="btn btn-g" type="button" onClick={onExport} disabled={!hasDataset || loading}>
                        <I.download /> Export
                    </button>
                    <button className="btn btn-p" type="button" onClick={onRun} disabled={!canRun || runProgress?.status === 'running'}>
                        {runProgress?.status === 'running' && <span className="spinner" />} Run qualification
                    </button>
                </div>
            </div>

            <div className="cmd-row cmd-row-chips">
                <div className="chips-block" aria-label="View filters">
                    <span className="section-label">View filters</span>
                    <div className="chips-wrap">
                        {!viewFilters.length && <span className="chip chip-muted">No view filters</span>}
                        {viewFilters.map(filter => (
                            <span key={filter.id} className="chip">
                                <span>{filterLabel(filter)}</span>
                                <button type="button" onClick={() => onRemoveViewFilter(filter.id)} aria-label="Remove view filter">
                                    <I.x />
                                </button>
                            </span>
                        ))}
                    </div>
                </div>

                <div className="cmd-row-actions-end">
                    {!!viewFilters.length && (
                        <button className="btn btn-t" type="button" onClick={onClearViewFilters}>Clear view filters</button>
                    )}
                    <div className="cmd-panel-switch" role="tablist" aria-label="Context panels">
                        {panelOptions.map(option => (
                            <button
                                key={option.key}
                                type="button"
                                className={`cmd-panel-btn ${drawerState === option.key ? 'is-active' : ''}`}
                                onClick={() => onPanelClick(option.key)}
                                disabled={!hasDataset}
                                role="tab"
                                aria-selected={drawerState === option.key}
                            >
                                {option.label}
                            </button>
                        ))}
                    </div>
                </div>
            </div>

            {!canRun && hasDataset && !loading && <div className="cmd-note warn"><I.info /> {disabledReason}</div>}
            {isUnsaved && hasDataset && <div className="cmd-note"><I.info /> Unrun qualification changes in drawer filters.</div>}
            {runProgress?.status === 'running' && <div className="cmd-note"><span className="spinner" /> {runProgress?.message || 'Running qualification...'}</div>}
            {loading && loadMsg && <div className="cmd-note"><span className="spinner" /> {loadMsg}</div>}
        </div>
    );
}
