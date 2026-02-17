/* ── Left Rail (Attio-style Navigation) ───────────────────── */
function LeftRail({
    expanded,
    onToggleExpanded,
    hasDataset,
    activeDrawer,
    onOpenDrawer,
    onCloseDrawer,
    session,
    runHistory,
    jumpQuery,
    onJumpQuery,
    onSelectRun,
}) {
    const history = runHistory || [];

    const nav = [
        { key: DrawerState.FILTERS, icon: <I.filter />, label: 'Qualification', hotkey: '⌘1' },
        { key: DrawerState.VALIDATION, icon: <I.globe />, label: 'Validation', hotkey: '⌘2' },
        { key: DrawerState.EXPORT, icon: <I.download />, label: 'Export', hotkey: '⌘3' },
    ];

    const query = String(jumpQuery || '').trim().toLowerCase();
    const filteredHistory = query
        ? history.filter(item => {
            const fileName = String(item?.fileName || '').toLowerCase();
            const stats = `${item?.qualifiedCount || 0} ${item?.removedCount || 0}`.toLowerCase();
            return fileName.includes(query) || stats.includes(query);
        })
        : history;

    const togglePanel = (panelKey) => {
        if (!hasDataset) return;
        if (activeDrawer === panelKey) onCloseDrawer();
        else onOpenDrawer(panelKey);
    };

    return (
        <div className={`rail-shell ${expanded ? 'expanded' : 'collapsed'}`}>
            <div className="rail-top">
                <button className="rail-brand" type="button" aria-label="Kennel workspace home">
                    <span className="rail-mark"><I.dog /></span>
                    {expanded && (
                        <span className="rail-brand-text">
                            <strong>Kennel</strong>
                            <small>Hound Suite</small>
                        </span>
                    )}
                </button>
                <button
                    className="rail-toggle"
                    type="button"
                    onClick={onToggleExpanded}
                    aria-label={expanded ? 'Collapse rail' : 'Expand rail'}
                >
                    <I.chevRight style={{ transform: expanded ? 'rotate(180deg)' : 'none' }} />
                </button>
            </div>

            {expanded && (
                <div className="rail-profile">
                    <div className="rail-profile-name">Hound GTM Ops</div>
                    <div className="rail-profile-sub">Live workspace</div>
                </div>
            )}

            {expanded && (
                <div className="rail-jump">
                    <I.search />
                    <input
                        type="text"
                        value={jumpQuery || ''}
                        onChange={event => onJumpQuery(event.target.value)}
                        placeholder="Jump to run"
                        aria-label="Jump to run"
                    />
                    <kbd>⌘K</kbd>
                </div>
            )}

            <div className="rail-grow">
                {expanded && <div className="rail-title">Workspace</div>}
                <nav className="rail-nav" aria-label="Workspace tools">
                    {nav.map(item => (
                        <button
                            key={item.key}
                            type="button"
                            className={`rail-item ${activeDrawer === item.key ? 'active' : ''}`}
                            onClick={() => togglePanel(item.key)}
                            disabled={!hasDataset}
                            aria-pressed={activeDrawer === item.key}
                        >
                            {item.icon}
                            {expanded && (
                                <>
                                    <span>{item.label}</span>
                                    <kbd>{item.hotkey}</kbd>
                                </>
                            )}
                        </button>
                    ))}
                </nav>

                {expanded && (
                    <>
                        <div className="rail-title mt-section">Recent runs</div>
                        <div className="rail-history">
                            {filteredHistory.length === 0 && (
                                <div className="rail-empty">Run history appears here after qualification.</div>
                            )}
                            {filteredHistory.map(item => (
                                <button
                                    key={item.id}
                                    type="button"
                                    className="rail-history-item"
                                    onClick={() => onSelectRun?.(item)}
                                >
                                    <span className="rail-history-title" title={item.fileName}>{item.fileName}</span>
                                    <small>
                                        {(item.qualifiedCount || 0).toLocaleString()} qualified · {(item.removedCount || 0).toLocaleString()} removed
                                    </small>
                                </button>
                            ))}
                        </div>
                    </>
                )}
            </div>

            {expanded && hasDataset && (
                <footer className="rail-footer">
                    <div className="rail-meta-links">
                        <button type="button" className="rail-link">{session?.totalRows?.toLocaleString?.() || 0} rows</button>
                        <button type="button" className="rail-link">{session?.columns?.length || 0} columns</button>
                        {session?.dedupe?.enabled && <button type="button" className="rail-link">HubSpot dedupe on</button>}
                    </div>
                </footer>
            )}
        </div>
    );
}
