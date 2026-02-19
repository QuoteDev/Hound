/* ── Left Rail (Attio-style Navigation) ───────────────────── */
function LeftRail({
    expanded,
    onToggleExpanded,
    hasDataset,
    session,
    jumpQuery,
    onJumpQuery,
    onOpenImportModal,
    sessionList,
    activeSessionId,
    onSwitchSession,
    onDeleteSession,
    onRenameSession,
    onCreateBlank,
}) {
    const sourceNames = Array.isArray(session?.fileNames) ? session.fileNames.filter(Boolean) : [];
    const sourceSummary = sourceNames.length > 1 ? `${sourceNames.length} files` : (sourceNames[0] || session?.fileName || 'No dataset');

    const [showAddMenu, setShowAddMenu] = useState(false);
    const addMenuRef = useRef(null);

    useEffect(() => {
        if (!showAddMenu) return;
        const onClickOutside = (event) => {
            if (addMenuRef.current && !addMenuRef.current.contains(event.target)) {
                setShowAddMenu(false);
            }
        };
        const onEscape = (event) => {
            if (event.key === 'Escape') setShowAddMenu(false);
        };
        document.addEventListener('mousedown', onClickOutside);
        document.addEventListener('keydown', onEscape);
        return () => {
            document.removeEventListener('mousedown', onClickOutside);
            document.removeEventListener('keydown', onEscape);
        };
    }, [showAddMenu]);

    const handleImport = () => {
        setShowAddMenu(false);
        onOpenImportModal?.();
    };

    const handleNewBlank = () => {
        setShowAddMenu(false);
        const name = typeof window !== 'undefined'
            ? window.prompt('New table name', 'Untitled')
            : 'Untitled';
        if (name === null) return;
        const clean = String(name || '').trim();
        if (!clean) return;
        if (typeof onCreateBlank === 'function') {
            onCreateBlank(clean);
        } else {
            onOpenImportModal?.();
        }
    };

    const handleRename = (sid) => {
        if (typeof onRenameSession !== 'function') return;
        const item = (sessionList || []).find(s => s.sessionId === sid);
        const current = item?.fileName || 'Table';
        const next = typeof window !== 'undefined'
            ? window.prompt('Rename table', current)
            : null;
        if (next === null) return;
        const clean = String(next || '').trim();
        if (!clean) return;
        onRenameSession(sid, clean);
    };

    const handleDelete = (sid) => {
        if (typeof onDeleteSession !== 'function') return;
        const item = (sessionList || []).find(s => s.sessionId === sid);
        const label = item?.fileName || 'this table';
        if (typeof window !== 'undefined' && !window.confirm(`Delete "${label}"?`)) return;
        onDeleteSession(sid);
    };

    const query = String(jumpQuery || '').trim().toLowerCase();
    const items = (sessionList || []).filter(item => {
        if (!query) return true;
        const name = String(item?.fileName || '').toLowerCase();
        return name.includes(query);
    });

    return (
        <div className={`rail-shell ${expanded ? 'expanded' : 'collapsed'}`}>
            <div className="rail-top">
                <button className="rail-brand" type="button" aria-label="Kennel workspace home">
                    <span className="rail-mark"><I.dog /></span>
                    {expanded && (
                        <span className="rail-brand-text">
                            <strong>Workspace</strong>
                        </span>
                    )}
                </button>
                <button
                    className="rail-toggle"
                    type="button"
                    onClick={onToggleExpanded}
                    aria-label={expanded ? 'Collapse workspace switcher' : 'Expand workspace switcher'}
                >
                    <I.chevRight style={{ transform: expanded ? 'rotate(180deg)' : 'none' }} />
                </button>
            </div>

            {expanded && (
                <div className="rail-jump">
                    <I.search />
                    <input
                        type="text"
                        value={jumpQuery || ''}
                        onChange={event => onJumpQuery(event.target.value)}
                        placeholder="Find table"
                        aria-label="Find table"
                    />
                </div>
            )}

            <div className="rail-grow">
                {expanded && (
                    <div className="rail-title-row" ref={addMenuRef}>
                        <div className="rail-title">Tables</div>
                        <button
                            className={`rail-add-btn ${showAddMenu ? 'is-open' : ''}`}
                            type="button"
                            onClick={() => setShowAddMenu(!showAddMenu)}
                            aria-label="Add table"
                        >
                            <I.plus />
                        </button>
                        {showAddMenu && (
                            <div className="rail-add-menu">
                                <button className="rail-add-menu-item" type="button" onClick={handleImport}>
                                    <I.upload /> Import CSV / TSV
                                </button>
                                <button className="rail-add-menu-item" type="button" onClick={handleNewBlank}>
                                    <I.table /> New blank table
                                </button>
                            </div>
                        )}
                    </div>
                )}
                <nav className="rail-nav" aria-label="Workspace tools">
                    {items.length === 0 && !hasDataset && (
                        <div className="rail-empty">No tables yet.</div>
                    )}
                    {items.map(item => {
                        const isActive = item.sessionId === activeSessionId;
                        return (
                            <div key={item.sessionId} className="rail-item-wrap">
                                <button
                                    type="button"
                                    className={`rail-item ${isActive ? 'active' : ''}`}
                                    onClick={() => {
                                        if (!isActive && typeof onSwitchSession === 'function') {
                                            onSwitchSession(item.sessionId);
                                        }
                                    }}
                                    aria-current={isActive ? 'page' : undefined}
                                >
                                    <I.table />
                                    {expanded && <span title={item.fileName}>{item.fileName}</span>}
                                </button>
                                {expanded && (
                                    <details className="menu-wrap rail-ctx-wrap">
                                        <summary className="rail-ctx-btn" aria-label={`Options for ${item.fileName}`}>
                                            <I.moreH />
                                        </summary>
                                        <div className="menu-panel rail-ctx-menu">
                                            <button className="menu-item" type="button" onClick={() => handleRename(item.sessionId)}>
                                                <I.edit /> Rename
                                            </button>
                                            <button className="menu-item" type="button" onClick={() => handleDelete(item.sessionId)}>
                                                <I.x /> Delete
                                            </button>
                                        </div>
                                    </details>
                                )}
                            </div>
                        );
                    })}
                </nav>
            </div>
        </div>
    );
}
