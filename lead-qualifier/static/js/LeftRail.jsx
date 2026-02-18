/* ── Left Rail (Attio-style Navigation) ───────────────────── */
function LeftRail({
    expanded,
    onToggleExpanded,
    hasDataset,
    session,
    jumpQuery,
    onJumpQuery,
}) {
    const sourceNames = Array.isArray(session?.fileNames) ? session.fileNames.filter(Boolean) : [];
    const sourceSummary = sourceNames.length > 1 ? `${sourceNames.length} files` : (sourceNames[0] || session?.fileName || 'No dataset');

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
                {expanded && <div className="rail-title">Tables</div>}
                <nav className="rail-nav" aria-label="Workspace tools">
                    <button
                        type="button"
                        className="rail-item active"
                        disabled={!hasDataset}
                        aria-current="page"
                    >
                        <I.table />
                        {expanded && <span title={sourceSummary}>{sourceSummary}</span>}
                    </button>
                </nav>
            </div>
        </div>
    );
}
