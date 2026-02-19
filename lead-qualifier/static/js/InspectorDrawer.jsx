/* ── Row Inspector Content ─────────────────────────────────── */
function InspectorDrawer({ row }) {
    if (!row) {
        return (
            <div className="empty inspector-empty">
                <I.search style={{ width: 24, height: 24 }} />
                <h3>Select a row</h3>
                <p>Click a table row to inspect reason chain and field values.</p>
            </div>
        );
    }

    const keys = Object.keys(row).filter(k => !k.startsWith('_'));
    const reasons = row._rowReasons || [];
    const status = row._rowStatus || RowStatus.QUALIFIED;
    const isDisqualified = String(status).startsWith('removed_');
    const primaryReason = reasons[0] || '';
    const dedupeMatch = row._dedupeMatch && typeof row._dedupeMatch === 'object' ? row._dedupeMatch : null;

    const disqualificationSummary = (() => {
        if (!isDisqualified) return '';
        if (status === RowStatus.REMOVED_FILTER) return 'This row was removed because it did not satisfy the configured qualification filters.';
        if (status === RowStatus.REMOVED_DOMAIN) return 'This row was removed by domain/homepage/TLD validation checks.';
        if (status === RowStatus.REMOVED_HUBSPOT) return 'This row was removed because it matched an existing record in attached dedupe files.';
        return 'This row was removed during qualification.';
    })();

    return (
        <div className="inspector-shell">
            <div className="inspector-meta">
                <span className={`status-chip ${status}`}>{RowStatusLabel(status)}</span>
                <span className="badge badge-m">Row {row._rowId ?? '-'}</span>
            </div>

            <section className="inspector-section">
                <div className="mini-card-title">{isDisqualified ? 'Why disqualified' : 'Qualification summary'}</div>
                <div className="inline-help">
                    <I.info />
                    <span>{isDisqualified ? disqualificationSummary : 'This row is currently qualified.'}</span>
                </div>
                {primaryReason && (
                    <div className="mini-row mt12">
                        <span>Primary reason</span>
                        <span>{RowReason(primaryReason)}</span>
                    </div>
                )}
                {status === RowStatus.REMOVED_HUBSPOT && dedupeMatch && (
                    <>
                        <div className="mini-row mt12">
                            <span>Matched key type</span>
                            <span>{String(dedupeMatch.keyType || '').toUpperCase() || '-'}</span>
                        </div>
                        <div className="mini-row">
                            <span>Source field</span>
                            <span>{dedupeMatch.sourceColumn || '-'}</span>
                        </div>
                        <div className="mini-row">
                            <span>Source value</span>
                            <span>{dedupeMatch.sourceValue || '-'}</span>
                        </div>
                        <div className="mini-row">
                            <span>Dedupe field</span>
                            <span>{dedupeMatch.hubspotColumn || '-'}</span>
                        </div>
                        <div className="mini-row">
                            <span>Dedupe value</span>
                            <span>{dedupeMatch.hubspotValue || '-'}</span>
                        </div>
                        <div className="mini-row">
                            <span>Match mode</span>
                            <span>{dedupeMatch.matchMode || 'exact'}</span>
                        </div>
                    </>
                )}
            </section>

            <section className="inspector-section">
                <div className="mini-card-title">Reason chain</div>
                {reasons.length === 0 && <div className="muted">No reason metadata</div>}
                {reasons.map((reason, idx) => (
                    <div key={idx} className="mini-row">
                        <span>{RowReason(reason)}</span>
                    </div>
                ))}
            </section>

            <section className="inspector-section mt12">
                <div className="mini-card-title">Row values</div>
                <div className="inspector-list">
                    {keys.map(key => (
                        <div key={key} className="inspector-item">
                            <span className="inspector-key">{key}</span>
                            <span className="inspector-value">{String(row[key] ?? '')}</span>
                        </div>
                    ))}
                </div>
            </section>
        </div>
    );
}
