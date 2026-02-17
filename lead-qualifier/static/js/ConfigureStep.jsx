/* ── Configure Step ────────────────────────────────────────── */
function ConfigStep({
    columns,
    columnProfiles,
    totalRows,
    rules,
    onUpdate,
    onRemove,
    onAdd,
    domChk,
    toggleDom,
    domField,
    setDomField,
    loading,
    onBack,
    onGo,
    canRun,
    estimate,
    previewLoading,
    dirty,
    dedupeFile,
    dedupeError,
    onDedupeFile,
}) {
    const activeCount = rules.filter(rule => rule.field).length;
    const disableReason = !canRun ? 'Add a filter, enable domain verification, or upload a HubSpot CSV.' : '';
    const domainRuntimeMins = Math.max(1, Math.round(totalRows / 700));

    return (
        <section>
            <div className="pg-head">
                <h1>Configure qualification</h1>
                <p>Define your ICP criteria, then run filtering and optional domain verification.</p>
            </div>

            <div className="context-bar">
                <div className="context-item"><I.columns /> {columns.length} columns</div>
                <div className="context-item"><I.rows /> {totalRows.toLocaleString()} rows</div>
                <div className="context-item"><I.filter /> {activeCount} active {activeCount === 1 ? 'filter' : 'filters'}</div>
                {estimate && (
                    <div className="context-item">
                        <span className="badge badge-ok">{estimate.estimatedQualifiedCount.toLocaleString()} estimated qualified</span>
                    </div>
                )}
                {previewLoading && <span className="mini-loader"><span className="spinner" /> Updating estimate…</span>}
            </div>

            <div className="pnl">
                <div className="pnl-head">
                    <h2><I.filter /> Filter rules</h2>
                </div>
                <div className="pnl-body">
                    <div className="rules-list">
                        {rules.map(rule => (
                            <RuleRow
                                key={rule.id}
                                rule={rule}
                                columns={columns}
                                columnProfiles={columnProfiles}
                                onChange={patch => onUpdate(rule.id, patch)}
                                onRemove={() => onRemove(rule.id)}
                                canRemove={rules.length > 1}
                            />
                        ))}
                    </div>
                    <button className="btn btn-t mt12" onClick={onAdd}><I.plus /> Add filter</button>
                </div>
            </div>

            <div className="pnl">
                <div className="pnl-head">
                    <h2><I.globe /> Domain verification</h2>
                </div>
                <div className="pnl-body">
                    <label htmlFor="domain-check" className="switch-row">
                        <span className="switch-text">
                            <span className="switch-title">Enable domain liveness checks</span>
                            <span className="switch-sub">
                                Detect parked or unreachable company websites before export.
                            </span>
                        </span>
                        <span className="switch-control">
                            <input
                                id="domain-check"
                                type="checkbox"
                                checked={domChk}
                                onChange={event => toggleDom(event.target.checked)}
                            />
                            <span className="switch-ui" />
                        </span>
                    </label>

                    {domChk && (
                        <div className="dom-row">
                            <div>
                                <span className="rule-lbl">Website column</span>
                                <select value={domField} onChange={e => setDomField(e.target.value)} aria-label="Website column">
                                    <option value="">Select column...</option>
                                    {columns.map(col => <option key={col.name} value={col.name}>{col.name}</option>)}
                                </select>
                            </div>
                            <div className="domain-note">
                                Estimated runtime: ~{domainRuntimeMins} min for {totalRows.toLocaleString()} rows.
                            </div>
                        </div>
                    )}
                </div>
            </div>

            <div className="pnl">
                <div className="pnl-head">
                    <h2><I.file /> HubSpot duplicate removal</h2>
                </div>
                <div className="pnl-body">
                    <div className="hubspot-upload">
                        <div>
                            <div className="switch-title">Upload HubSpot companies CSV</div>
                            <div className="switch-sub">Rows already present in HubSpot will be removed from export.</div>
                        </div>
                        <label className="btn btn-g file-btn">
                            <input
                                type="file"
                                accept=".csv,.tsv,text/csv,text/tab-separated-values"
                                onChange={e => onDedupeFile(e.target.files?.[0] || null)}
                            />
                            <I.upload /> Choose CSV
                        </label>
                    </div>
                    {dedupeFile && (
                        <div className="inline-msg ok mt12">
                            <I.check /> Using {dedupeFile.name} ({toReadableFileSize(dedupeFile.size)})
                            <button type="button" className="btn btn-t" onClick={() => onDedupeFile(null)}>Remove file</button>
                        </div>
                    )}
                    {dedupeError && <div className="inline-msg err mt12"><I.alertTri /> {dedupeError}</div>}
                    {estimate?.estimatedDuplicatesRemoved > 0 && (
                        <div className="inline-help mt12">
                            <I.info /> Estimated duplicates removed from HubSpot list: {estimate.estimatedDuplicatesRemoved.toLocaleString()}
                        </div>
                    )}
                </div>
            </div>

            <div className="action-rail sticky">
                <div className="action-rail-meta">
                    {disableReason && <span className="inline-help"><I.info /> {disableReason}</span>}
                    {!disableReason && dirty && <span className="inline-help"><I.info /> You have unsaved changes in this configuration.</span>}
                </div>
                <div className="btn-row">
                    <button className="btn btn-g" onClick={onBack}><I.arrowL /> Back</button>
                    <button className="btn btn-p" onClick={onGo} disabled={loading || !canRun}>
                        {loading && <span className="spinner" />} Run qualification
                    </button>
                </div>
            </div>
        </section>
    );
}
