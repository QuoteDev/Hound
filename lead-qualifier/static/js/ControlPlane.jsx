/* ── Drawer Content Modules ────────────────────────────────── */
function ControlPlane({
    drawerState,
    session,
    config,
    estimate,
    dedupeMeta,
    exportName,
    onExportName,
    exportColumns,
    onExportColumns,
    onRuleUpdate,
    onRuleRemove,
    onRuleAdd,
    presets,
    selectedPresetId,
    presetName,
    onPresetSelect,
    onPresetName,
    onSavePreset,
    onApplyPreset,
    onDeletePreset,
    onToggleDomain,
    onToggleHomepage,
    onDomainField,
    onWebsiteKeywords,
    onToggleCountryTlds,
    onTldDisallowList,
    onTldAllowList,
    onUploadDedupe,
    onClearDedupe,
    onExport,
    onResetConfig,
    loading,
    scrapeProgress,
    onStartScrape,
}) {
    const dedupeInputRef = useRef();
    const [presetNameDraft, setPresetNameDraft] = useState(presetName || '');
    const columns = session?.columns || [];
    const dedupeFileNames = Array.isArray(dedupeMeta?.fileNames)
        ? dedupeMeta.fileNames.map(name => String(name || '').trim()).filter(Boolean)
        : (dedupeMeta?.fileName ? [String(dedupeMeta.fileName)] : []);
    const exportColumnNames = columns.map(col => col?.name).filter(Boolean);
    const columnProfiles = session?.columnProfiles || [];
    const exportSelected = (exportColumns || []).filter(col => exportColumnNames.includes(col));
    const exportSelectedSet = new Set(exportSelected);
    const presetList = presets || [];
    const ruleCount = (config?.rules || []).filter(r => r.field).length;
    const hasTldFilter = !!(config?.tldCountryChk || (config?.tldDisallow || []).length);
    const hasHomepageKeywords = !!(config?.websiteKeywords || []).length;
    const scrapeStatus = String(scrapeProgress?.status || 'idle');
    const scrapeRunning = scrapeStatus === 'running';
    const scrapeDone = scrapeStatus === 'done';
    const scrapeError = scrapeStatus === 'error';
    const scrapeProgressPct = Math.max(0, Math.min(100, Math.round((scrapeProgress?.progress || 0) * 100)));
    const scrapeCanStart = !!(config?.domField && typeof onStartScrape === 'function' && !scrapeRunning);
    const gapReport = useMemo(
        () => buildIcpGapReport(session, config),
        [
            session?.totalRows,
            JSON.stringify(session?.columns || []),
            JSON.stringify(session?.columnProfiles || []),
            config?.domChk,
            config?.homepageChk,
            config?.domField,
            config?.tldCountryChk,
            JSON.stringify(config?.tldDisallow || []),
            JSON.stringify(config?.websiteKeywords || []),
        ]
    );

    useEffect(() => {
        setPresetNameDraft(presetName || '');
    }, [presetName, selectedPresetId, drawerState]);

    const updatePresetNameDraft = (value) => {
        setPresetNameDraft(value);
        if (typeof onPresetName === 'function') onPresetName(value);
    };

    const updateExportColumns = (nextColumns) => {
        if (typeof onExportColumns === 'function') onExportColumns(nextColumns);
    };

    const setAllExportColumns = () => updateExportColumns([...exportColumnNames]);
    const clearExportColumns = () => updateExportColumns([]);

    const toggleExportColumn = (columnName, enabled) => {
        if (!columnName) return;
        const nextSet = new Set(exportSelected);
        if (enabled) nextSet.add(columnName);
        else nextSet.delete(columnName);
        const ordered = exportColumnNames.filter(name => nextSet.has(name));
        updateExportColumns(ordered);
    };

    if (drawerState === DrawerState.FILTERS) {
        return (
            <div className="drawer-section drawer-stack">
                <div className="drawer-info">
                    <span className="badge badge-m">{ruleCount} active qualification rules</span>
                    {estimate && (
                        <span className="drawer-metric">
                            {estimate.estimatedQualifiedCount.toLocaleString()} qualified · {estimate.estimatedRemovedCount.toLocaleString()} removed
                        </span>
                    )}
                </div>

                <section className="sheet-block">
                    <div className="mini-card-title">Filter and validation presets</div>
                    <div className="mini-row">
                        <span>Saved presets</span>
                        <span>{presetList.length}</span>
                    </div>
                    <div>
                        <span className="rule-lbl">Saved presets</span>
                        <select value={selectedPresetId || ''} onChange={e => onPresetSelect(e.target.value)}>
                            <option value="">Select preset...</option>
                            {presetList.map(item => <option key={item.id} value={item.id}>{item.name}</option>)}
                        </select>
                    </div>
                    <div>
                        <span className="rule-lbl">Preset name</span>
                        <input
                            type="text"
                            value={presetNameDraft}
                            onChange={e => updatePresetNameDraft(e.target.value)}
                            placeholder="US B2B Seed profile"
                        />
                    </div>
                    <div className="btn-row">
                        <button className="btn btn-g" type="button" onClick={() => onSavePreset(presetNameDraft)} disabled={!String(presetNameDraft || '').trim()}>Save</button>
                        <button className="btn btn-g" type="button" onClick={onApplyPreset} disabled={!selectedPresetId}>Apply</button>
                        <button className="btn btn-t" type="button" onClick={onDeletePreset} disabled={!selectedPresetId}>Delete</button>
                    </div>
                </section>

                <div className="rules-list">
                    {(config?.rules || []).map(rule => (
                        <RuleRow
                            key={rule.id}
                            rule={rule}
                            columns={columns}
                            columnProfiles={columnProfiles}
                            onChange={patch => onRuleUpdate(rule.id, patch)}
                            onRemove={() => onRuleRemove(rule.id)}
                            canRemove={(config?.rules || []).length > 1}
                        />
                    ))}
                </div>

                <div className="btn-row mt12">
                    <button className="btn btn-g drawer-add-rule" onClick={onRuleAdd}>
                        <I.plus /> Add rule
                    </button>
                    <button className="btn btn-t" onClick={onResetConfig}>Reset qualification rules</button>
                </div>
            </div>
        );
    }

    if (drawerState === DrawerState.VALIDATION) {
        return (
            <div className="drawer-section drawer-stack">
                <section className="sheet-block">
                    <div className="mini-card-title">Filter and validation presets</div>
                    <div className="mini-row">
                        <span>Saved presets</span>
                        <span>{presetList.length}</span>
                    </div>
                    <div>
                        <span className="rule-lbl">Saved presets</span>
                        <select value={selectedPresetId || ''} onChange={e => onPresetSelect(e.target.value)}>
                            <option value="">Select preset...</option>
                            {presetList.map(item => <option key={item.id} value={item.id}>{item.name}</option>)}
                        </select>
                    </div>
                    <div>
                        <span className="rule-lbl">Preset name</span>
                        <input
                            type="text"
                            value={presetNameDraft}
                            onChange={e => updatePresetNameDraft(e.target.value)}
                            placeholder="US B2B Seed profile"
                        />
                    </div>
                    <div className="btn-row">
                        <button className="btn btn-g" type="button" onClick={() => onSavePreset(presetNameDraft)} disabled={!String(presetNameDraft || '').trim()}>Save</button>
                        <button className="btn btn-g" type="button" onClick={onApplyPreset} disabled={!selectedPresetId}>Apply</button>
                        <button className="btn btn-t" type="button" onClick={onDeletePreset} disabled={!selectedPresetId}>Delete</button>
                    </div>
                </section>

                <section className="sheet-block">
                    <label className="switch-row" htmlFor="drawer-domain-check">
                        <span className="switch-text">
                            <span className="switch-title">Domain liveness check</span>
                            <span className="switch-sub">Exclude unreachable or parked domains during qualification.</span>
                        </span>
                        <span className="switch-control">
                            <input
                                id="drawer-domain-check"
                                type="checkbox"
                                checked={!!config.domChk}
                                onChange={e => onToggleDomain(e.target.checked)}
                            />
                            <span className="switch-ui" />
                        </span>
                    </label>

                    <label className="switch-row" htmlFor="drawer-homepage-check">
                        <span className="switch-text">
                            <span className="switch-title">Homepage signal check</span>
                            <span className="switch-sub">Scrape homepage signals to screen for B2B and disqualifying website intent.</span>
                        </span>
                        <span className="switch-control">
                            <input
                                id="drawer-homepage-check"
                                type="checkbox"
                                checked={!!config.homepageChk}
                                onChange={e => onToggleHomepage(e.target.checked)}
                            />
                            <span className="switch-ui" />
                        </span>
                    </label>

                    {(config.domChk || config.homepageChk || hasTldFilter) && (
                        <div>
                            <span className="rule-lbl">Domain column</span>
                            <select value={config.domField || ''} onChange={e => onDomainField(e.target.value)}>
                                <option value="">Select column...</option>
                                {columns.map(col => <option key={col.name} value={col.name}>{col.name}</option>)}
                            </select>
                        </div>
                    )}

                    {config.homepageChk && (
                        <>
                            <div>
                                <span className="rule-lbl">Website keywords (optional)</span>
                                <textarea
                                    className="tld-textarea"
                                    value={typeof config.websiteKeywordsText === 'string'
                                        ? config.websiteKeywordsText
                                        : formatKeywordListInput(config.websiteKeywords || [])}
                                    onChange={e => onWebsiteKeywords(e.target.value)}
                                    placeholder="api, workflow automation, compliance, enterprise, developer platform"
                                />
                                <div className="inline-help mt12">
                                    <I.info /> When provided, at least one keyword must appear on the homepage.
                                </div>
                            </div>
                            <div className="mini-row">
                                <span>Keyword rules</span>
                                <span>{hasHomepageKeywords ? `${(config.websiteKeywords || []).length} active` : 'None (generic B2B checks only)'}</span>
                            </div>
                        </>
                    )}

                    <label className="switch-row" htmlFor="drawer-country-tld-check">
                        <span className="switch-text">
                            <span className="switch-title">Exclude country-specific TLDs</span>
                            <span className="switch-sub">Comprehensive match for two-letter country roots (for example `.de`, `.co.uk`, `.com.au`).</span>
                        </span>
                        <span className="switch-control">
                            <input
                                id="drawer-country-tld-check"
                                type="checkbox"
                                checked={!!config.tldCountryChk}
                                onChange={e => onToggleCountryTlds(e.target.checked)}
                            />
                            <span className="switch-ui" />
                        </span>
                    </label>

                    <div>
                        <span className="rule-lbl">Always allow TLDs</span>
                        <textarea
                            className="tld-textarea"
                            value={formatTldListInput(config.tldAllow || [])}
                            onChange={e => onTldAllowList(e.target.value)}
                            placeholder=".com, .io, .ai, .dev, .co, .org, .net, .app, .tech, .so, .gg"
                        />
                    </div>

                    <div>
                        <span className="rule-lbl">Additional disallowed TLDs (optional)</span>
                        <textarea
                            className="tld-textarea"
                            value={formatTldListInput(config.tldDisallow || [])}
                            onChange={e => onTldDisallowList(e.target.value)}
                            placeholder=".co.uk, .de, .fr, .com.au, .ca"
                        />
                    </div>
                </section>

                <section className="sheet-block">
                    <div className="mini-card-title">Homepage scraper enrichment</div>
                    <div className="switch-sub">Run async scraping once to add `scrape_*` columns (title, descriptions, headings, body text, and keywords) to this dataset.</div>
                    <div className="btn-row mt12">
                        <button
                            type="button"
                            className="btn btn-g"
                            onClick={onStartScrape}
                            disabled={!scrapeCanStart || loading}
                        >
                            {scrapeRunning ? <><span className="spinner" /> Scraping…</> : 'Run homepage scraper'}
                        </button>
                    </div>
                    {scrapeStatus !== 'idle' && (
                        <div className={`inline-help mt12 ${scrapeError ? 'err' : ''}`}>
                            {scrapeRunning ? <span className="spinner" /> : <I.info />}
                            {' '}
                            {scrapeProgress?.message || 'Scraper status available.'}
                            {' '}
                            ({scrapeProgressPct}% · {Number(scrapeProgress?.processed || 0).toLocaleString()}/{Number(scrapeProgress?.total || 0).toLocaleString()})
                        </div>
                    )}
                    {scrapeDone && scrapeProgress?.result?.warnings?.length > 0 && (
                        <div className="inline-help mt12">
                            <I.info /> {scrapeProgress.result.warnings[0]}
                        </div>
                    )}
                </section>

                <section className="sheet-block">
                    <div className="mini-card-title">HubSpot duplicate guard</div>
                    <div className="switch-sub">Attach one or more HubSpot company exports to remove known records from output.</div>

                    <div className="hubspot-upload mt12">
                        <button className="btn btn-g file-btn" onClick={() => dedupeInputRef.current?.click()}>
                            <I.upload /> Upload HubSpot files
                            <input
                                ref={dedupeInputRef}
                                type="file"
                                multiple
                                accept=".csv,.tsv,text/csv,text/tab-separated-values"
                                onChange={e => {
                                    const files = Array.from(e.target.files || []);
                                    if (files.length) onUploadDedupe(files);
                                }}
                            />
                        </button>
                        {dedupeMeta?.enabled && (
                            <button type="button" className="btn btn-t" onClick={onClearDedupe}>Remove</button>
                        )}
                    </div>

                    {dedupeMeta?.enabled && (
                        <div className="inline-help mt12">
                            <I.check /> {dedupeFileNames.length > 1
                                ? `${dedupeFileNames.length} HubSpot files attached`
                                : (dedupeMeta.fileName || 'HubSpot file attached')}
                            {dedupeMeta?.inferredMatch?.keyType && <span className="badge badge-m">{dedupeMeta.inferredMatch.keyType} match</span>}
                        </div>
                    )}
                    {dedupeMeta?.enabled && dedupeFileNames.length > 0 && (
                        <div className="dedupe-file-list mt12" aria-label="Attached HubSpot dedupe files">
                            {dedupeFileNames.map((fileName, index) => (
                                <div key={`${fileName}-${index}`} className="dedupe-file-item">
                                    <I.file />
                                    <span>{fileName}</span>
                                </div>
                            ))}
                        </div>
                    )}
                </section>

                <section className="sheet-block">
                    <div className="mini-card-title">ICP coverage and gap plan</div>
                    <div className="mini-row">
                        <span>Ready</span>
                        <span>{gapReport.summary.ready}/{gapReport.summary.total}</span>
                    </div>
                    <div className="mini-row">
                        <span>Partial</span>
                        <span>{gapReport.summary.partial}/{gapReport.summary.total}</span>
                    </div>
                    <div className="mini-row">
                        <span>Gap</span>
                        <span>{gapReport.summary.gap}/{gapReport.summary.total}</span>
                    </div>
                    {gapReport.criteria.map(item => (
                        <div key={item.label}>
                            <div className="mini-row">
                                <span>{item.label}</span>
                                <span>{item.status} · {item.coverageLabel}</span>
                            </div>
                            <div className="inline-help">
                                <I.info /> {item.columns.length ? `Using: ${item.columns.slice(0, 3).join(', ')}` : 'No matching source column detected.'}
                            </div>
                            <div className="inline-help">
                                <I.check /> {item.gapPlan}
                            </div>
                        </div>
                    ))}
                </section>
            </div>
        );
    }

    if (drawerState === DrawerState.EXPORT) {
        return (
            <div className="drawer-section drawer-stack">
                <section className="sheet-block">
                    <div className="mini-card-title">Export preset</div>
                    <span className="rule-lbl">File name</span>
                    <input value={exportName} onChange={e => onExportName(e.target.value)} placeholder="qualified_leads.csv" />
                </section>

                <section className="sheet-block">
                    <div className="mini-card-title">Export columns</div>
                    <div className="mini-row">Selected <span>{exportSelected.length}/{exportColumnNames.length}</span></div>
                    <div className="btn-row">
                        <button className="btn btn-g" type="button" onClick={setAllExportColumns} disabled={!exportColumnNames.length}>Select all</button>
                        <button className="btn btn-t" type="button" onClick={clearExportColumns} disabled={!exportSelected.length}>Clear</button>
                    </div>
                    <div className="export-columns-list" role="group" aria-label="Export columns">
                        {!exportColumnNames.length && <div className="inline-help"><I.info /> No columns available for export.</div>}
                        {exportColumnNames.map(columnName => (
                            <label key={columnName} className="export-column-row">
                                <input
                                    type="checkbox"
                                    checked={exportSelectedSet.has(columnName)}
                                    onChange={e => toggleExportColumn(columnName, e.target.checked)}
                                />
                                <span>{columnName}</span>
                            </label>
                        ))}
                    </div>
                </section>

                <div className="cp-grid-two mt12">
                    <section className="sheet-block">
                        <div className="mini-card-title">Current estimate</div>
                        <div className="mini-row">Qualified <span>{estimate?.estimatedQualifiedCount?.toLocaleString?.() || '0'}</span></div>
                        <div className="mini-row">Removed <span>{estimate?.estimatedRemovedCount?.toLocaleString?.() || '0'}</span></div>
                    </section>
                    <section className="sheet-block">
                        <div className="mini-card-title">Dataset status</div>
                        <div className="mini-row">File <span>{session?.fileName || 'None'}</span></div>
                        <div className="mini-row">Rows <span>{(session?.totalRows || 0).toLocaleString()}</span></div>
                    </section>
                </div>

                <button className="btn btn-p mt12" onClick={onExport} disabled={loading || !exportSelected.length}>
                    {loading && <span className="spinner" />} Export CSV
                </button>
            </div>
        );
    }

    return null;
}
