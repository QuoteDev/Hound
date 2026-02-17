/* ── Kennel App V2 (Hybrid Attio model) ───────────────────── */
function App() {
    const [railExpanded, setRailExpanded] = useState(true);

    const [session, setSession] = useState(WorkspaceSession({}));
    const [config, setConfig] = useState(defaultWorkspaceConfig([]));
    const [estimate, setEstimate] = useState(null);
    const [runSummaries, setRunSummaries] = useState({});
    const [runProgressBySession, setRunProgressBySession] = useState({});
    const [runHistory, setRunHistory] = useState(() => loadRecentRuns());
    const [selectedRow, setSelectedRow] = useState(null);
    const [drawerState, setDrawerState] = useState(DrawerState.NONE);

    const [viewSearch, setViewSearch] = useState('');
    const [viewSort, setViewSort] = useState({ column: null, direction: null });
    const [viewFilters, setViewFilters] = useState([]);

    const [exportName, setExportName] = useState('qualified_leads.csv');
    const [exportColumns, setExportColumns] = useState([]);
    const [showImportModal, setShowImportModal] = useState(true);

    const [loading, setLoading] = useState(false);
    const [loadMsg, setLoadMsg] = useState('');
    const [error, setError] = useState('');
    const [presets, setPresets] = useState(() => loadFilterPresets());
    const [selectedPresetId, setSelectedPresetId] = useState('');
    const [presetName, setPresetName] = useState('');
    const [jumpQuery, setJumpQuery] = useState('');
    const currentSessionIdRef = useRef('');

    const sessionId = session?.sessionId;
    const hasDataset = !!sessionId;
    const runSummary = sessionId ? (runSummaries[sessionId] || null) : null;
    const runProgress = sessionId ? (runProgressBySession[sessionId] || null) : null;
    const sessionColumnNames = (session?.columns || []).map(col => col?.name).filter(Boolean);
    const selectedExportColumns = (exportColumns || []).filter(col => sessionColumnNames.includes(col));

    const dedupeSig = `${session?.dedupe?.enabled ? 1 : 0}:${(session?.dedupe?.fileNames || []).join('|') || session?.dedupe?.fileName || ''}`;
    const configSignature = ruleSignature(
        config.rules || [],
        config.domChk,
        config.homepageChk,
        config.domField,
        config.websiteKeywords || [],
        dedupeSig,
        config.tldCountryChk,
        config.tldDisallow || [],
        config.tldAllow || []
    );
    const lastRunSig = runSummary?._configSignature || '';
    const isUnsaved = !!runSummary && configSignature !== lastRunSig;
    const hasTldFilter = !!(config.tldCountryChk || (config.tldDisallow || []).length);
    const hasWebsiteValidation = !!(config.domChk || config.homepageChk || hasTldFilter);
    const hasValidWebsiteValidation = hasWebsiteValidation && !!config.domField;
    const hasInvalidWebsiteValidation = hasWebsiteValidation && !config.domField;

    const hasRuleLogic = (config.rules || []).some(rule => {
        if (!rule?.field) return false;
        if (rule.matchType === 'range') {
            return String(rule.min || '').trim() || String(rule.max || '').trim();
        }
        if (rule.matchType === 'dates') {
            return String(rule.startDate || '').trim() || String(rule.endDate || '').trim();
        }
        return (rule.groups || []).some(group => (group.tags || []).some(tag => String(tag || '').trim()));
    });

    const canRun = hasDataset && !hasInvalidWebsiteValidation && (
        hasRuleLogic ||
        hasValidWebsiteValidation ||
        session?.dedupe?.enabled
    );

    const disabledReason = !hasDataset
        ? 'Import a source CSV/TSV to begin qualification.'
        : hasInvalidWebsiteValidation
            ? 'Select a domain column to use domain/homepage/TLD validation.'
        : 'Add a qualification rule, enable domain/homepage/TLD validation, or attach a HubSpot dedupe CSV/TSV.';

    useEffect(() => {
        currentSessionIdRef.current = sessionId || '';
    }, [sessionId]);

    useEffect(() => {
        if (!hasDataset) setShowImportModal(true);
    }, [hasDataset]);

    const openDrawer = useCallback((state) => {
        if (!hasDataset) return;
        setDrawerState(state);
        if (state !== DrawerState.ROW_INSPECTOR) setSelectedRow(null);
    }, [hasDataset]);

    const closeDrawer = useCallback(() => {
        setDrawerState(DrawerState.NONE);
        setSelectedRow(null);
    }, []);

    const resetConfig = () => {
        if (!session?.columns?.length) {
            setConfig(defaultWorkspaceConfig([]));
            return;
        }
        const next = defaultWorkspaceConfig(session.columns);
        const guessed = guessDomainColumn(session.columns);
        next.domField = guessed;
        next.domChk = !!guessed;
        setConfig(next);
        setSelectedRow(null);
    };

    const onPresetSelect = useCallback((presetId) => {
        setSelectedPresetId(presetId || '');
        const preset = (presets || []).find(item => item.id === presetId);
        setPresetName(preset?.name || '');
    }, [presets]);

    const onSavePreset = useCallback((nameOverride = '') => {
        const name = String(nameOverride || presetName || '').trim();
        if (!name) {
            setError('Enter a preset name before saving.');
            return;
        }
        const outcome = saveFilterPreset({
            name,
            config,
            presetId: selectedPresetId || '',
        });
        setPresets(outcome.presets || []);
        if (!outcome.ok || !outcome.saved?.id) {
            setError('Unable to save preset.');
            return;
        }
        setSelectedPresetId(outcome.saved.id);
        setPresetName(outcome.saved.name);
        setError('');
    }, [presetName, config, selectedPresetId]);

    const onApplyPreset = useCallback(() => {
        if (!selectedPresetId) return;
        const preset = (presets || []).find(item => item.id === selectedPresetId);
        if (!preset?.config) {
            setError('Preset not found.');
            return;
        }
        const nextConfig = importConfigPreset(preset.config, session?.columns || []);
        setConfig(nextConfig);
        setPresetName(preset.name || '');
        setError('');
    }, [selectedPresetId, presets, session?.columns]);

    const onDeletePreset = useCallback(() => {
        if (!selectedPresetId) return;
        const preset = (presets || []).find(item => item.id === selectedPresetId);
        const label = preset?.name || 'this preset';
        if (typeof window !== 'undefined' && !window.confirm(`Delete preset "${label}"?`)) return;
        const outcome = deleteFilterPreset(selectedPresetId);
        setPresets(outcome.presets || []);
        if (!outcome.ok) {
            setError('Unable to delete preset.');
            return;
        }
        setSelectedPresetId('');
        setPresetName('');
        setError('');
    }, [selectedPresetId, presets]);

    const hydrateSessionState = useCallback((data) => {
        const nextSession = WorkspaceSession(data);
        const nextConfig = defaultWorkspaceConfig(nextSession.columns);
        const guessed = guessDomainColumn(nextSession.columns);
        nextConfig.domField = guessed;
        nextConfig.domChk = !!guessed;

        setSession(nextSession);
        setExportColumns((nextSession.columns || []).map(col => col?.name).filter(Boolean));
        setConfig(nextConfig);
        setEstimate(null);
        setSelectedRow(null);
        setDrawerState(DrawerState.NONE);
        setViewSearch('');
        setViewFilters([]);
        setViewSort({ column: null, direction: null });
        setShowImportModal(false);
    }, []);

    const onUploadSource = useCallback(async (sourceFiles, dedupeFiles = []) => {
        setError('');
        const nextSourceFiles = Array.isArray(sourceFiles) ? sourceFiles : (sourceFiles ? [sourceFiles] : []);
        const nextDedupeFiles = Array.isArray(dedupeFiles) ? dedupeFiles : (dedupeFiles ? [dedupeFiles] : []);
        if (!nextSourceFiles.length) return;
        if (nextSourceFiles.some(file => !isCsvFile(file))) {
            setError('Source file must be CSV or TSV.');
            return;
        }
        if (nextDedupeFiles.some(file => !isCsvFile(file))) {
            setError('HubSpot dedupe files must be CSV or TSV.');
            return;
        }

        setLoading(true);
        setLoadMsg('Uploading source files...');
        try {
            const fd = new FormData();
            nextSourceFiles.forEach(file => fd.append('files', file));
            nextDedupeFiles.forEach(file => fd.append('dedupeFiles', file));
            const data = await requestJSON(`${API}/api/session/upload`, { method: 'POST', body: fd });
            hydrateSessionState(data);
        } catch (e) {
            setError(e.message || 'Upload failed.');
        } finally {
            setLoading(false);
            setLoadMsg('');
        }
    }, [hydrateSessionState]);

    const onUploadDedupe = useCallback(async (files) => {
        const uploadFiles = Array.isArray(files) ? files : (files ? [files] : []);
        if (!sessionId || !uploadFiles.length) return;
        setError('');
        if (uploadFiles.some(file => !isCsvFile(file))) {
            setError('HubSpot dedupe files must be CSV or TSV.');
            return;
        }

        setLoading(true);
        setLoadMsg('Uploading HubSpot dedupe files...');
        try {
            const fd = new FormData();
            fd.append('sessionId', sessionId);
            uploadFiles.forEach(file => fd.append('dedupeFiles', file));
            const data = await requestJSON(`${API}/api/session/dedupe`, { method: 'POST', body: fd });
            setSession(prev => ({ ...prev, dedupe: data.dedupe }));
        } catch (e) {
            setError(e.message || 'Failed to set dedupe files.');
        } finally {
            setLoading(false);
            setLoadMsg('');
        }
    }, [sessionId]);

    const onClearDedupe = useCallback(async () => {
        if (!sessionId) return;
        setError('');
        setLoading(true);
        setLoadMsg('Removing HubSpot dedupe CSV...');
        try {
            const fd = new FormData();
            fd.append('sessionId', sessionId);
            const data = await requestJSON(`${API}/api/session/dedupe`, { method: 'POST', body: fd });
            setSession(prev => ({ ...prev, dedupe: data.dedupe }));
        } catch (e) {
            setError(e.message || 'Failed to remove dedupe file.');
        } finally {
            setLoading(false);
            setLoadMsg('');
        }
    }, [sessionId]);

    useEffect(() => {
        let cancelled = false;
        if (!sessionId) {
            setEstimate(null);
            return;
        }

        const timer = setTimeout(async () => {
            try {
                const fd = new FormData();
                fd.append('sessionId', sessionId);
                appendConfigFormData(fd, config);
                const data = await requestJSON(`${API}/api/session/preview`, { method: 'POST', body: fd });
                if (!cancelled) setEstimate(data);
            } catch (e) {
                if (!cancelled) setEstimate(null);
            }
        }, 260);

        return () => {
            cancelled = true;
            clearTimeout(timer);
        };
    }, [sessionId, configSignature]);

    const onRun = useCallback(async () => {
        if (!sessionId || !canRun) return;
        const targetSessionId = sessionId;
        const targetFileName = session?.fileName || 'dataset.csv';
        const targetFileNames = Array.isArray(session?.fileNames) ? session.fileNames : [];
        const targetTotalRows = session?.totalRows || 0;
        const signatureAtRunStart = configSignature;

        setError('');
        setRunProgressBySession(prev => ({
            ...prev,
            [targetSessionId]: {
            status: 'running',
            stage: 'starting',
            progress: 0,
            processedRows: 0,
                totalRows: targetTotalRows,
            qualifiedCount: 0,
            removedCount: 0,
            removedBreakdown: { removedFilter: 0, removedDomain: 0, removedHubspot: 0 },
            },
        }));

        try {
            const fd = new FormData();
            fd.append('sessionId', targetSessionId);
            appendConfigFormData(fd, config);
            await requestJSON(`${API}/api/session/qualify/start`, { method: 'POST', body: fd });

            let donePayload = null;
            for (let i = 0; i < 1200; i += 1) {
                const progress = await requestJSON(`${API}/api/session/qualify/progress?sessionId=${encodeURIComponent(targetSessionId)}`);
                setRunProgressBySession(prev => ({ ...prev, [targetSessionId]: progress }));

                if (progress?.status === 'done') {
                    donePayload = progress;
                    break;
                }
                if (progress?.status === 'error') {
                    throw new Error(progress?.error || 'Qualification failed.');
                }
                await new Promise(resolve => setTimeout(resolve, 350));
            }

            if (!donePayload?.result) {
                throw new Error('Qualification run timed out before completion.');
            }

            const summary = RunSummary(donePayload.result);
            summary._configSignature = signatureAtRunStart;
            setRunSummaries(prev => ({ ...prev, [targetSessionId]: summary }));

            const completedAt = new Date().toISOString();
            const historyOutcome = appendRecentRun({
                id: `${targetSessionId}:${donePayload?.runId || completedAt}`,
                runId: donePayload?.runId || '',
                sessionId: targetSessionId,
                fileName: targetFileName,
                fileNames: targetFileNames,
                totalRows: summary.totalRows || targetTotalRows,
                qualifiedCount: summary.qualifiedCount,
                removedCount: summary.removedCount,
                removedBreakdown: summary.removedBreakdown,
                completedAt,
                processingMs: summary?.meta?.processingMs || 0,
                meta: {
                    domainCheckEnabled: !!summary?.meta?.domainCheckEnabled,
                    homepageCheckEnabled: !!summary?.meta?.homepageCheckEnabled,
                    dedupeEnabled: !!summary?.meta?.dedupe?.enabled,
                },
            });
            setRunHistory(historyOutcome.runs || []);

            if (currentSessionIdRef.current === targetSessionId) {
                closeDrawer();
            }
        } catch (e) {
            setError(e.message || 'Qualification failed.');
            setRunProgressBySession(prev => ({
                ...prev,
                [targetSessionId]: {
                    ...(prev[targetSessionId] || {}),
                    status: 'error',
                    stage: 'error',
                    progress: 1,
                    message: e?.message || 'Qualification failed.',
                    error: e?.message || 'Qualification failed.',
                },
            }));
        }
    }, [sessionId, canRun, config, configSignature, closeDrawer, session?.fileName, session?.fileNames, session?.totalRows]);

    const onExport = useCallback(async () => {
        if (!sessionId) return;
        if (!selectedExportColumns.length) {
            setError('Select at least one export column.');
            return;
        }
        setError('');
        setLoading(true);
        setLoadMsg('Preparing export...');
        try {
            const fd = new FormData();
            fd.append('sessionId', sessionId);
            appendConfigFormData(fd, config);
            fd.append('exportColumns', JSON.stringify(selectedExportColumns));
            fd.append('fileName', exportName || 'qualified_leads.csv');
            const blob = await requestBlob(`${API}/api/session/export`, { method: 'POST', body: fd });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = (exportName || 'qualified_leads.csv').endsWith('.csv') ? exportName : `${exportName}.csv`;
            a.click();
            URL.revokeObjectURL(url);
        } catch (e) {
            setError(e.message || 'Export failed.');
        } finally {
            setLoading(false);
            setLoadMsg('');
        }
    }, [sessionId, config, exportName, selectedExportColumns]);

    const onSelectRecentRun = useCallback(async (entry) => {
        const targetSessionId = String(entry?.sessionId || '').trim();
        if (!targetSessionId) {
            setError('This recent run does not have a restorable session.');
            return;
        }

        setError('');
        setLoading(true);
        setLoadMsg(`Loading ${entry?.fileName || 'recent run'}...`);
        try {
            const data = await requestJSON(`${API}/api/session/state?sessionId=${encodeURIComponent(targetSessionId)}`);
            hydrateSessionState(data);
        } catch (e) {
            setError(e.message || 'Unable to load this recent run.');
        } finally {
            setLoading(false);
            setLoadMsg('');
        }
    }, [hydrateSessionState]);

    const updateRule = (id, patch) => setConfig(curr => ({
        ...curr,
        rules: (curr.rules || []).map(r => (r.id === id ? { ...r, ...patch } : r)),
    }));

    const removeRule = (id) => setConfig(curr => ({
        ...curr,
        rules: (curr.rules || []).length <= 1 ? curr.rules : curr.rules.filter(r => r.id !== id),
    }));

    const addRule = () => setConfig(curr => ({
        ...curr,
        rules: [...(curr.rules || []), Rule(session?.columns?.[0]?.name || '', session?.columns || [])],
    }));

    const addViewFilter = (filter) => {
        setViewFilters(curr => [...curr, ViewFilter(filter)]);
    };

    const removeViewFilter = (id) => setViewFilters(curr => curr.filter(f => f.id !== id));

    const promoteViewFilters = () => {
        if (!hasDataset) return;

        const supported = [];
        const unsupported = [];
        const cols = session?.columns || [];

        for (const filter of (viewFilters || [])) {
            if (!filter?.field) continue;

            const r = Rule(filter.field, cols);
            if (filter.op === 'contains') r.matchType = 'contains';
            else if (filter.op === 'equals') r.matchType = 'exact';
            else if (filter.op === 'not_equals') r.matchType = 'excludes';
            else if (filter.op === 'is_empty') r.matchType = 'exact';
            else if (filter.op === 'is_not_empty') r.matchType = 'excludes';
            else if (filter.op === 'before') {
                r.matchType = 'dates';
                r.startDate = '';
                r.endDate = String(filter.value || '');
            } else if (filter.op === 'after') {
                r.matchType = 'dates';
                r.startDate = String(filter.value || '');
                r.endDate = '';
            }

            if (r.matchType !== 'dates') {
                const value = (filter.op === 'is_empty' || filter.op === 'is_not_empty') ? '' : String(filter.value || '');
                r.groups = [RuleGroup([value])];
                r.groupsLogic = 'or';
            }
            supported.push(r);
        }

        if (!supported.length) {
            setError('No view filters could be promoted to qualification filters.');
            openDrawer(DrawerState.FILTERS);
            return;
        }

        setConfig(curr => ({
            ...curr,
            rules: [...(curr.rules || []).filter(r => r.field), ...supported],
        }));

        if (unsupported.length) {
            setError(`Some view filters were skipped: ${unsupported.join(', ')}`);
        } else {
            setError('');
        }

        openDrawer(DrawerState.FILTERS);
    };

    const onSelectRow = (row) => {
        setSelectedRow(row);
        setDrawerState(DrawerState.ROW_INSPECTOR);
    };

    const drawerMeta = {
        [DrawerState.FILTERS]: {
            title: 'Qualification filters',
            subtitle: 'Advanced filter logic for qualification rules. This is separate from top view filters.',
        },
        [DrawerState.VALIDATION]: {
            title: 'Validation and dedupe',
            subtitle: 'Domain verification and HubSpot duplicate removal for cleaner output.',
        },
        [DrawerState.EXPORT]: {
            title: 'Export options',
            subtitle: 'Set filename and review estimated output before downloading.',
        },
        [DrawerState.ROW_INSPECTOR]: {
            title: 'Row inspector',
            subtitle: 'Inspect row status, reason chain, and full values.',
        },
    };

    const drawerContent = drawerState === DrawerState.ROW_INSPECTOR
        ? <InspectorDrawer row={selectedRow} />
        : (
            <ControlPlane
                drawerState={drawerState}
                session={session}
                config={config}
                estimate={estimate}
                dedupeMeta={session?.dedupe}
                exportName={exportName}
                onExportName={setExportName}
                exportColumns={selectedExportColumns}
                onExportColumns={setExportColumns}
                onRuleUpdate={updateRule}
                onRuleRemove={removeRule}
                onRuleAdd={addRule}
                presets={presets}
                selectedPresetId={selectedPresetId}
                presetName={presetName}
                onPresetSelect={onPresetSelect}
                onPresetName={setPresetName}
                onSavePreset={onSavePreset}
                onApplyPreset={onApplyPreset}
                onDeletePreset={onDeletePreset}
                onToggleDomain={(checked) => setConfig(curr => {
                    const shouldKeepDomainField = checked || curr.homepageChk || curr.tldCountryChk || (curr.tldDisallow || []).length > 0;
                    const guessedField = curr.domField || guessDomainColumn(session?.columns || []);
                    return {
                        ...curr,
                        domChk: checked,
                        domField: shouldKeepDomainField ? guessedField : '',
                    };
                })}
                onToggleHomepage={(checked) => setConfig(curr => {
                    const shouldKeepDomainField = checked || curr.domChk || curr.tldCountryChk || (curr.tldDisallow || []).length > 0;
                    const guessedField = curr.domField || guessDomainColumn(session?.columns || []);
                    return {
                        ...curr,
                        homepageChk: checked,
                        domField: shouldKeepDomainField ? guessedField : '',
                    };
                })}
                onDomainField={(field) => setConfig(curr => ({ ...curr, domField: field }))}
                onToggleCountryTlds={(checked) => setConfig(curr => {
                    const shouldKeepDomainField = checked || curr.domChk || curr.homepageChk || (curr.tldDisallow || []).length > 0;
                    const guessedField = curr.domField || guessDomainColumn(session?.columns || []);
                    return {
                        ...curr,
                        tldCountryChk: checked,
                        domField: shouldKeepDomainField ? guessedField : '',
                    };
                })}
                onTldDisallowList={(raw) => setConfig(curr => {
                    const nextDisallow = parseTldListInput(raw);
                    const shouldKeepDomainField = curr.domChk || curr.homepageChk || curr.tldCountryChk || nextDisallow.length > 0;
                    const guessedField = curr.domField || guessDomainColumn(session?.columns || []);
                    return {
                        ...curr,
                        tldDisallow: nextDisallow,
                        domField: shouldKeepDomainField ? guessedField : '',
                    };
                })}
                onTldAllowList={(raw) => setConfig(curr => ({
                    ...curr,
                    tldAllow: parseTldListInput(raw),
                }))}
                onWebsiteKeywords={(raw) => setConfig(curr => ({
                    ...curr,
                    websiteKeywordsText: raw,
                    websiteKeywords: parseKeywordListInput(raw),
                }))}
                onUploadDedupe={onUploadDedupe}
                onClearDedupe={onClearDedupe}
                onExport={onExport}
                onResetConfig={resetConfig}
                loading={loading}
            />
        );

    const drawerFooter = (drawerState === DrawerState.FILTERS || drawerState === DrawerState.VALIDATION) && estimate
        ? (
            <div className="drawer-foot-row">
                <span>{estimate.estimatedQualifiedCount.toLocaleString()} qualified</span>
                <span>{estimate.estimatedRemovedCount.toLocaleString()} removed</span>
            </div>
        )
        : null;

    return (
        <div>
            {error && <div className="inline-msg err"><I.alertTri /> {error}</div>}

            <WorkspaceShell
                leftRail={(
                    <LeftRail
                        expanded={railExpanded}
                        onToggleExpanded={() => setRailExpanded(!railExpanded)}
                        hasDataset={hasDataset}
                        activeDrawer={drawerState}
                        onOpenDrawer={openDrawer}
                        onCloseDrawer={closeDrawer}
                        session={session}
                        runHistory={runHistory}
                        jumpQuery={jumpQuery}
                        onJumpQuery={setJumpQuery}
                        onSelectRun={onSelectRecentRun}
                    />
                )}
                header={(
                    <CommandHeader
                        session={session}
                        runSummary={runSummary}
                        estimate={estimate}
                        drawerState={drawerState}
                        canRun={canRun}
                        disabledReason={disabledReason}
                        isUnsaved={isUnsaved}
                        loading={loading}
                        loadMsg={loadMsg}
                        runProgress={runProgress}
                        onOpenDrawer={openDrawer}
                        onCloseDrawer={closeDrawer}
                        onRun={onRun}
                        onExport={onExport}
                        onOpenImportModal={() => setShowImportModal(true)}
                        viewSearch={viewSearch}
                        onViewSearch={setViewSearch}
                        viewFilters={viewFilters}
                        onAddViewFilter={addViewFilter}
                        onRemoveViewFilter={removeViewFilter}
                        onClearViewFilters={() => setViewFilters([])}
                        onPromoteViewFilters={promoteViewFilters}
                    />
                )}
                mainContent={(
                    <DataCanvas
                        session={session}
                        runSummary={runSummary}
                        runProgress={runProgress}
                        viewSearch={viewSearch}
                        viewSort={viewSort}
                        onViewSort={setViewSort}
                        viewFilters={viewFilters}
                        selectedRowId={selectedRow?._rowId}
                        onSelectRow={onSelectRow}
                    />
                )}
                drawer={drawerState !== DrawerState.NONE && (
                    <ContextDrawer
                        title={drawerMeta[drawerState]?.title}
                        subtitle={drawerMeta[drawerState]?.subtitle}
                        onClose={closeDrawer}
                        footer={drawerFooter}
                    >
                        {drawerContent}
                    </ContextDrawer>
                )}
                drawerOpen={drawerState !== DrawerState.NONE}
            />

            <EmptyDatasetView
                open={showImportModal}
                loading={loading}
                canClose={hasDataset && !loading}
                onClose={() => setShowImportModal(false)}
                onUploadSource={onUploadSource}
            />
        </div>
    );
}

ReactDOM.createRoot(document.getElementById('root')).render(<App />);
