/* ── Kennel App V2 (Hybrid Attio model) ───────────────────── */
const VIEW_STORAGE_PREFIX = 'hound_table_views_v1';

function _viewsStorageKey(sessionId) {
    return `${VIEW_STORAGE_PREFIX}:${String(sessionId || '')}`;
}

function _loadSavedViews(sessionId) {
    if (!sessionId || typeof window === 'undefined' || !window.localStorage) return [];
    try {
        const raw = window.localStorage.getItem(_viewsStorageKey(sessionId));
        const parsed = JSON.parse(raw || '[]');
        if (!Array.isArray(parsed)) return [];
        return parsed
            .filter(view => view && typeof view === 'object' && view.id && view.name)
            .map(view => ({
                id: String(view.id),
                name: String(view.name),
                search: String(view.search || ''),
                sort: {
                    column: view?.sort?.column || null,
                    direction: view?.sort?.direction || null,
                },
                filters: Array.isArray(view.filters)
                    ? view.filters.map(filter => ({
                        field: String(filter?.field || ''),
                        op: String(filter?.op || 'contains'),
                        value: String(filter?.value || ''),
                        value2: String(filter?.value2 || ''),
                    }))
                    : [],
            }));
    } catch (_e) {
        return [];
    }
}

function _persistSavedViews(sessionId, views) {
    if (!sessionId || typeof window === 'undefined' || !window.localStorage) return;
    try {
        window.localStorage.setItem(_viewsStorageKey(sessionId), JSON.stringify(views || []));
    } catch (_e) {
        // Ignore storage write failures.
    }
}

function App() {
    const [railExpanded, setRailExpanded] = useState(false);

    const [session, setSession] = useState(WorkspaceSession({}));
    const [config, setConfig] = useState(defaultWorkspaceConfig([]));
    const [estimate, setEstimate] = useState(null);
    const [runSummaries, setRunSummaries] = useState({});
    const [runProgressBySession, setRunProgressBySession] = useState({});
    const [scrapeProgressBySession, setScrapeProgressBySession] = useState({});
    const [runHistory, setRunHistory] = useState(() => loadRecentRuns());
    const [selectedRow, setSelectedRow] = useState(null);
    const [drawerState, setDrawerState] = useState(DrawerState.NONE);

    const [viewSearch, setViewSearch] = useState('');
    const [viewSort, setViewSort] = useState({ column: null, direction: null });
    const [viewFilters, setViewFilters] = useState([]);
    const [savedViews, setSavedViews] = useState([]);
    const [activeViewId, setActiveViewId] = useState('default');
    const [columnPrefs, setColumnPrefs] = useState({ hidden: {}, labels: {}, formats: {} });

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
    const runContextRef = useRef({});
    const restoringRef = useRef(false);

    const sessionId = session?.sessionId;
    const hasDataset = !!sessionId;
    const runSummary = sessionId ? (runSummaries[sessionId] || null) : null;
    const runProgress = sessionId ? (runProgressBySession[sessionId] || null) : null;
    const scrapeProgress = sessionId ? (scrapeProgressBySession[sessionId] || null) : null;
    const isScrapeRunning = String(scrapeProgress?.status || '') === 'running';
    const sessionColumnNames = (session?.columns || []).map(col => col?.name).filter(Boolean);
    const hiddenColumnMap = columnPrefs?.hidden || {};
    const totalColumnCount = sessionColumnNames.length;
    const visibleColumnCount = (session?.columns || []).filter(col => !hiddenColumnMap[col?.name]).length;
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
    const hasInvalidWebsiteValidation = hasWebsiteValidation && !config.domField;

    const canRun = hasDataset && !hasInvalidWebsiteValidation && !isScrapeRunning;

    const disabledReason = !hasDataset
        ? 'Import a source CSV/TSV to begin qualification.'
        : isScrapeRunning
            ? 'Homepage scraper enrichment is running. Wait for it to finish before qualification.'
        : hasInvalidWebsiteValidation
            ? 'Select a domain column to use domain/homepage/TLD validation.'
        : '';

    useEffect(() => {
        if (!sessionId) {
            setSavedViews([]);
            setActiveViewId('default');
            return;
        }
        setSavedViews(_loadSavedViews(sessionId));
        setActiveViewId('default');
    }, [sessionId]);

    useEffect(() => {
        if (!sessionId) return;
        _persistSavedViews(sessionId, savedViews);
    }, [sessionId, savedViews]);

    useEffect(() => {
        currentSessionIdRef.current = sessionId || '';
    }, [sessionId]);

    useEffect(() => {
        if (!hasDataset) setShowImportModal(true);
    }, [hasDataset]);

    useEffect(() => {
        if (restoringRef.current) return;
        restoringRef.current = true;
        let cancelled = false;

        const restore = async () => {
            try {
                const saved = loadWorkspaceRestoreState();
                const preferredSessionId = String(saved?.sessionId || '').trim();

                if (preferredSessionId) {
                    const data = await requestJSON(`${API}/api/session/state?sessionId=${encodeURIComponent(preferredSessionId)}`);
                    if (!cancelled) {
                        hydrateSessionState(data);
                        if (!data?.workspaceConfig && saved?.config) {
                            const cols = Array.isArray(data?.columns) ? data.columns : [];
                            setConfig(importConfigPreset(saved.config, cols));
                        }
                        return;
                    }
                }
            } catch (_e) {
                // Fall through to latest session restore.
            }

            try {
                const latest = await requestJSON(`${API}/api/session/latest`);
                if (!cancelled) hydrateSessionState(latest);
            } catch (_e) {
                // No persisted session to restore.
            }
        };

        restore();
        return () => {
            cancelled = true;
        };
    }, [hydrateSessionState]);

    useEffect(() => {
        if (!sessionId) return;
        saveWorkspaceRestoreState({
            sessionId,
            config: exportConfigPreset(config),
            updatedAt: new Date().toISOString(),
        });
    }, [sessionId, configSignature]);

    useEffect(() => {
        if (!sessionId) return;
        let cancelled = false;
        const timer = setTimeout(async () => {
            try {
                const fd = new FormData();
                fd.append('sessionId', sessionId);
                appendConfigFormData(fd, config);
                await requestJSON(`${API}/api/session/config`, { method: 'POST', body: fd });
            } catch (_e) {
                if (!cancelled) {
                    // Keep UI responsive even when config persistence fails.
                }
            }
        }, 260);

        return () => {
            cancelled = true;
            clearTimeout(timer);
        };
    }, [sessionId, configSignature, config]);

    const openDrawer = useCallback((state) => {
        if (!hasDataset && state !== DrawerState.ACTIVITY) return;
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
        const hasPersistedConfig = !!(data?.workspaceConfig && typeof data.workspaceConfig === 'object');
        const nextConfig = hasPersistedConfig
            ? importConfigPreset(data.workspaceConfig, nextSession.columns)
            : (() => {
                const fallback = defaultWorkspaceConfig(nextSession.columns);
                const guessed = guessDomainColumn(nextSession.columns);
                fallback.domField = guessed;
                fallback.domChk = !!guessed;
                return fallback;
            })();

        setSession(nextSession);
        setExportColumns((nextSession.columns || []).map(col => col?.name).filter(Boolean));
        setConfig(nextConfig);
        setEstimate(null);
        setSelectedRow(null);
        setDrawerState(DrawerState.NONE);
        setViewSearch('');
        setViewFilters([]);
        setViewSort({ column: null, direction: null });
        setColumnPrefs({ hidden: {}, labels: {}, formats: {} });
        setShowImportModal(false);

        if (data?.activeRun) {
            setRunProgressBySession(prev => ({ ...prev, [nextSession.sessionId]: data.activeRun }));
        }
        setScrapeProgressBySession(prev => ({
            ...prev,
            [nextSession.sessionId]: data?.activeScrape || { status: 'idle', stage: 'idle', progress: 0 },
        }));
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

    const applyRunCompletion = useCallback((targetSessionId, donePayload) => {
        if (!donePayload?.result) return;
        const ctx = runContextRef.current[targetSessionId] || {};
        const summary = RunSummary(donePayload.result);
        summary._configSignature = ctx.signatureAtRunStart || configSignature;
        setRunSummaries(prev => ({ ...prev, [targetSessionId]: summary }));

        const completedAt = new Date().toISOString();
        const historyOutcome = appendRecentRun({
            id: `${targetSessionId}:${donePayload?.runId || completedAt}`,
            runId: donePayload?.runId || '',
            sessionId: targetSessionId,
            fileName: ctx.fileName || session?.fileName || 'dataset.csv',
            fileNames: ctx.fileNames || (Array.isArray(session?.fileNames) ? session.fileNames : []),
            totalRows: summary.totalRows || ctx.totalRows || session?.totalRows || 0,
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
        if (currentSessionIdRef.current === targetSessionId) closeDrawer();
    }, [closeDrawer, configSignature, session?.fileName, session?.fileNames, session?.totalRows]);

    const onRun = useCallback(async () => {
        if (!sessionId || !canRun) return;
        const targetSessionId = sessionId;
        setError('');

        runContextRef.current[targetSessionId] = {
            signatureAtRunStart: configSignature,
            fileName: session?.fileName || 'dataset.csv',
            fileNames: Array.isArray(session?.fileNames) ? session.fileNames : [],
            totalRows: session?.totalRows || 0,
        };

        setRunProgressBySession(prev => ({
            ...prev,
            [targetSessionId]: {
                status: 'running',
                stage: 'starting',
                progress: 0,
                processedRows: 0,
                totalRows: session?.totalRows || 0,
                qualifiedCount: 0,
                removedCount: 0,
                removedBreakdown: { removedFilter: 0, removedDomain: 0, removedHubspot: 0 },
            },
        }));

        try {
            const fd = new FormData();
            fd.append('sessionId', targetSessionId);
            appendConfigFormData(fd, config);
            const startPayload = await requestJSON(`${API}/api/session/qualify/start`, { method: 'POST', body: fd });
            setRunProgressBySession(prev => ({ ...prev, [targetSessionId]: startPayload }));
        } catch (e) {
            setError(e.message || 'Qualification failed to start.');
            setRunProgressBySession(prev => ({
                ...prev,
                [targetSessionId]: {
                    ...(prev[targetSessionId] || {}),
                    status: 'error',
                    stage: 'error',
                    progress: 1,
                    message: e?.message || 'Qualification failed to start.',
                    error: e?.message || 'Qualification failed to start.',
                },
            }));
        }
    }, [sessionId, canRun, config, configSignature, session?.fileName, session?.fileNames, session?.totalRows]);

    const onPauseRun = useCallback(async () => {
        if (!sessionId) return;
        try {
            const fd = new FormData();
            fd.append('sessionId', sessionId);
            const payload = await requestJSON(`${API}/api/session/qualify/pause`, { method: 'POST', body: fd });
            setRunProgressBySession(prev => ({ ...prev, [sessionId]: payload }));
            if (payload?.status === 'done' && payload?.result) applyRunCompletion(sessionId, payload);
        } catch (e) {
            setError(e.message || 'Failed to pause qualification.');
        }
    }, [sessionId, applyRunCompletion]);

    const onResumeRun = useCallback(async () => {
        if (!sessionId) return;
        try {
            runContextRef.current[sessionId] = {
                signatureAtRunStart: configSignature,
                fileName: session?.fileName || 'dataset.csv',
                fileNames: Array.isArray(session?.fileNames) ? session.fileNames : [],
                totalRows: session?.totalRows || 0,
            };
            const fd = new FormData();
            fd.append('sessionId', sessionId);
            const payload = await requestJSON(`${API}/api/session/qualify/resume`, { method: 'POST', body: fd });
            setRunProgressBySession(prev => ({ ...prev, [sessionId]: payload }));
        } catch (e) {
            setError(e.message || 'Failed to resume qualification.');
        }
    }, [sessionId, configSignature, session?.fileName, session?.fileNames, session?.totalRows]);

    const onFinishRun = useCallback(async () => {
        if (!sessionId) return;
        try {
            const fd = new FormData();
            fd.append('sessionId', sessionId);
            const payload = await requestJSON(`${API}/api/session/qualify/finish`, { method: 'POST', body: fd });
            setRunProgressBySession(prev => ({ ...prev, [sessionId]: payload }));
            if (payload?.status === 'done' && payload?.result) applyRunCompletion(sessionId, payload);
        } catch (e) {
            setError(e.message || 'Failed to finish paused qualification.');
        }
    }, [sessionId, applyRunCompletion]);

    const applyScrapeCompletion = useCallback(async (targetSessionId) => {
        try {
            const data = await requestJSON(`${API}/api/session/state?sessionId=${encodeURIComponent(targetSessionId)}`);
            const nextSession = WorkspaceSession(data);
            if (currentSessionIdRef.current === targetSessionId) {
                setSession(nextSession);
                setExportColumns((nextSession.columns || []).map(col => col?.name).filter(Boolean));
                setConfig(curr => importConfigPreset(exportConfigPreset(curr), nextSession.columns || []));
            }
            setRunSummaries(prev => {
                const next = { ...prev };
                delete next[targetSessionId];
                return next;
            });
            setEstimate(null);
        } catch (e) {
            setError(e?.message || 'Scrape completed, but session refresh failed.');
        }
    }, []);

    const onStartScrape = useCallback(async () => {
        if (!sessionId) return;
        setError('');
        setScrapeProgressBySession(prev => ({
            ...prev,
            [sessionId]: {
                status: 'running',
                stage: 'starting',
                progress: 0,
                message: 'Preparing homepage scraping job...',
                processed: 0,
                total: 0,
                ok: 0,
                fail: 0,
            },
        }));
        try {
            const fd = new FormData();
            fd.append('sessionId', sessionId);
            fd.append('domainField', config?.domField || '');
            const payload = await requestJSON(`${API}/api/session/scrape/start`, { method: 'POST', body: fd });
            setScrapeProgressBySession(prev => ({ ...prev, [sessionId]: payload }));
        } catch (e) {
            setError(e.message || 'Failed to start homepage scraper.');
            setScrapeProgressBySession(prev => ({
                ...prev,
                [sessionId]: {
                    ...(prev[sessionId] || {}),
                    status: 'error',
                    stage: 'error',
                    progress: 1,
                    message: e?.message || 'Failed to start homepage scraper.',
                    error: e?.message || 'Failed to start homepage scraper.',
                },
            }));
        }
    }, [sessionId, config?.domField]);

    useEffect(() => {
        if (!sessionId) return;
        const status = String(runProgress?.status || '');
        if (!['running', 'pausing'].includes(status)) return;

        let cancelled = false;
        const poll = async () => {
            while (!cancelled) {
                try {
                    const progress = await requestJSON(`${API}/api/session/qualify/progress?sessionId=${encodeURIComponent(sessionId)}`);
                    if (cancelled) return;
                    setRunProgressBySession(prev => ({ ...prev, [sessionId]: progress }));

                    if (progress?.status === 'done') {
                        if (progress?.result) applyRunCompletion(sessionId, progress);
                        return;
                    }
                    if (progress?.status === 'error' || progress?.status === 'paused') {
                        return;
                    }
                } catch (e) {
                    if (!cancelled) {
                        setError(e?.message || 'Failed to read qualification progress.');
                    }
                    return;
                }
                await new Promise(resolve => setTimeout(resolve, 350));
            }
        };

        poll();
        return () => {
            cancelled = true;
        };
    }, [sessionId, runProgress?.status, applyRunCompletion]);

    useEffect(() => {
        if (!sessionId) return;
        const status = String(scrapeProgress?.status || '');
        if (status !== 'running') return;

        let cancelled = false;
        const poll = async () => {
            while (!cancelled) {
                try {
                    const progress = await requestJSON(`${API}/api/session/scrape/progress?sessionId=${encodeURIComponent(sessionId)}`);
                    if (cancelled) return;
                    setScrapeProgressBySession(prev => ({ ...prev, [sessionId]: progress }));
                    if (progress?.status === 'done') {
                        await applyScrapeCompletion(sessionId);
                        return;
                    }
                    if (progress?.status === 'error') {
                        return;
                    }
                } catch (e) {
                    if (!cancelled) {
                        setError(e?.message || 'Failed to read scraper progress.');
                    }
                    return;
                }
                await new Promise(resolve => setTimeout(resolve, 400));
            }
        };

        poll();
        return () => {
            cancelled = true;
        };
    }, [sessionId, scrapeProgress?.status, applyScrapeCompletion]);

    useEffect(() => {
        if (!sessionId) return;
        let cancelled = false;
        const hydrateProgress = async () => {
            try {
                const progress = await requestJSON(`${API}/api/session/qualify/progress?sessionId=${encodeURIComponent(sessionId)}`);
                if (cancelled) return;
                setRunProgressBySession(prev => ({ ...prev, [sessionId]: progress }));
                if (progress?.status === 'done' && progress?.result) {
                    applyRunCompletion(sessionId, progress);
                }
            } catch (_e) {
                // Ignore when there is no active run snapshot.
            }
        };
        hydrateProgress();
        return () => {
            cancelled = true;
        };
    }, [sessionId, applyRunCompletion]);

    useEffect(() => {
        if (!sessionId) return;
        let cancelled = false;
        const hydrateScrape = async () => {
            try {
                const progress = await requestJSON(`${API}/api/session/scrape/progress?sessionId=${encodeURIComponent(sessionId)}`);
                if (cancelled) return;
                setScrapeProgressBySession(prev => ({ ...prev, [sessionId]: progress }));
            } catch (_e) {
                // Ignore when no scrape snapshot exists.
            }
        };
        hydrateScrape();
        return () => {
            cancelled = true;
        };
    }, [sessionId]);

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
    const updateViewFilter = (id, patch) => setViewFilters(curr => curr.map(f => (f.id === id ? { ...f, ...patch } : f)));

    const currentViewSnapshot = useCallback(() => ({
        search: String(viewSearch || ''),
        sort: {
            column: viewSort?.column || null,
            direction: viewSort?.direction || null,
        },
        filters: (viewFilters || []).map(filter => ({
            field: String(filter?.field || ''),
            op: String(filter?.op || 'contains'),
            value: String(filter?.value || ''),
            value2: String(filter?.value2 || ''),
        })),
    }), [viewSearch, viewSort?.column, viewSort?.direction, JSON.stringify(viewFilters)]);

    const applySavedView = useCallback((view) => {
        if (!view) {
            setViewSearch('');
            setViewSort({ column: null, direction: null });
            setViewFilters([]);
            return;
        }
        setViewSearch(String(view.search || ''));
        setViewSort({
            column: view?.sort?.column || null,
            direction: view?.sort?.direction || null,
        });
        setViewFilters((view.filters || []).map(filter => ViewFilter(filter)));
    }, []);

    const onSelectView = useCallback((viewId) => {
        const id = String(viewId || 'default');
        setActiveViewId(id);
        if (id === 'default') {
            applySavedView(null);
            return;
        }
        const target = (savedViews || []).find(view => view.id === id);
        applySavedView(target || null);
    }, [savedViews, applySavedView]);

    const onCreateView = useCallback(() => {
        if (!sessionId) return;
        const defaultName = activeViewId === 'default' ? 'New view' : 'Copy view';
        const name = typeof window !== 'undefined'
            ? window.prompt('Name this view', defaultName)
            : defaultName;
        const clean = String(name || '').trim();
        if (!clean) return;
        const snapshot = currentViewSnapshot();
        const next = {
            id: `view_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`,
            name: clean,
            ...snapshot,
        };
        setSavedViews(curr => [next, ...(curr || []).filter(view => view.id !== next.id)]);
        setActiveViewId(next.id);
    }, [sessionId, currentViewSnapshot, activeViewId]);

    const onSaveActiveView = useCallback(() => {
        if (!sessionId) return;
        if (activeViewId === 'default') {
            onCreateView();
            return;
        }
        const snapshot = currentViewSnapshot();
        setSavedViews(curr => (curr || []).map(view => (
            view.id === activeViewId
                ? { ...view, ...snapshot }
                : view
        )));
    }, [sessionId, activeViewId, currentViewSnapshot, onCreateView]);

    const onDeleteActiveView = useCallback(() => {
        if (!sessionId || activeViewId === 'default') return;
        if (typeof window !== 'undefined' && !window.confirm('Delete this view?')) return;
        setSavedViews(curr => (curr || []).filter(view => view.id !== activeViewId));
        setActiveViewId('default');
        applySavedView(null);
    }, [sessionId, activeViewId, applySavedView]);

    const hideColumn = useCallback((columnName) => {
        if (!columnName) return;
        setColumnPrefs(curr => ({ ...curr, hidden: { ...(curr.hidden || {}), [columnName]: true } }));
        setViewSort(curr => (curr?.column === columnName ? { column: null, direction: null } : curr));
    }, []);

    const showAllColumns = useCallback(() => {
        setColumnPrefs(curr => ({ ...curr, hidden: {} }));
    }, []);

    const renameColumn = useCallback((columnName, label) => {
        if (!columnName) return;
        const trimmed = String(label || '').trim();
        setColumnPrefs(curr => {
            const nextLabels = { ...(curr.labels || {}) };
            if (!trimmed || trimmed === columnName) delete nextLabels[columnName];
            else nextLabels[columnName] = trimmed;
            return { ...curr, labels: nextLabels };
        });
    }, []);

    const formatColumn = useCallback((columnName, format) => {
        if (!columnName) return;
        const normalized = ['auto', 'text', 'number', 'currency', 'url'].includes(String(format || '').toLowerCase())
            ? String(format || '').toLowerCase()
            : 'auto';
        setColumnPrefs(curr => {
            const nextFormats = { ...(curr.formats || {}) };
            if (normalized === 'auto') delete nextFormats[columnName];
            else nextFormats[columnName] = normalized;
            return { ...curr, formats: nextFormats };
        });
    }, []);

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

    const onSelectRow = (row, options = {}) => {
        setSelectedRow(row);
        if (options?.openInspector) setDrawerState(DrawerState.ROW_INSPECTOR);
    };

    const activityQuery = String(jumpQuery || '').trim().toLowerCase();
    const filteredActivity = useMemo(() => {
        if (!activityQuery) return runHistory || [];
        return (runHistory || []).filter(item => {
            const fileName = String(item?.fileName || '').toLowerCase();
            const stats = `${item?.qualifiedCount || 0} ${item?.removedCount || 0}`.toLowerCase();
            return fileName.includes(activityQuery) || stats.includes(activityQuery);
        });
    }, [runHistory, activityQuery]);

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
        [DrawerState.ACTIVITY]: {
            title: 'Activity',
            subtitle: 'Recent runs and restorable sessions.',
        },
        [DrawerState.ROW_INSPECTOR]: {
            title: 'Row inspector',
            subtitle: 'Inspect row status, reason chain, and full values.',
        },
    };

    const drawerContent = drawerState === DrawerState.ROW_INSPECTOR
        ? <InspectorDrawer row={selectedRow} />
        : drawerState === DrawerState.ACTIVITY
            ? (
                <div className="drawer-section drawer-stack">
                    <section className="sheet-block">
                        <div className="mini-card-title">Recent runs</div>
                        <div className="inline-help">
                            <I.info /> Select a run to restore its session and continue working from the grid.
                        </div>
                    </section>
                    <section className="sheet-block">
                        <div className="search-wrap">
                            <I.search />
                            <input
                                type="text"
                                className="search-input"
                                value={jumpQuery}
                                onChange={e => setJumpQuery(e.target.value)}
                                placeholder="Search by file name or counts"
                                aria-label="Search recent runs"
                            />
                        </div>
                    </section>
                    <section className="sheet-block">
                        <div className="activity-list">
                            {filteredActivity.length === 0 && (
                                <div className="rail-empty">No recent runs match your search.</div>
                            )}
                            {filteredActivity.map(item => (
                                <button
                                    key={item.id}
                                    type="button"
                                    className="activity-item"
                                    onClick={() => onSelectRecentRun(item)}
                                >
                                    <span className="activity-title" title={item.fileName}>{item.fileName}</span>
                                    <span className="activity-meta">
                                        {(item.qualifiedCount || 0).toLocaleString()} qualified · {(item.removedCount || 0).toLocaleString()} removed
                                    </span>
                                    <span className="activity-meta">
                                        {(item.totalRows || 0).toLocaleString()} rows · {new Date(item.completedAt || Date.now()).toLocaleString()}
                                    </span>
                                </button>
                            ))}
                        </div>
                    </section>
                </div>
            )
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
                scrapeProgress={scrapeProgress}
                onStartScrape={onStartScrape}
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
                        scrapeProgress={scrapeProgress}
                        onOpenDrawer={openDrawer}
                        onCloseDrawer={closeDrawer}
                        onRun={onRun}
                        onPauseRun={onPauseRun}
                        onResumeRun={onResumeRun}
                        onFinishRun={onFinishRun}
                        onExport={onExport}
                        onOpenImportModal={() => setShowImportModal(true)}
                        viewSearch={viewSearch}
                        onViewSearch={setViewSearch}
                        viewFilters={viewFilters}
                        onAddViewFilter={addViewFilter}
                        onUpdateViewFilter={updateViewFilter}
                        onRemoveViewFilter={removeViewFilter}
                        onClearViewFilters={() => setViewFilters([])}
                        onPromoteViewFilters={promoteViewFilters}
                        viewSort={viewSort}
                        onViewSort={setViewSort}
                        savedViews={savedViews}
                        activeViewId={activeViewId}
                        onSelectView={onSelectView}
                        onCreateView={onCreateView}
                        onSaveActiveView={onSaveActiveView}
                        onDeleteActiveView={onDeleteActiveView}
                        totalColumnCount={totalColumnCount}
                        visibleColumnCount={visibleColumnCount}
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
                        onAddViewFilter={addViewFilter}
                        selectedRowId={selectedRow?._rowId}
                        onSelectRow={onSelectRow}
                        columnPrefs={columnPrefs}
                        onHideColumn={hideColumn}
                        onShowAllColumns={showAllColumns}
                        onRenameColumn={renameColumn}
                        onFormatColumn={formatColumn}
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
                railExpanded={railExpanded}
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
