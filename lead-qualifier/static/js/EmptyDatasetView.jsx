/* ── Import Modal (In-context) ────────────────────────────── */
function EmptyDatasetView({ open, loading, canClose, onClose, onUploadSource }) {
    const sourceRef = useRef();
    const dedupeRef = useRef();

    const [sourceFiles, setSourceFiles] = useState([]);
    const [dedupeFiles, setDedupeFiles] = useState([]);
    const [isOver, setIsOver] = useState(false);

    useEffect(() => {
        if (!open) {
            setSourceFiles([]);
            setDedupeFiles([]);
            setIsOver(false);
        }
    }, [open]);

    if (!open) return null;

    const onDropSource = (event) => {
        event.preventDefault();
        setIsOver(false);
        const files = Array.from(event.dataTransfer.files || []);
        if (!files.length) return;
        setSourceFiles(files);
    };

    const submitImport = () => {
        if (!sourceFiles.length || loading) return;
        onUploadSource(sourceFiles, dedupeFiles);
    };
    const dedupeFileNames = dedupeFiles
        .map(file => String(file?.name || '').trim())
        .filter(Boolean);

    const describeFiles = (files, emptyText) => {
        if (!files?.length) return emptyText;
        const totalBytes = files.reduce((acc, item) => acc + (item?.size || 0), 0);
        if (files.length === 1) return `${files[0].name} · ${toReadableFileSize(files[0].size)}`;
        const first = files[0]?.name || 'file';
        return `${files.length} files · ${toReadableFileSize(totalBytes)} · ${first} + ${files.length - 1} more`;
    };

    return (
        <div className="modal-backdrop" role="dialog" aria-modal="true" aria-label="Import source dataset">
            <div className="modal-panel">
                <div className="modal-head">
                    <div>
                        <h2>Import source data</h2>
                        <p>Upload a source CSV/TSV to start qualification.</p>
                    </div>
                    {canClose && (
                        <button className="drawer-close" type="button" onClick={onClose} aria-label="Close import modal">
                            <I.x />
                        </button>
                    )}
                </div>

                <div
                    className={`import-drop ${isOver ? 'is-over' : ''}`}
                    onDragOver={(event) => {
                        event.preventDefault();
                        setIsOver(true);
                    }}
                    onDragLeave={() => setIsOver(false)}
                    onDrop={onDropSource}
                    onClick={() => sourceRef.current?.click()}
                    role="button"
                    tabIndex={0}
                    onKeyDown={(event) => {
                        if (event.key === 'Enter' || event.key === ' ') {
                            event.preventDefault();
                            sourceRef.current?.click();
                        }
                    }}
                    aria-label="Drop source file"
                >
                    <span className="import-drop-icon"><I.upload /></span>
                    <span className="import-drop-copy">
                        <span className="import-drop-top">
                            <span className="import-label">Source dataset</span>
                            <span
                                className="hint-tip"
                                title="Accepted formats: CSV or TSV, up to 100MB."
                                tabIndex={0}
                                role="img"
                                aria-label="Source dataset requirements"
                            >
                                <I.info />
                            </span>
                        </span>
                        <span className="import-drop-title">
                            {loading
                                ? 'Uploading source dataset...'
                                : describeFiles(sourceFiles, 'Drop source CSV/TSV files or click to browse')}
                        </span>
                    </span>
                    <span className="import-drop-cta">{sourceFiles.length ? 'Change files' : 'Select files'}</span>
                    <input
                        ref={sourceRef}
                        type="file"
                        multiple
                        accept=".csv,.tsv,text/csv,text/tab-separated-values"
                        onChange={(event) => setSourceFiles(Array.from(event.target.files || []))}
                        style={{ display: 'none' }}
                    />
                </div>

                <div className="import-inline-grid">
                    <button type="button" className="import-chooser secondary" onClick={() => dedupeRef.current?.click()}>
                        <span>
                            <span className="import-drop-top">
                                <span className="import-label">Optional dedupe</span>
                                <span
                                    className="hint-tip"
                                    title="Optional: attach HubSpot companies CSV to remove existing records."
                                    tabIndex={0}
                                    role="img"
                                    aria-label="Dedupe information"
                                >
                                    <I.info />
                                </span>
                            </span>
                            <span className="import-file">
                                {describeFiles(dedupeFiles, 'Attach HubSpot companies CSV/TSV files')}
                            </span>
                            {dedupeFileNames.length > 0 && (
                                <span className="import-file-list" aria-label="Selected dedupe files">
                                    {dedupeFileNames.map((fileName, index) => (
                                        <span key={`${fileName}-${index}`} className="import-file-item">
                                            <I.file /> {fileName}
                                        </span>
                                    ))}
                                </span>
                            )}
                        </span>
                        <span className="import-chooser-action">{dedupeFiles.length ? 'Replace files' : 'Attach files'}</span>
                    </button>
                    <input
                        ref={dedupeRef}
                        type="file"
                        multiple
                        accept=".csv,.tsv,text/csv,text/tab-separated-values"
                        onChange={(event) => setDedupeFiles(Array.from(event.target.files || []))}
                        style={{ display: 'none' }}
                    />
                </div>

                <div className="import-cta-row">
                    <button className="btn btn-p" type="button" onClick={submitImport} disabled={!sourceFiles.length || loading}>
                        {loading && <span className="spinner" />}
                        {loading ? 'Importing dataset...' : 'Import dataset'}
                    </button>
                    <span className="cmd-note">{sourceFiles.length ? 'Ready to import.' : 'Select source files to continue.'}</span>
                </div>
            </div>
        </div>
    );
}
