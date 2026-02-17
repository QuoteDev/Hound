/* ── Import Step ───────────────────────────────────────────── */
function ImportStep({
    onFile,
    loading,
    uploadError,
    file,
    columns,
    columnProfiles,
    previewRows,
    totalRows,
    onContinue,
}) {
    const inputRef = useRef();
    const [over, setOver] = useState(false);

    const onDrop = (event) => {
        event.preventDefault();
        setOver(false);
        const f = event.dataTransfer.files?.[0];
        if (f) onFile(f);
    };

    const hasPreview = columns.length > 0 && previewRows.length > 0;

    return (
        <section>
            <div className="pg-head">
                <h1>Import your leads</h1>
                <p>Upload a CSV to profile your dataset and prepare qualification filters.</p>
            </div>

            <div className="import-grid">
                <div className="pnl import-upload-panel">
                    <div className="pnl-body">
                        <div
                            className={`dz ${over ? 'over' : ''}`}
                            onDragOver={(event) => { event.preventDefault(); setOver(true); }}
                            onDragLeave={() => setOver(false)}
                            onDrop={onDrop}
                            onClick={() => inputRef.current?.click()}
                            role="button"
                            tabIndex={0}
                            onKeyDown={(event) => { if (event.key === 'Enter' || event.key === ' ') inputRef.current?.click(); }}
                            aria-label="Upload CSV"
                        >
                            <div className="dz-ic"><I.upload /></div>
                            <h3>{loading ? <><span className="spinner" /> Parsing CSV...</> : 'Drop CSV file here'}</h3>
                            <div className="dz-sub">or <strong>browse files</strong> to upload</div>
                            <input
                                ref={inputRef}
                                type="file"
                                accept=".csv,.tsv,text/csv,text/tab-separated-values"
                                onChange={e => e.target.files?.[0] && onFile(e.target.files[0])}
                            />
                        </div>

                        <div className="validation-list">
                            <p><I.info /> Accepted format: `.csv`</p>
                            <p><I.info /> First 8 rows will be previewed instantly</p>
                            <p><I.info /> Column types are inferred for faster filtering</p>
                        </div>

                        {uploadError && <div className="inline-msg err"><I.alertTri /> {uploadError}</div>}

                        {file && columns.length > 0 && (
                            <div className="inline-msg ok">
                                <I.check />
                                Ready: {file.name} · {toReadableFileSize(file.size)} · {totalRows.toLocaleString()} rows · {columns.length} columns
                            </div>
                        )}
                    </div>
                </div>

                <div className="pnl import-preview-panel">
                    <div className="pnl-head">
                        <h2><I.table /> Data preview</h2>
                        {hasPreview && <span className="badge badge-m">{Math.min(previewRows.length, 8)} sample rows</span>}
                    </div>
                    <div className="pnl-body">
                        {!hasPreview && (
                            <div className="empty">
                                <I.file style={{ width: 28, height: 28 }} />
                                <h3>Upload a CSV to preview schema</h3>
                                <p>We will show inferred field types, sample rows, and dataset shape here.</p>
                            </div>
                        )}

                        {hasPreview && (
                            <>
                                <div className="schema-chips">
                                    {columnProfiles.map(profile => (
                                        <span key={profile.name} className="schema-chip">
                                            <FieldType name={profile.name} inferredType={profile.inferredType} /> {profile.name}
                                        </span>
                                    ))}
                                </div>
                                <div className="tw preview-table">
                                    <table>
                                        <thead>
                                            <tr>
                                                {columns.map(col => <th key={col.name}>{col.name}</th>)}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {previewRows.slice(0, 8).map((row, rowIndex) => (
                                                <tr key={rowIndex}>
                                                    {columns.map(col => (
                                                        <td key={col.name} title={row[col.name]}>{renderTableCell(col.name, row[col.name])}</td>
                                                    ))}
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            </>
                        )}
                    </div>
                </div>
            </div>

            <div className="action-rail">
                <div className="action-rail-meta">
                    {file && columns.length > 0
                        ? <span>Dataset ready for configuration.</span>
                        : <span>Upload a CSV to continue.</span>}
                </div>
                <button className="btn btn-p" disabled={!file || columns.length === 0 || loading} onClick={onContinue}>
                    Continue to Configure <I.chevRight />
                </button>
            </div>
        </section>
    );
}
