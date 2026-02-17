/* ── Rule Builder Card ────────────────────────────────────── */

function TagGroup({ group, onChangeGroup, onRemoveGroup, canRemoveGroup, col, matchType }) {
    const inputRef = useRef();
    const [draft, setDraft] = useState('');
    const tags = group.tags || [];
    const isContains = matchType === 'contains';

    const addTag = (value) => {
        const t = value.trim();
        if (t && !tags.includes(t)) onChangeGroup({ ...group, tags: [...tags, t] });
    };

    const removeTag = (index) => onChangeGroup({ ...group, tags: tags.filter((_, idx) => idx !== index) });

    const handleKey = (event) => {
        if ((event.key === 'Enter' || event.key === ',' || event.key === 'Tab') && draft.trim()) {
            event.preventDefault();
            const nextTags = [...tags];
            draft.split(',').map(s => s.trim()).filter(Boolean).forEach(v => {
                if (!nextTags.includes(v)) nextTags.push(v);
            });
            onChangeGroup({ ...group, tags: nextTags });
            setDraft('');
        } else if (event.key === 'Backspace' && !draft && tags.length) {
            onChangeGroup({ ...group, tags: tags.slice(0, -1) });
        }
    };

    return (
        <div className="value-group">
            <div className="vg-header">
                {isContains && tags.length > 1 && (
                    <span className="logic-gate" aria-label="Group logic">
                        <button
                            type="button"
                            className={`logic-opt ${group.logic === 'and' ? 'active' : ''}`}
                            onClick={() => onChangeGroup({ ...group, logic: 'and' })}
                        >
                            and
                        </button>
                        <button
                            type="button"
                            className={`logic-opt ${group.logic === 'or' ? 'active' : ''}`}
                            onClick={() => onChangeGroup({ ...group, logic: 'or' })}
                        >
                            or
                        </button>
                    </span>
                )}
                <div style={{ flex: 1 }} />
                {canRemoveGroup && (
                    <button type="button" className="vg-remove" onClick={onRemoveGroup} aria-label="Remove value group">
                        <I.x />
                    </button>
                )}
            </div>

            <div className="tag-input-wrap" onClick={() => inputRef.current?.focus()}>
                {tags.map((tag, index) => (
                    <span key={index} className="val-tag">
                        {tag}
                        <button type="button" aria-label={`Remove ${tag}`} onClick={(e) => { e.stopPropagation(); removeTag(index); }}>
                            <I.x />
                        </button>
                    </span>
                ))}
                <input
                    ref={inputRef}
                    className="tag-input"
                    value={draft}
                    onChange={e => setDraft(e.target.value)}
                    onKeyDown={handleKey}
                    onBlur={() => { if (draft.trim()) { addTag(draft); setDraft(''); } }}
                    placeholder={tags.length ? '' : 'Type and press Enter'}
                    aria-label="Rule values"
                />
            </div>

            {col && col.sampleValues?.length > 0 && (
                <div className="tags" role="list" aria-label="Suggested values">
                    {col.sampleValues.filter(v => !tags.includes(v)).slice(0, 20).map((value, index) => (
                        <button type="button" key={index} className="tag" onClick={() => addTag(value)}>
                            {value}
                        </button>
                    ))}
                </div>
            )}
        </div>
    );
}

function matchTypeLabel(value) {
    const found = MATCH_TYPES.find(item => item.value === value);
    return found ? found.label : value;
}

function RuleRow({ rule, columns, columnProfiles, onChange, onRemove, canRemove }) {
    const col = columns.find(c => c.name === rule.field);
    const profile = columnProfiles.find(c => c.name === rule.field);
    const isRange = rule.matchType === 'range';
    const isDateRange = rule.matchType === 'dates';
    const isContains = rule.matchType === 'contains';
    const isFuzzy = rule.matchType === 'fuzzy' || rule.matchType === 'excludes';
    const groups = rule.groups || [RuleGroup()];

    const updateGroup = (index, nextGroup) => {
        const next = [...groups];
        next[index] = nextGroup;
        onChange({ groups: next });
    };

    const removeGroup = (index) => onChange({ groups: groups.filter((_, i) => i !== index) });
    const addGroup = () => onChange({ groups: [...groups, RuleGroup()] });

    const handleFieldChange = (event) => {
        const field = event.target.value;
        const selected = columns.find(x => x.name === field);
        const mt = guessCondition(field, selected);
        onChange({ field, matchType: mt, groups: [RuleGroup()], min: '', max: '', startDate: '', endDate: '' });
    };

    return (
        <article className={`rule-item ${(isRange || isDateRange) ? 'range' : ''}`}>
            <header className="rule-item-head">
                <div className="rule-item-summary">
                    {col ? <FieldType name={col.name} inferredType={profile?.inferredType} /> : <span className="ft ft-default">T</span>}
                    <span>{rule.field || 'Choose a field'}</span>
                    {rule.field && <span className="badge badge-m">{matchTypeLabel(rule.matchType)}</span>}
                </div>
                {canRemove && (
                    <button type="button" className="rule-x" onClick={onRemove} aria-label="Remove filter rule">
                        <I.x />
                    </button>
                )}
            </header>

            <div className="rule-item-grid">
                <div>
                    <div className="rule-lbl">Field</div>
                    <select value={rule.field} onChange={handleFieldChange} aria-label="Select field">
                        <option value="">Select column...</option>
                        {columns.map(c => <option key={c.name} value={c.name}>{c.name} ({c.uniqueCount})</option>)}
                    </select>
                </div>

                <div>
                    <div className="rule-lbl">Condition</div>
                    <select value={rule.matchType} onChange={e => onChange({ matchType: e.target.value })} aria-label="Select condition">
                        {MATCH_TYPES.map(t => <option key={t.value} value={t.value}>{t.label}</option>)}
                    </select>
                </div>
            </div>

            {isRange ? (
                <div className="rule-range-grid">
                    <div>
                        <div className="rule-lbl"><I.hash /> Min</div>
                        <input type="number" value={rule.min} onChange={e => onChange({ min: e.target.value })} placeholder="Minimum" />
                    </div>
                    <div>
                        <div className="rule-lbl"><I.hash /> Max</div>
                        <input type="number" value={rule.max} onChange={e => onChange({ max: e.target.value })} placeholder="Maximum" />
                    </div>
                </div>
            ) : isDateRange ? (
                <div className="rule-range-grid">
                    <div>
                        <div className="rule-lbl">Start date</div>
                        <input type="date" value={rule.startDate || ''} onChange={e => onChange({ startDate: e.target.value })} />
                    </div>
                    <div>
                        <div className="rule-lbl">End date</div>
                        <input type="date" value={rule.endDate || ''} onChange={e => onChange({ endDate: e.target.value })} />
                    </div>
                </div>
            ) : (
                <div className="values-shell">
                    {groups.map((group, index) => (
                        <React.Fragment key={group.id || index}>
                            {index > 0 && (
                                <div className="group-divider">
                                    <div className="group-divider-line" />
                                    <span className="logic-gate group-logic-gate" aria-label="Group merge logic">
                                        <button
                                            type="button"
                                            className={`logic-opt ${rule.groupsLogic === 'or' ? 'active' : ''}`}
                                            onClick={() => onChange({ groupsLogic: 'or' })}
                                        >
                                            or
                                        </button>
                                        <button
                                            type="button"
                                            className={`logic-opt ${rule.groupsLogic === 'and' ? 'active' : ''}`}
                                            onClick={() => onChange({ groupsLogic: 'and' })}
                                        >
                                            and
                                        </button>
                                    </span>
                                    <div className="group-divider-line" />
                                </div>
                            )}
                            <TagGroup
                                group={group}
                                onChangeGroup={(updated) => updateGroup(index, updated)}
                                onRemoveGroup={() => removeGroup(index)}
                                canRemoveGroup={groups.length > 1}
                                col={col}
                                matchType={rule.matchType}
                            />
                        </React.Fragment>
                    ))}

                    {isContains && (
                        <button type="button" className="btn btn-t group-add-btn" onClick={addGroup}>
                            <I.plus /> Add group
                        </button>
                    )}

                    {isFuzzy && (
                        <div className="thresh">
                            <span className="form-label">Similarity</span>
                            <input
                                type="range"
                                min="30"
                                max="100"
                                value={rule.threshold}
                                onChange={e => onChange({ threshold: parseInt(e.target.value, 10) })}
                                aria-label="Similarity threshold"
                            />
                            <span className="thresh-val">{rule.threshold}%</span>
                        </div>
                    )}
                </div>
            )}

            <footer className="rule-item-foot">
                <span className="meta-item"><I.columns /> Distinct values: {col?.uniqueCount || 0}</span>
                <span className="meta-item"><I.rows /> Sample values: {col?.sampleValues?.length || 0}</span>
            </footer>
        </article>
    );
}
