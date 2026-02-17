/* ── Shared Context Drawer ─────────────────────────────────── */
function ContextDrawer({
    title,
    subtitle,
    onClose,
    children,
    footer,
}) {
    return (
        <div className="drawer-shell" role="dialog" aria-label={title || 'Context panel'}>
            <div className="drawer-head">
                <div className="drawer-head-copy">
                    <h3>{title || 'Panel'}</h3>
                    {subtitle && <p>{subtitle}</p>}
                </div>
                <button className="drawer-close" onClick={onClose} aria-label="Close panel">
                    <I.x />
                </button>
            </div>
            <div className="drawer-body">{children}</div>
            {footer && <div className="drawer-foot">{footer}</div>}
        </div>
    );
}
