/* ── Workspace Shell (Table-first + Context Drawer) ────────── */
function WorkspaceShell({
    leftRail,
    header,
    mainContent,
    drawer,
    drawerOpen,
    railExpanded,
}) {
    return (
        <div className={`ws-root ${drawerOpen ? 'drawer-open' : ''} ${railExpanded ? 'rail-expanded' : ''}`}>
            <aside className="ws-rail">
                {leftRail}
            </aside>
            <section className="ws-main">
                <div className="ws-header">{header}</div>
                <main className="ws-canvas">{mainContent}</main>
            </section>
            <aside className={`ws-drawer ${drawerOpen ? 'open' : ''}`}>
                {drawer}
            </aside>
        </div>
    );
}
