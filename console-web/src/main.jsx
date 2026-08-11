import React, { useCallback, useEffect, useId, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  ArrowRight,
  Beaker,
  CheckCircle2,
  ChevronLeft,
  CircleAlert,
  Code2,
  Database,
  FileCode2,
  GitBranch,
  Inbox,
  LoaderCircle,
  Orbit,
  Save,
  Search,
  Send,
  ShieldCheck,
  SlidersHorizontal,
  Sparkles,
  Split,
  Workflow,
  X
} from "lucide-react";
import { AnimatePresence, motion } from "framer-motion";
import {
  Background,
  Controls,
  Handle,
  MarkerType,
  MiniMap,
  Position,
  ReactFlow,
  useEdgesState,
  useNodesState
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import {
  HoverCard,
  HoverCardContent,
  HoverCardPortal,
  HoverCardTrigger
} from "./components/ui/hover-card";
import "./styles.css";

const API = "";

async function request(path, options = {}) {
  const response = await fetch(`${API}${path}`, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.detail || "console_operation_failed");
  return payload;
}

const stageIcons = {
  ingestion: Inbox,
  intake_guard: ShieldCheck,
  route_decision: GitBranch,
  handoff: Workflow,
  draft: FileCode2,
  approval: CheckCircle2,
  send: Send
};

function App() {
  const [view, setView] = useState("trace");
  const [emails, setEmails] = useState({ items: [], total: 0 });
  const [filters, setFilters] = useState({ status: "", sender: "" });
  const [selectedId, setSelectedId] = useState("");
  const [trace, setTrace] = useState(null);
  const [rules, setRules] = useState([]);
  const [activeRule, setActiveRule] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [toast, setToast] = useState("");

  const loadEmails = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const params = new URLSearchParams({ page: "1", page_size: "25" });
      if (filters.status) params.set("status", filters.status);
      if (filters.sender) params.set("sender", filters.sender);
      const data = await request(`/api/emails?${params}`);
      setEmails(data);
      if (!selectedId && data.items[0]) setSelectedId(data.items[0].external_email_id);
    } catch (cause) {
      setError(cause.message);
    } finally {
      setLoading(false);
    }
  }, [filters, selectedId]);

  const loadTrace = useCallback(async () => {
    if (!selectedId) return;
    try {
      setTrace(await request(`/api/emails/${encodeURIComponent(selectedId)}/trace`));
    } catch (cause) {
      setError(cause.message);
    }
  }, [selectedId]);

  const loadRules = useCallback(async () => {
    try {
      setRules(await request("/api/rules"));
    } catch (cause) {
      setError(cause.message);
    }
  }, []);

  useEffect(() => {
    loadEmails();
  }, [loadEmails]);
  useEffect(() => {
    loadTrace();
  }, [loadTrace]);
  useEffect(() => {
    if (view === "rules") loadRules();
  }, [view, loadRules]);
  useEffect(() => {
    if (!toast) return undefined;
    const timer = window.setTimeout(() => setToast(""), 3800);
    return () => window.clearTimeout(timer);
  }, [toast]);

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark"><Orbit size={19} /></div>
          <div>
            <div className="brand-name">AI EXCHANGE</div>
            <div className="brand-subtitle">OPERATIONS CONSOLE</div>
          </div>
        </div>
        <div className="environment-chip"><span className="pulse-dot" /> LOCAL / READ ONLY</div>
        <div className="topbar-meta"><span>Durable Inbox</span><span className="meta-divider" /><span>Tier 1 Registry</span></div>
      </header>

      <div className="workspace">
        <aside className="sidebar">
          <div className="sidebar-label">Workspace</div>
          <button className={`nav-item ${view === "trace" ? "active" : ""}`} onClick={() => setView("trace")}>
            <Activity size={17} /><span>Pipeline Trace</span><span className="nav-count">{emails.total || "—"}</span>
          </button>
          <button className={`nav-item ${view === "rules" ? "active" : ""}`} onClick={() => setView("rules")}>
            <SlidersHorizontal size={17} /><span>Rule Drafts</span><span className="nav-count">{rules.length || "—"}</span>
          </button>
          <div className="sidebar-spacer" />
          <div className="system-card">
            <div className="system-card-title"><Database size={14} /> DATA ACCESS</div>
            <div className="system-status"><span className="status-orb" /> Postgres projection</div>
            <div className="system-muted">Read-only role · business tables</div>
          </div>
        </aside>

        <main className="main-content">
          {error && <div className="error-banner"><CircleAlert size={16} /> {error}<button onClick={() => setError("")}>Dismiss</button></div>}
          {view === "trace" ? (
            <TraceWorkspace
              emails={emails}
              filters={filters}
              setFilters={setFilters}
              selectedId={selectedId}
              setSelectedId={setSelectedId}
              trace={trace}
              loading={loading}
              onRefresh={loadEmails}
            />
          ) : (
            <RulesWorkspace
              rules={rules}
              activeRule={activeRule}
              setActiveRule={setActiveRule}
              onSaved={(message) => { setToast(message); loadRules(); }}
              onError={setError}
            />
          )}
        </main>
      </div>
      <AnimatePresence>
        {toast && <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: 12 }} className="toast"><CheckCircle2 size={16} /> {toast}</motion.div>}
      </AnimatePresence>
    </div>
  );
}

function TraceWorkspace({ emails, filters, setFilters, selectedId, setSelectedId, trace, loading, onRefresh }) {
  return (
    <div className="trace-layout">
      <section className="content-header">
        <div>
          <div className="eyebrow"><span className="eyebrow-line" /> OBSERVABILITY / SINGLE MESSAGE</div>
          <h1>Pipeline Trace</h1>
          <p>Replay the durable business journey behind one inbound email.</p>
        </div>
        <button className="ghost-button" onClick={onRefresh}><Activity size={15} /> Refresh projection</button>
      </section>
      <div className="trace-grid">
        <section className="email-list panel">
          <div className="panel-header">
            <div><div className="panel-title">Inbound emails</div><div className="panel-caption">{emails.total || 0} durable records</div></div>
            <Search size={16} className="muted-icon" />
          </div>
          <div className="filter-row">
            <input aria-label="Filter sender" placeholder="Filter sender" value={filters.sender} onChange={(event) => setFilters({ ...filters, sender: event.target.value })} />
            <select aria-label="Filter status" value={filters.status} onChange={(event) => setFilters({ ...filters, status: event.target.value })}>
              <option value="">All statuses</option><option value="waiting_approval">Waiting approval</option><option value="sent">Sent</option><option value="manual_review">Manual review</option><option value="no_action">No action</option>
            </select>
          </div>
          <div className="email-list-scroll">
            {loading && <div className="empty-state"><LoaderCircle className="spin" size={20} /> Loading projection…</div>}
            {!loading && !emails.items.length && <div className="empty-state"><Inbox size={24} /><span>No email projection found</span><small>Start the local console with a read-only DSN.</small></div>}
            {emails.items.map((email) => (
              <button key={email.external_email_id} className={`email-row ${selectedId === email.external_email_id ? "selected" : ""}`} onClick={() => setSelectedId(email.external_email_id)}>
                <div className="email-row-top"><span className={`status-dot status-${email.status}`} /><span className="email-status">{email.status}</span><span className="email-time">{formatTime(email.received_at)}</span></div>
                <div className="email-subject">{email.subject || "Untitled message"}</div>
                <div className="email-sender">{email.sender || email.external_email_id}</div>
                <div className="email-row-bottom">{email.route && <span className="mini-tag">{email.route}</span>}{email.tier && <span className="mini-tag tier">{email.tier}</span>}</div>
              </button>
            ))}
          </div>
        </section>
        <section className="trace-panel panel">
          {trace ? <TraceDetail trace={trace} /> : <div className="trace-empty"><Sparkles size={28} /><h2>Select an email</h2><p>Choose a durable email record to reveal its business-stage trace.</p></div>}
        </section>
      </div>
    </div>
  );
}

function TraceDetail({ trace }) {
  const nodes = trace.nodes.map((item, index) => ({
    id: item.id,
    type: "trace",
    position: { x: index * 198, y: 88 },
    data: { ...item, index }
  }));
  const edges = trace.edges.map((edge) => ({
    ...edge,
    type: "smoothstep",
    animated: trace.nodes.find((node) => node.id === edge.target)?.status === "active",
    markerEnd: { type: MarkerType.ArrowClosed, color: "#5b6b8e" },
    style: { stroke: "#33415e", strokeWidth: 1.5 }
  }));
  return (
    <div className="trace-detail">
      <div className="trace-detail-header">
        <div className="trace-title-wrap"><div className="trace-kicker">MESSAGE TRACE / {trace.current_status || "UNKNOWN"}</div><h2>{trace.subject || "Untitled message"}</h2><div className="trace-address">{trace.sender || "Unknown sender"} <span>·</span> {trace.external_email_id}</div></div>
        <div className={`route-badge route-${trace.nodes.find((node) => node.id === "route_decision")?.detail?.route || "unknown"}`}>{trace.nodes.find((node) => node.id === "route_decision")?.detail?.route || "unresolved"}</div>
      </div>
      <div className="trace-summary">
        <SummaryMetric label="Current state" value={trace.current_status || "unknown"} />
        <SummaryMetric label="Route tier" value={trace.nodes.find((node) => node.id === "route_decision")?.detail?.tier || "—"} />
        <SummaryMetric label="Inbox ID" value={trace.inbox_id ? `${trace.inbox_id.slice(0, 8)}…` : "—"} />
        <SummaryMetric label="Stages" value={`${trace.nodes.filter((node) => node.status === "completed").length} / ${trace.nodes.length}`} />
      </div>
      <div className="graph-label"><span>BUSINESS-STAGE REPLAY</span><span className="graph-hint"><Sparkles size={13} /> Hover or focus a node to inspect · click to pin</span></div>
      <div className="graph-frame">
        <ReactFlow nodes={nodes} edges={edges} nodeTypes={{ trace: TraceNode }} fitView fitViewOptions={{ padding: 0.12 }} nodesDraggable={false} nodesConnectable={false} zoomOnDoubleClick={false}>
          <Background color="#182238" gap={24} size={1} />
          <MiniMap pannable zoomable nodeColor={(node) => node.data.status === "failed" ? "#f26d78" : node.data.status === "completed" ? "#51d6a3" : "#647aa8"} maskColor="rgba(8, 11, 18, 0.72)" />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>
      <div className="trace-inspector">
        <div className="inspector-header"><span>STAGE INSPECTOR</span><span className="inspector-line" /></div>
        <div className="inspector-grid">{trace.nodes.map((node) => <StageCard key={node.id} node={node} />)}</div>
      </div>
    </div>
  );
}

function TraceNode({ data }) {
  const Icon = stageIcons[data.kind] || Workflow;
  const detailsId = `trace-node-details-${useId().replaceAll(":", "")}`;
  const [hoverOpen, setHoverOpen] = useState(false);
  const [pinned, setPinned] = useState(false);
  const lastTouchToggleAt = useRef(0);
  const open = hoverOpen || pinned;
  const closeDetails = () => {
    setPinned(false);
    setHoverOpen(false);
  };
  const togglePinned = (event) => {
    event.stopPropagation();
    if (event.type === "click" && Date.now() - lastTouchToggleAt.current < 500) return;
    setPinned((current) => {
      const next = !current;
      setHoverOpen(next);
      return next;
    });
  };
  const handlePointerDown = (event) => {
    if (event.pointerType === "touch") {
      event.preventDefault();
      lastTouchToggleAt.current = Date.now();
      togglePinned(event);
    }
  };
  const handleKeyDown = (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      togglePinned(event);
    }
  };
  return (
    <HoverCard
      open={open}
      onOpenChange={(nextOpen) => {
        if (!pinned) setHoverOpen(nextOpen);
      }}
      openDelay={180}
      closeDelay={120}
    >
      <HoverCardTrigger asChild>
        <motion.div
          data-testid={`trace-node-${data.id}`}
          role="button"
          tabIndex={0}
          data-trace-node-trigger={data.id}
          className={`flow-node flow-${data.status} nodrag nopan`}
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: data.index * 0.08 }}
          aria-controls={detailsId}
          aria-expanded={open}
          onPointerDown={handlePointerDown}
          onClick={togglePinned}
          onKeyDown={handleKeyDown}
        >
          <Handle type="target" position={Position.Left} className="flow-handle" />
          <div className="flow-node-top"><div className="flow-icon"><Icon size={15} /></div><span>{String(data.index + 1).padStart(2, "0")}</span></div>
          <div className="flow-node-label">{data.label}</div>
          <div className="flow-node-status"><span className="tiny-status" />{data.status}</div>
          <Handle type="source" position={Position.Right} className="flow-handle" />
        </motion.div>
      </HoverCardTrigger>
      <HoverCardPortal>
        <HoverCardContent
          id={detailsId}
          className="hover-card-content"
          side="bottom"
          align="start"
          sideOffset={10}
          collisionPadding={12}
          onPointerDownOutside={(event) => {
            const target = event.detail?.originalEvent?.target;
            const trigger = target instanceof Element
              ? target.closest("[data-trace-node-trigger]")
              : null;
            if (trigger?.getAttribute("data-trace-node-trigger") === data.id) return;
            closeDetails();
          }}
          onEscapeKeyDown={closeDetails}
        >
          <TraceNodeDetails node={data} onClose={closeDetails} />
        </HoverCardContent>
      </HoverCardPortal>
    </HoverCard>
  );
}

function TraceNodeDetails({ node, onClose }) {
  const detail = node.detail || {};
  const useful = Object.entries(detail).filter(([key, value]) => value !== null && value !== undefined && value !== "" && !(Array.isArray(value) && !value.length));
  return (
    <div className="trace-node-details" role="dialog" aria-label={`${node.label} details`}>
      <div className="hover-card-header">
        <div>
          <div className="hover-card-kicker">STAGE {String(node.index + 1).padStart(2, "0")}</div>
          <strong>{node.label}</strong>
        </div>
        <button type="button" className="hover-card-close" aria-label={`Close ${node.label} details`} onClick={onClose}>
          <X size={14} />
        </button>
      </div>
      <div className={`hover-card-status hover-card-status-${node.status}`}>
        <span className="tiny-status" /> {node.status}
      </div>
      {node.safe_error_code && <div className="hover-card-error"><CircleAlert size={13} /> {node.safe_error_code}</div>}
      <div className="hover-card-details">
        {useful.length ? useful.map(([key, value]) => (
          <div className="hover-card-detail" key={key}>
            <span>{key.replaceAll("_", " ")}</span>
            <strong>{formatDetail(value)}</strong>
          </div>
        )) : <span className="hover-card-empty">No additional safe detail</span>}
      </div>
      <div className="hover-card-hint">Click the node to keep this panel open</div>
    </div>
  );
}

function StageCard({ node }) {
  const detail = node.detail || {};
  const useful = Object.entries(detail).filter(([key, value]) => value !== null && value !== undefined && value !== "" && !(Array.isArray(value) && !value.length));
  return (
    <div className={`stage-card stage-${node.status}`}>
      <div className="stage-card-heading"><span className="stage-card-label">{node.label}</span><span className="stage-status">{node.status}</span></div>
      {node.safe_error_code && <div className="safe-error"><CircleAlert size={13} /> {node.safe_error_code}</div>}
      {useful.map(([key, value]) => <div className="detail-line" key={key}><span>{key.replaceAll("_", " ")}</span><strong>{formatDetail(value)}</strong></div>)}
    </div>
  );
}

function SummaryMetric({ label, value }) {
  return <div className="summary-metric"><span>{label}</span><strong>{value}</strong></div>;
}

function RulesWorkspace({ rules, activeRule, setActiveRule, onSaved, onError }) {
  const [showEditor, setShowEditor] = useState(false);
  return (
    <div className="rules-layout">
      <section className="content-header">
        <div><div className="eyebrow"><span className="eyebrow-line" /> GOVERNANCE / TIER 1 REGISTRY</div><h1>Rule Drafts</h1><p>Author deterministic routing intent, validate it against the production compiler, then deploy manually.</p></div>
        <button className="primary-button" onClick={() => { setActiveRule(null); setShowEditor(true); }}><Sparkles size={15} /> New rule draft</button>
      </section>
      <div className="rule-warning"><ShieldCheck size={16} /><div><strong>Local files only.</strong> Saving writes to <code>tier1_rules/</code>. The console never commits, restarts, hot-reloads, or deploys production.</div></div>
      {showEditor ? <RuleEditor rule={activeRule} onClose={() => setShowEditor(false)} onSaved={(message) => { setShowEditor(false); onSaved(message); }} onError={onError} /> : <RuleTable rules={rules} onEdit={(rule) => { setActiveRule(rule); setShowEditor(true); }} />}
    </div>
  );
}

function RuleTable({ rules, onEdit }) {
  return (
    <section className="panel rule-table-panel">
      <div className="panel-header"><div><div className="panel-title">Registry rules</div><div className="panel-caption">{rules.length} local manifests</div></div><Code2 size={16} className="muted-icon" /></div>
      {!rules.length ? <div className="empty-state rule-empty"><FileCode2 size={26} /><span>No rule manifests loaded</span><small>Create a draft to begin authoring.</small></div> : <div className="rule-table"><div className="rule-table-row rule-table-head"><span>Rule ID</span><span>Route</span><span>Status</span><span>Owner</span><span /></div>{rules.map((rule) => <button className="rule-table-row" key={rule.rule_id} onClick={() => onEdit(rule)}><span className="rule-id"><span className="rule-glyph" />{rule.rule_id}<small>v{rule.rule_version} · {rule.filename}</small></span><span className={`route-text route-text-${rule.route}`}>{rule.route}</span><span><span className={`status-pill status-pill-${rule.status}`}>{rule.status}</span></span><span className="owner-text">{rule.owner || "—"}</span><ArrowRight size={15} /></button>)}</div>}
    </section>
  );
}

function RuleEditor({ rule, onClose, onSaved, onError }) {
  const [rawMode, setRawMode] = useState(false);
  const [rawYaml, setRawYaml] = useState(rule?.manifest ? toYaml(rule.manifest) : defaultRuleYaml());
  const [fields, setFields] = useState(() => rule?.manifest ? fieldsFromManifest(rule.manifest) : defaultFields());
  const [validation, setValidation] = useState(null);
  const [saving, setSaving] = useState(false);
  const [testEmailId, setTestEmailId] = useState("");
  const [matchTest, setMatchTest] = useState(null);
  const update = (key, value) => setFields((current) => ({ ...current, [key]: value }));
  const buildManifest = () => ({
    schema_version: 1,
    rule_id: fields.rule_id,
    rule_version: Number(fields.rule_version || 1),
    status: fields.status,
    owner: fields.owner || undefined,
    purpose: fields.purpose || undefined,
    validity: { effective_from: fields.effective_from || undefined, expires_at: fields.expires_at || undefined },
    match: {
      anchor: fields.anchor_field === "sender.address"
        ? { any: [{ field: fields.anchor_field, op: "eq", value: fields.anchor_value }] }
        : { any: [{ field: fields.anchor_field, op: fields.anchor_op, values: fields.anchor_value.split(",").map((value) => value.trim()).filter(Boolean) }] },
      ...(fields.condition_value ? {
        conditions: fields.condition_op === "contains_any"
          ? { field: fields.condition_field, op: fields.condition_op, values: fields.condition_value.split(",").map((value) => value.trim()).filter(Boolean) }
          : { field: fields.condition_field, op: fields.condition_op, value: fields.condition_value }
      } : {})
    },
    decision: { route: fields.route, params: routeParams(fields) },
    governance: {
      criticality: fields.criticality || undefined,
      risk_notes: fields.risk_notes || undefined,
      positive_cases: fields.positive_cases ? JSON.parse(fields.positive_cases) : [],
      negative_cases: fields.negative_cases ? JSON.parse(fields.negative_cases) : [],
      external_recipient_acknowledged: fields.external_ack,
      full_text_match_acknowledged: fields.full_text_ack
    }
  });
  const payload = () => rawMode ? { raw_yaml: rawYaml } : { rule_id: fields.rule_id, manifest: buildManifest() };
  const validate = async () => {
    try { setValidation(await request("/api/rules/validate", { method: "POST", body: JSON.stringify(payload()) })); } catch (cause) { onError(cause.message); }
  };
  const save = async () => {
    setSaving(true);
    try {
      const response = await request("/api/rules", { method: "POST", body: JSON.stringify(payload()) });
      onSaved(response.message);
    } catch (cause) { onError(cause.message); } finally { setSaving(false); }
  };
  const runMatch = async (saveAs = null) => {
    try {
      const response = await request(`/api/rules/${encodeURIComponent(fields.rule_id)}/test-match`, {
        method: "POST",
        body: JSON.stringify({ external_email_id: testEmailId, save_as: saveAs })
      });
      setMatchTest(response);
      if (response.saved_as) onSaved(`Saved ${response.case_id} to ${response.saved_as}.`);
    } catch (cause) { onError(cause.message); }
  };
  return (
    <section className="editor-panel panel">
      <div className="editor-header"><button className="back-button" onClick={onClose}><ChevronLeft size={16} /> Registry</button><div className="editor-actions"><button className="ghost-button compact" onClick={() => setRawMode(!rawMode)}>{rawMode ? <SlidersHorizontal size={14} /> : <Code2 size={14} />}{rawMode ? "Structured form" : "Raw YAML"}</button><button className="primary-button compact" disabled={saving} onClick={save}>{saving ? <LoaderCircle className="spin" size={14} /> : <Save size={14} />} Save draft</button></div></div>
      <div className="editor-title"><div className="eyebrow"><span className="eyebrow-line" /> RULE DRAFT / {fields.rule_id || "UNNAMED"}</div><h2>{rule ? "Edit Rule Manifest" : "Author a routing rule"}</h2><p>All enabled rules must pass the same schema, fixture, regex, overlap, and external-recipient checks as deployment.</p></div>
      {rawMode ? <textarea className="yaml-editor" value={rawYaml} onChange={(event) => setRawYaml(event.target.value)} spellCheck="false" /> : <div className="structured-form"><FormSection title="Identity"><div className="form-grid"><Field label="Rule ID" value={fields.rule_id} onChange={(value) => update("rule_id", value)} placeholder="sender-finance-001" /><Field label="Version" value={fields.rule_version} onChange={(value) => update("rule_version", value)} /><Field label="Status" type="select" options={["proposed", "enabled", "retired"]} value={fields.status} onChange={(value) => update("status", value)} /><Field label="Criticality" type="select" options={["", "P0", "P1", "P2", "P3"]} value={fields.criticality} onChange={(value) => update("criticality", value)} /><Field label="Owner" value={fields.owner} onChange={(value) => update("owner", value)} /><Field label="Purpose" value={fields.purpose} onChange={(value) => update("purpose", value)} wide /></div></FormSection><FormSection title="Match anchor"><div className="form-grid"><Field label="Anchor field" type="select" options={["sender.address", "to.addresses", "cc.addresses"]} value={fields.anchor_field} onChange={(value) => update("anchor_field", value)} /><Field label="Anchor operator" type="select" options={fields.anchor_field === "sender.address" ? ["eq", "in"] : ["has_any", "has_all"]} value={fields.anchor_op} onChange={(value) => update("anchor_op", value)} /><Field label="Value(s), comma-separated" value={fields.anchor_value} onChange={(value) => update("anchor_value", value)} placeholder="sender@example.com" /></div><div className="condition-row"><span className="condition-prefix">AND content</span><Field label="Field" type="select" options={["subject", "body.current_text", "body.full_text"]} value={fields.condition_field} onChange={(value) => update("condition_field", value)} /><Field label="Operator" type="select" options={["contains", "contains_any", "regex"]} value={fields.condition_op} onChange={(value) => update("condition_op", value)} /><Field label="Value(s), optional" value={fields.condition_value} onChange={(value) => update("condition_value", value)} /></div></FormSection><FormSection title="Decision"><div className="form-grid"><Field label="Canonical route" type="select" options={["reply", "forward", "read_only", "no_action", "manual_review"]} value={fields.route} onChange={(value) => update("route", value)} /><Field label="Reply mode" type="select" options={["sender_only", "sender_and_original_cc"]} value={fields.reply_mode} onChange={(value) => update("reply_mode", value)} /><Field label="Fixed recipients (comma-separated)" value={fields.fixed_recipients} onChange={(value) => update("fixed_recipients", value)} wide /><Field label="Reason code" value={fields.reason_code} onChange={(value) => update("reason_code", value)} /></div></FormSection><FormSection title="Governance fixtures"><div className="form-grid"><Field label="Positive cases JSON" value={fields.positive_cases} onChange={(value) => update("positive_cases", value)} wide placeholder='[{"case_id":"p1","email":{...}}]' /><Field label="Negative cases JSON" value={fields.negative_cases} onChange={(value) => update("negative_cases", value)} wide placeholder='[{"case_id":"n1","email":{...}}]' /><label className="check-field"><input type="checkbox" checked={fields.external_ack} onChange={(event) => update("external_ack", event.target.checked)} /> External recipient acknowledged</label><label className="check-field"><input type="checkbox" checked={fields.full_text_ack} onChange={(event) => update("full_text_ack", event.target.checked)} /> Full text match acknowledged</label></div></FormSection></div>}
      <div className="validation-row"><button className="validate-button" onClick={validate}><Beaker size={15} /> Validate against compiler</button>{validation && <ValidationResult result={validation} />}</div>
      <div className="sandbox-row"><div><div className="form-section-title"><Beaker size={12} /> TEST MATCH SANDBOX</div><p>Run the real matcher against a historical email projection. No message body or attachment is returned.</p></div><div className="sandbox-actions"><input value={testEmailId} onChange={(event) => setTestEmailId(event.target.value)} placeholder="external email id" /><button className="ghost-button compact" disabled={!testEmailId || !fields.rule_id} onClick={() => runMatch()}>Run match</button>{matchTest && <span className={`match-result match-${matchTest.result.toLowerCase()}`}>{matchTest.result}</span>}{matchTest?.result === "MATCHED" && <button className="mini-action" onClick={() => runMatch("positive_cases")}>Save positive</button>}{matchTest?.result === "NOT_MATCHED" && <button className="mini-action" onClick={() => runMatch("negative_cases")}>Save negative</button>}</div></div>
      <div className="editor-footnote"><ShieldCheck size={14} /> Save is local-only. After saving: commit the file, run the deployment pipeline, and restart the production service through the existing manual process.</div>
    </section>
  );
}

function FormSection({ title, children }) { return <div className="form-section"><div className="form-section-title">{title}</div>{children}</div>; }
function Field({ label, value, onChange, type = "text", options = [], wide = false, placeholder = "" }) {
  return <label className={`field ${wide ? "field-wide" : ""}`}><span>{label}</span>{type === "select" ? <select value={value} onChange={(event) => onChange(event.target.value)}>{options.map((option) => <option key={option} value={option}>{option || "—"}</option>)}</select> : <input value={value} onChange={(event) => onChange(event.target.value)} placeholder={placeholder} />}</label>;
}
function ValidationResult({ result }) { return <div className={`validation-result ${result.valid ? "valid" : "invalid"}`}><span>{result.valid ? <CheckCircle2 size={15} /> : <CircleAlert size={15} />} {result.valid ? `Valid · ${result.enabled_rule_count} enabled rules` : `${result.errors.length} compiler issue${result.errors.length === 1 ? "" : "s"}`}</span>{!result.valid && <details><summary>View issues</summary>{result.errors.map((issue) => <div key={`${issue.code}-${issue.rule_id}`}><strong>{issue.code}</strong> {issue.message}</div>)}</details>}</div>; }

function defaultFields() { return { rule_id: "", rule_version: "1", status: "proposed", owner: "", purpose: "", criticality: "P2", anchor_field: "sender.address", anchor_op: "eq", anchor_value: "", condition_field: "subject", condition_op: "contains", condition_value: "", route: "reply", reply_mode: "sender_only", fixed_recipients: "", reason_code: "console_rule", positive_cases: "[]", negative_cases: "[]", external_ack: false, full_text_ack: false, effective_from: "", expires_at: "", risk_notes: "" }; }
function fieldsFromManifest(manifest) { const fields = defaultFields(); const anchor = manifest.match?.anchor?.any?.[0] || manifest.match?.anchor?.all?.[0] || {}; const condition = manifest.match?.conditions || {}; const params = manifest.decision?.params || {}; return { ...fields, rule_id: manifest.rule_id || "", rule_version: String(manifest.rule_version || 1), status: manifest.status || "proposed", owner: manifest.owner || "", purpose: manifest.purpose || "", criticality: manifest.governance?.criticality || "", anchor_field: anchor.field || fields.anchor_field, anchor_op: anchor.op || fields.anchor_op, anchor_value: anchor.value || (anchor.values || []).join(", "), condition_field: condition.field || fields.condition_field, condition_op: condition.op || fields.condition_op, condition_value: condition.value || (condition.values || []).join(", "), route: manifest.decision?.route || "reply", reply_mode: params.reply_mode || fields.reply_mode, fixed_recipients: (params.fixed_recipients || []).join(", "), reason_code: params.reason_code || "", positive_cases: JSON.stringify(manifest.governance?.positive_cases || []), negative_cases: JSON.stringify(manifest.governance?.negative_cases || []), external_ack: Boolean(manifest.governance?.external_recipient_acknowledged), full_text_ack: Boolean(manifest.governance?.full_text_match_acknowledged), effective_from: manifest.validity?.effective_from || "", expires_at: manifest.validity?.expires_at || "" }; }
function routeParams(fields) { if (fields.route === "forward") return { fixed_recipients: fields.fixed_recipients.split(",").map((value) => value.trim()).filter(Boolean), cc: [], allow_recipient_edit: true, include_attachments: false }; if (fields.route === "reply") return { reply_mode: fields.reply_mode }; if (fields.route === "no_action" || fields.route === "manual_review") return { reason_code: fields.reason_code || "console_rule" }; return {}; }
function defaultRuleYaml() { return `schema_version: 1\nrule_id: example-rule-001\nrule_version: 1\nstatus: proposed\nowner: operator\npurpose: Describe why this rule exists.\nmatch:\n  anchor:\n    any:\n      - field: sender.address\n        op: eq\n        value: sender@example.com\ndecision:\n  route: reply\n  params:\n    reply_mode: sender_only\ngovernance:\n  positive_cases: []\n  negative_cases: []\n`; }
function toYaml(value) { return Object.entries(value).map(([key, item]) => `${key}: ${typeof item === "object" ? JSON.stringify(item) : item}`).join("\n"); }
function formatTime(value) { if (!value) return "—"; return new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }).format(new Date(value)); }
function formatDetail(value) { if (typeof value === "object") return JSON.stringify(value); return String(value); }

export { App, TraceNode, TraceNodeDetails };

const rootElement = document.getElementById("root");
if (rootElement) createRoot(rootElement).render(<App />);
