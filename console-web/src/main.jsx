import React, { useCallback, useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import { CheckCircle2, CircleAlert } from "lucide-react";
import { AnimatePresence, motion } from "framer-motion";
import "@xyflow/react/dist/style.css";
import { request, getTrace, listEmails, listRules } from "./lib/api";
import { labelForStatus } from "./lib/labels.zh-CN";
import { Sidebar } from "./layout/Sidebar";
import { Topbar } from "./layout/Topbar";
import { TraceWorkspace } from "./trace/TraceWorkspace";
import { TraceNode, TraceNodeDetails } from "./trace/TraceNode";
import { RulesWorkspace } from "./rules/RulesWorkspace";
import "./styles.css";

const DEFAULT_FILTERS = {
  query: "",
  sender: "",
  status: "",
  route: "",
  tier: "",
  receivedFrom: "",
  receivedTo: "",
  requiresHuman: "",
  hasAnomaly: ""
};

function readUrlState() {
  if (typeof window === "undefined") return { view: "trace", selectedId: "", selectedStage: "route_decision" };
  const params = new URLSearchParams(window.location.search);
  return {
    view: params.get("view") === "rules" ? "rules" : "trace",
    selectedId: params.get("email") || "",
    selectedStage: params.get("stage") || "route_decision"
  };
}

function useStoredBoolean(key, fallback = false) {
  const [value, setValue] = useState(() => {
    if (typeof window === "undefined") return fallback;
    return window.localStorage.getItem(key) === "true";
  });
  useEffect(() => {
    window.localStorage.setItem(key, String(value));
  }, [key, value]);
  return [value, setValue];
}

function useDebouncedValue(value, delay) {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const timer = window.setTimeout(() => setDebounced(value), delay);
    return () => window.clearTimeout(timer);
  }, [delay, value]);
  return debounced;
}

function App() {
  const urlState = useMemo(readUrlState, []);
  const [view, setView] = useState(urlState.view);
  const [emails, setEmails] = useState({ items: [], total: 0 });
  const [filters, setFilters] = useState(DEFAULT_FILTERS);
  const debouncedFilters = useDebouncedValue(filters, 300);
  const [selectedId, setSelectedId] = useState(urlState.selectedId);
  const [selectedStage, setSelectedStage] = useState(urlState.selectedStage);
  const [trace, setTrace] = useState(null);
  const [routeDecision, setRouteDecision] = useState(null);
  const [rules, setRules] = useState([]);
  const [activeRule, setActiveRule] = useState(null);
  const [loading, setLoading] = useState(false);
  const [traceLoading, setTraceLoading] = useState(false);
  const [error, setError] = useState("");
  const [toast, setToast] = useState("");
  const [lastUpdated, setLastUpdated] = useState("");
  const [sidebarCollapsed, setSidebarCollapsed] = useStoredBoolean("console.sidebar.collapsed");
  const [emailListCollapsed, setEmailListCollapsed] = useStoredBoolean("console.email-list.collapsed");
  const [focusMode, setFocusMode] = useStoredBoolean("console.trace.focus-mode");

  const loadEmails = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const data = await listEmails(debouncedFilters);
      setEmails(data);
      setLastUpdated(new Date().toISOString());
      setSelectedId((current) => {
        if (current && data.items.some((email) => email.external_email_id === current)) return current;
        return data.items[0]?.external_email_id || "";
      });
      return data;
    } catch (cause) {
      setError(cause.message || "邮件列表加载失败");
    } finally {
      setLoading(false);
    }
  }, [debouncedFilters]);

  const loadTrace = useCallback(async (emailId = selectedId) => {
    if (!emailId) {
      setTrace(null);
      setRouteDecision(null);
      return;
    }
    setTraceLoading(true);
    try {
      const traceData = await getTrace(emailId);
      let decision = traceData.route_decision || null;
      try {
        decision = await request(`/api/emails/${encodeURIComponent(emailId)}/route-decision`);
      } catch {
        // Older read-only projections may not expose the optional detail route.
      }
      setRouteDecision(decision);
      setTrace({ ...traceData, route_decision: decision || traceData.route_decision });
    } catch (cause) {
      setError(cause.message || "处理详情加载失败");
      setTrace(null);
    } finally {
      setTraceLoading(false);
    }
  }, [selectedId]);

  const loadRules = useCallback(async () => {
    try {
      setRules(await listRules());
    } catch (cause) {
      setError(cause.message || "规则清单加载失败");
    }
  }, []);

  const refresh = useCallback(async () => {
    const data = await loadEmails();
    const nextId = selectedId && data?.items?.some((email) => email.external_email_id === selectedId)
      ? selectedId
      : data?.items?.[0]?.external_email_id;
    if (nextId) await loadTrace(nextId);
  }, [loadEmails, loadTrace, selectedId]);

  useEffect(() => { loadEmails(); }, [loadEmails]);
  useEffect(() => { loadTrace(); }, [loadTrace]);
  useEffect(() => { if (view === "rules") loadRules(); }, [loadRules, view]);
  useEffect(() => {
    if (trace && !trace.nodes.some((node) => node.id === selectedStage)) {
      setSelectedStage(trace.nodes[0]?.id || "route_decision");
    }
  }, [selectedStage, trace]);
  useEffect(() => {
    if (!toast) return undefined;
    const timer = window.setTimeout(() => setToast(""), 3800);
    return () => window.clearTimeout(timer);
  }, [toast]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const params = new URLSearchParams(window.location.search);
    if (selectedId) params.set("email", selectedId); else params.delete("email");
    if (selectedStage) params.set("stage", selectedStage); else params.delete("stage");
    if (view === "rules") params.set("view", "rules"); else params.delete("view");
    const query = params.toString();
    window.history.replaceState(null, "", `${window.location.pathname}${query ? `?${query}` : ""}`);
  }, [selectedId, selectedStage, view]);

  const handleFilterChange = useCallback((name, value) => {
    const normalizedValue = name === "receivedFrom"
      ? (value ? `${value}T00:00:00Z` : "")
      : name === "receivedTo"
        ? (value ? `${value}T23:59:59.999Z` : "")
        : value;
    setFilters((current) => ({ ...current, [name]: normalizedValue }));
  }, []);

  const handleSelectEmail = useCallback((emailId) => {
    setSelectedId(emailId);
    setSelectedStage("route_decision");
  }, []);

  const handleViewChange = useCallback((nextView) => {
    setView(nextView);
    if (nextView === "rules") setFocusMode(false);
  }, [setFocusMode]);

  return (
    <div className={`app-shell ${focusMode ? "app-focus-mode" : ""}`}>
      <Topbar lastUpdated={lastUpdated ? new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", second: "2-digit" }).format(new Date(lastUpdated)) : ""} onRefresh={refresh} refreshing={loading || traceLoading} />
      <div className="workspace">
        <Sidebar
          collapsed={sidebarCollapsed}
          onToggle={() => setSidebarCollapsed((value) => !value)}
          view={view}
          onViewChange={handleViewChange}
          emailCount={emails.total}
          ruleCount={rules.length}
        />
        <main className="main-content">
          {error && <div className="error-banner"><CircleAlert size={16} /><span>{error}</span><button type="button" onClick={() => setError("")}>关闭</button></div>}
          {view === "trace" ? (
            <TraceWorkspace
              emails={emails}
              filters={filters}
              onFilterChange={handleFilterChange}
              selectedId={selectedId}
              onSelectEmail={handleSelectEmail}
              trace={trace ? { ...trace, route_decision: routeDecision || trace.route_decision } : null}
              loading={loading}
              traceLoading={traceLoading}
              onRefresh={refresh}
              emailListCollapsed={emailListCollapsed}
              onToggleEmailList={() => setEmailListCollapsed((value) => !value)}
              focusMode={focusMode}
              onToggleFocus={() => setFocusMode((value) => !value)}
              selectedStage={selectedStage}
              onSelectStage={setSelectedStage}
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

export { App, TraceNode, TraceNodeDetails, labelForStatus };

if (typeof document !== "undefined") {
  const rootElement = document.getElementById("root");
  if (rootElement) createRoot(rootElement).render(<App />);
}
