import React, { useMemo, useState } from "react";
import { Activity, AlertTriangle, ChevronDown, ChevronUp, Clock3, Focus, Sparkles } from "lucide-react";
import { EmailList } from "./EmailList";
import { TraceGraph } from "./TraceGraph";
import { StageDetail } from "./StageDetail";
import { formatDateTime, formatSender, labelForRoute, labelForStatus, labelForTier } from "../lib/formatters";
import { labelForStage } from "../lib/labels.zh-CN";
import { PageHeading } from "../layout/Topbar";

const auditLabels = {
  send: "发送",
  forward: "转发",
  archive: "归档",
  mark_read: "标记已读",
  approval: "审批",
  draft: "草稿",
  delivery: "通知"
};

export function TraceWorkspace({
  emails,
  filters,
  onFilterChange,
  selectedId,
  onSelectEmail,
  trace,
  loading,
  traceLoading,
  onRefresh,
  emailListCollapsed,
  onToggleEmailList,
  focusMode,
  onToggleFocus,
  selectedStage,
  onSelectStage
}) {
  return (
    <div className={`trace-layout ${emailListCollapsed ? "trace-email-collapsed" : ""} ${focusMode ? "trace-focus-mode" : ""}`}>
      <PageHeading
        eyebrow="处理追踪 / 单封邮件"
        title="邮件处理监控"
        description="从 Durable Inbox 到执行结果，查看一封邮件的持久化业务路径。"
        action={
          <button type="button" className={`ghost-button focus-button ${focusMode ? "active" : ""}`} onClick={onToggleFocus}>
            <Focus size={15} />{focusMode ? "退出展开分析" : "展开分析"}
          </button>
        }
      />
      <div className="trace-grid">
        {!focusMode && (
          <EmailList
            emails={emails}
            filters={filters}
            onFilterChange={onFilterChange}
            selectedId={selectedId}
            onSelect={onSelectEmail}
            loading={loading}
            collapsed={emailListCollapsed}
            onToggle={onToggleEmailList}
          />
        )}
        <section className="trace-panel panel">
          {traceLoading && <div className="trace-loading"><span className="loading-ring" />正在加载处理详情…</div>}
          {!traceLoading && trace ? (
            <TraceDetail
              trace={trace}
              selectedStage={selectedStage}
              onSelectStage={onSelectStage}
              onRefresh={onRefresh}
            />
          ) : !traceLoading ? (
            <div className="trace-empty"><Sparkles size={28} /><h2>请选择一封邮件</h2><p>从左侧列表选择一条 Durable Inbox 记录，查看七阶段处理路径。</p></div>
          ) : null}
        </section>
      </div>
    </div>
  );
}

function TraceDetail({ trace, selectedStage, onSelectStage, onRefresh }) {
  const [auditOpen, setAuditOpen] = useState(false);
  const route = trace.route_decision || trace.nodes.find((node) => node.id === "route_decision")?.business_detail || {};
  const sender = formatSender(trace.sender);
  const selectedNode = trace.nodes.find((node) => node.id === selectedStage) || trace.nodes[0];
  const completedCount = trace.nodes.filter((node) => node.status === "completed").length;
  const anomalyCount = trace.nodes.filter((node) => node.data_quality && node.data_quality !== "ok").length;
  const auditEvents = useMemo(() => buildAuditEvents(trace), [trace]);
  return (
    <div className="trace-detail">
      <div className="trace-detail-header">
        <div className="trace-title-wrap">
          <div className="trace-kicker">邮件处理记录 / {labelForStatus(trace.current_status)}</div>
          <h2>{trace.subject || "无主题"}</h2>
          <div className="trace-address"><strong>{sender.name}</strong>{sender.address && <span>{sender.address}</span>}<i>·</i><code>{trace.inbox_id || trace.external_email_id}</code></div>
        </div>
        <div className={`route-badge route-${route.final_route || route.route || "unknown"}`}>{labelForRoute(route.final_route || route.route)}</div>
      </div>
      <div className="trace-summary">
        <SummaryMetric label="当前状态" value={labelForStatus(trace.current_status)} />
        <SummaryMetric label="最终层级" value={labelForTier(route.final_tier || route.tier)} />
        <SummaryMetric label="阶段进度" value={`${completedCount} / ${trace.nodes.length}`} />
        <SummaryMetric label="记录质量" value={anomalyCount ? `异常 ${anomalyCount} 项` : "正常"} anomaly={anomalyCount > 0} />
      </div>
      <div className="trace-context-row">
        <div><Clock3 size={13} />最近更新：{formatDateTime(trace.updated_at)}</div>
        <div><Activity size={13} />仅展示业务持久事实，不读取 LangGraph Checkpoint</div>
        {anomalyCount > 0 && <div className="trace-anomaly"><AlertTriangle size={13} />存在数据异常</div>}
      </div>
      <div className="graph-label"><span>七阶段处理路径</span><span className="graph-hint"><Sparkles size={13} />悬停查看摘要，点击查看详情</span></div>
      <TraceGraph trace={trace} selectedStage={selectedStage} onSelectStage={onSelectStage} />
      <div className="trace-inspector">
        <div className="inspector-header"><span>当前阶段详情</span><span className="inspector-line" /><span className="inspector-stage">{selectedNode ? labelForStage(selectedNode.kind) : "—"}</span></div>
        <StageDetail key={selectedNode?.id || "empty"} node={selectedNode} routeDecision={trace.route_decision} />
      </div>
      <section className="audit-timeline">
        <button type="button" className="audit-toggle" onClick={() => setAuditOpen((value) => !value)} aria-expanded={auditOpen}>
          <span><Activity size={14} />完整处理记录 <small>{auditEvents.length} 条安全记录</small></span>
          {auditOpen ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
        </button>
        {auditOpen && <AuditTimeline events={auditEvents} />}
      </section>
    </div>
  );
}

function SummaryMetric({ label, value, anomaly = false }) {
  return <div className={`summary-metric ${anomaly ? "summary-anomaly" : ""}`}><span>{label}</span><strong>{value || "暂无数据"}</strong></div>;
}

function buildAuditEvents(trace) {
  const events = [];
  trace.nodes.forEach((node) => {
    const list = node.input_output?.audit_events || node.business_detail?.audit_events || [];
    if (!Array.isArray(list)) return;
    list.forEach((event) => {
      if (!event || typeof event !== "object") return;
      events.push({
        at: event.created_at,
        title: auditLabels[event.action] || event.action || labelForStage(node.kind),
        result: event.result || "未知",
        actor: event.actor || "系统",
        reason: event.reason || ""
      });
    });
  });
  return events.sort((left, right) => String(left.at || "").localeCompare(String(right.at || "")));
}

function AuditTimeline({ events }) {
  if (!events.length) return <div className="audit-empty">暂无完整安全记录。缺失不代表系统推测执行成功。</div>;
  return (
    <div className="timeline-list">
      {events.map((event, index) => (
        <div className="timeline-item" key={`${event.at}-${index}`}>
          <span className="timeline-dot" />
          <div><strong>{event.title}</strong><span>{event.result} · {event.actor}</span>{event.reason && <small>{event.reason}</small>}</div>
          <time>{formatDateTime(event.at)}</time>
        </div>
      ))}
    </div>
  );
}

export { TraceDetail, AuditTimeline };
