import React, { useEffect, useMemo, useState } from "react";
import { AlertTriangle, ChevronDown, ChevronRight, Code2, Database, FileCheck2, ShieldAlert } from "lucide-react";
import { formatDateTime, formatDuration, safeJson, labelForRoute, labelForStatus, labelForTier } from "../lib/formatters";
import { labelForStage, tabLabels } from "../lib/labels.zh-CN";

const fieldLabels = {
  status: "状态",
  route: "路由",
  tier: "层级",
  reason_code: "原因代码",
  disposition: "处置结果",
  state: "处理状态",
  revision_count: "修订次数",
  approval_count: "审批次数",
  plan_available: "处理计划",
  evidence_available: "证据",
  final_route: "最终路由",
  final_tier: "最终层级",
  confidence: "置信度",
  source: "来源",
  received_at: "接收时间",
  content_protected: "正文保护"
};

function fieldLabel(key) {
  return fieldLabels[key] || key.replaceAll("_", " ");
}

function Value({ value }) {
  if (value === null || value === undefined || value === "") return <span className="empty-value">暂无数据</span>;
  if (typeof value === "boolean") return <span>{value ? "是" : "否"}</span>;
  if (typeof value === "string" && (value.endsWith("_at") || value.includes("T"))) return <span>{value}</span>;
  return <span className={typeof value === "object" ? "value-json" : ""}>{safeJson(value)}</span>;
}

function KeyValueGrid({ value }) {
  const entries = Object.entries(value || {}).filter(([key, item]) => !isProtectedKey(key) && item !== null && item !== undefined && item !== "");
  if (!entries.length) return <div className="detail-empty">暂无安全结构化数据，系统不会根据缺失字段推测。</div>;
  return (
    <div className="detail-kv-grid">
      {entries.map(([key, item]) => (
        <div className="detail-kv" key={key}>
          <span>{fieldLabel(key)}</span>
          <strong><Value value={item} /></strong>
        </div>
      ))}
    </div>
  );
}

function isProtectedKey(key) {
  return ["attachment", "body", "content", "draft", "html", "prompt", "snippet", "text"].some((part) => key.toLowerCase().includes(part));
}

function RouteDecisionDetail({ detail }) {
  const steps = detail?.steps || [];
  return (
    <div className="route-decision-detail">
      <div className="route-final-summary">
        <div><span>最终路由</span><strong>{labelForRoute(detail?.final_route)}</strong></div>
        <div><span>最终层级</span><strong>{labelForTier(detail?.final_tier)}</strong></div>
        <div><span>置信度</span><strong>{typeof detail?.confidence === "number" ? `${Math.round(detail.confidence * 100)}%` : "暂无数据"}</strong></div>
      </div>
      {detail?.decision_data_quality !== "ok" && (
        <div className="anomaly-callout"><AlertTriangle size={15} /><div><strong>路由过程记录不完整</strong><span>页面只展示持久事实，不会根据当前规则文件补写历史过程。</span></div></div>
      )}
      <div className="route-stepper">
        {steps.map((step, index) => (
          <RouteStep key={step.tier} step={step} last={index === steps.length - 1} />
        ))}
      </div>
      {detail?.decision_digest && <div className="digest-line"><Code2 size={13} /> decision digest：{detail.decision_digest}</div>}
    </div>
  );
}

function RouteStep({ step, last }) {
  const [expanded, setExpanded] = useState(step.status !== "not_triggered" && step.status !== "unknown");
  return (
    <div className={`route-step route-step-${step.status}`}>
      <div className="route-step-marker" aria-hidden="true">{step.status === "completed" ? "✓" : step.status === "failed" ? "!" : "·"}</div>
      {!last && <div className="route-step-line" aria-hidden="true" />}
      <div className="route-step-body">
        <button type="button" className="route-step-heading" onClick={() => setExpanded((value) => !value)} aria-expanded={expanded}>
          <span><b>{step.tier === "tier1" ? "Tier 1" : step.tier === "tier2" ? "Tier 2" : "Tier 3"}</b><em>{labelForStatus(step.status)}</em></span>
          {expanded ? <ChevronDown size={15} /> : <ChevronRight size={15} />}
        </button>
        <p>{step.summary || "暂无处理说明"}</p>
        {step.continue_reason && <div className="continue-reason">继续原因：{step.continue_reason}</div>}
        {expanded && (
          <div className="route-step-detail">
            {step.matched_rules?.length > 0 && <DetailList title="命中规则" items={step.matched_rules} />}
            {step.candidates?.length > 0 && <DetailList title="候选结果与投票" items={step.candidates} />}
            {step.evidence?.length > 0 && <DetailList title="历史案例" items={step.evidence} />}
            {step.model_result && (
              <div className="model-result">
                <div className="subdetail-title">结构化模型结果</div>
                <KeyValueGrid value={step.model_result} />
              </div>
            )}
            {step.safe_error_code && <div className="safe-error"><ShieldAlert size={13} /> {step.safe_error_code}</div>}
            {!step.matched_rules?.length && !step.candidates?.length && !step.evidence?.length && !step.model_result && step.status !== "not_triggered" && (
              <div className="detail-empty">暂无该层的安全明细。</div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function DetailList({ title, items }) {
  return (
    <div className="route-evidence-list">
      <div className="subdetail-title">{title}</div>
      {items.map((item, index) => (
        <div className="route-evidence-item" key={`${title}-${index}`}>
          <span className="evidence-index">{String(index + 1).padStart(2, "0")}</span>
          <Value value={item} />
        </div>
      ))}
    </div>
  );
}

export function StageDetail({ node, routeDecision }) {
  const [tab, setTab] = useState("business");
  useEffect(() => setTab("business"), [node?.id]);
  const technical = useMemo(() => node?.technical_detail || {}, [node]);
  if (!node) {
    return <div className="stage-detail-empty"><Database size={25} /><strong>请选择一个阶段</strong><span>点击上方节点，查看该阶段的处理说明。</span></div>;
  }
  const content = tab === "business"
    ? node.business_detail || {}
    : tab === "data"
      ? node.input_output || {}
      : technical;
  return (
    <section className="stage-detail">
      <div className="stage-detail-heading">
        <div>
          <div className="stage-detail-kicker">当前阶段 · {labelForStage(node.kind)}</div>
          <h3>{labelForStage(node.kind)}</h3>
          <p>{node.summary || "暂无处理说明"}</p>
        </div>
        <div className={`stage-status-badge badge-${node.status}`}><span className={`status-icon status-icon-${node.status}`} aria-hidden="true">{node.status === "failed" ? "!" : "·"}</span>{labelForStatus(node.status)}</div>
      </div>
      {node.data_quality !== "ok" && (
        <div className={`anomaly-callout anomaly-${node.data_quality}`}><AlertTriangle size={15} /><div><strong>{node.data_quality === "inconsistent" ? "记录不一致" : "数据缺失"}</strong><span>{node.data_quality === "inconsistent" ? "阶段结果与实际效果记录不一致。" : "该阶段没有完整持久事实，页面不会补猜。"}</span></div></div>
      )}
      <div className="stage-detail-tabs" role="tablist" aria-label="阶段详情">
        {Object.entries(tabLabels).map(([key, label]) => (
          <button type="button" role="tab" aria-selected={tab === key} className={tab === key ? "active" : ""} key={key} onClick={() => setTab(key)}>
            {key === "business" ? <FileCheck2 size={14} /> : key === "data" ? <Database size={14} /> : <Code2 size={14} />}{label}
          </button>
        ))}
      </div>
      {node.kind === "route_decision" && tab === "business" && routeDecision
        ? <RouteDecisionDetail detail={routeDecision} />
        : <div className="stage-detail-panel"><KeyValueGrid value={content} /></div>}
      <div className="stage-detail-meta">
        <span>开始：{formatDateTime(node.started_at || node.timestamp)}</span>
        <span>结束：{formatDateTime(node.finished_at)}</span>
        <span>耗时：{formatDuration(node.duration_ms)}</span>
      </div>
    </section>
  );
}

export { RouteDecisionDetail };
