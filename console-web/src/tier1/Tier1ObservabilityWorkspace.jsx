import React, { useMemo } from "react";
import { AlertTriangle, BarChart3, GitBranch, Layers3, ShieldCheck } from "lucide-react";
import { PageHeading } from "../layout/Topbar";
import { labelForRoute } from "../lib/formatters";

const WINDOWS = [
  { value: "24h", label: "最近 24 小时" },
  { value: "7d", label: "最近 7 天" },
  { value: "30d", label: "最近 30 天" },
  { value: "90d", label: "最近 90 天" }
];

function percentage(value) {
  return `${(Math.max(0, Number(value) || 0) * 100).toFixed(1)}%`;
}

function shortDigest(value) {
  if (!value) return "未记录";
  return `${value.slice(0, 12)}…`;
}

function MetricCard({ label, value, detail, tone = "normal" }) {
  return (
    <div className={`tier1-metric tier1-metric-${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
      {detail && <small>{detail}</small>}
    </div>
  );
}

export function Tier1ObservabilityWorkspace({
  observation,
  rules,
  window,
  onWindowChange,
  loading
}) {
  const summary = observation?.summary;
  const noObservationRules = useMemo(() => {
    if (!observation) return [];
    const observed = new Set(
      observation.rules.map((rule) => `${rule.rule_id}@${rule.rule_version || "unknown"}`)
    );
    return (rules || []).filter(
      (rule) => rule.status === "enabled"
        && !observed.has(`${rule.rule_id}@${rule.rule_version}`)
    );
  }, [observation, rules]);
  const hasSignals = (summary?.conflict_count || 0) + (summary?.error_count || 0) > 0;

  return (
    <div className="tier1-observability-layout">
      <PageHeading
        eyebrow="治理 / TIER 1 可观察性"
        title="Tier 1 观察"
        description="基于持久化路由评估记录，查看规则覆盖、冲突与异常。"
        action={
          <label className="tier1-window-control">
            <span>观察窗口</span>
            <select
              aria-label="Tier 1 观察窗口"
              value={window}
              onChange={(event) => onWindowChange(event.target.value)}
            >
              {WINDOWS.map((option) => (
                <option key={option.value} value={option.value}>{option.label}</option>
              ))}
            </select>
          </label>
        }
      />

      <section className="tier1-governance-note">
        <GitBranch size={17} />
        <div>
          <strong>规则版本继续由 Git 管理。</strong>
          <span>此页面只读取运行时 artifact digest、规则版本和安全路由事实，不执行提交、部署或热加载。</span>
        </div>
      </section>

      {loading ? (
        <div className="tier1-loading"><span className="loading-ring" />正在汇总 Tier 1 观察数据…</div>
      ) : !observation ? (
        <div className="tier1-empty"><BarChart3 size={28} /><strong>暂无 Tier 1 观察数据</strong><span>刷新后将显示当前窗口内的持久化路由评估记录。</span></div>
      ) : (
        <>
          <section className="tier1-metrics">
            <MetricCard label="评估邮件" value={summary.evaluated_count} detail={`窗口内 · ${summary.artifact_count} 个制品`} />
            <MetricCard label="Tier 1 命中率" value={percentage(summary.match_rate)} detail={`${summary.matched_count} 封在 Tier 1 定案`} />
            <MetricCard label="继续 Tier 2" value={summary.abstained_count} detail="Tier 1 未命中" />
            <MetricCard label="规则冲突" value={summary.conflict_count} detail="已失败关闭至人工复核" tone={summary.conflict_count ? "warning" : "normal"} />
            <MetricCard label="规则异常" value={summary.error_count} detail="事实不足或评估异常" tone={summary.error_count ? "danger" : "normal"} />
          </section>

          <section className={`tier1-signal ${hasSignals ? "tier1-signal-alert" : ""}`}>
            {hasSignals ? <AlertTriangle size={16} /> : <ShieldCheck size={16} />}
            <span>{hasSignals
              ? "当前窗口存在 Tier 1 冲突或异常，请在处理追踪中核对相应邮件。"
              : "当前窗口未观察到 Tier 1 冲突或评估异常。"
            }</span>
          </section>

          <section className="panel tier1-rule-panel">
            <div className="panel-header">
              <div>
                <div className="panel-title">已观察规则版本</div>
                <div className="panel-caption">命中占评估 = 单条规则命中次数 ÷ 同一规则集制品的 Tier 1 评估邮件数</div>
              </div>
              <Layers3 size={16} className="muted-icon" />
            </div>
            {!observation.rules.length ? (
              <div className="tier1-empty tier1-table-empty"><Layers3 size={24} /><strong>窗口内没有规则命中或冲突记录</strong></div>
            ) : (
              <div className="tier1-rule-table">
                <div className="tier1-rule-row tier1-rule-head">
                  <span>规则版本</span><span>路由</span><span>命中</span><span>命中占评估</span><span>冲突参与</span><span>规则集制品</span>
                </div>
                {observation.rules.map((rule) => (
                  <div className="tier1-rule-row" key={`${rule.artifact_digest}-${rule.rule_id}-${rule.rule_version}`}>
                    <span className="tier1-rule-name">{rule.rule_id}<small>v{rule.rule_version || "?"}</small></span>
                    <span className={`route-text route-text-${rule.route || "unknown"}`}>{labelForRoute(rule.route)}</span>
                    <span>{rule.match_count}</span>
                    <span>{percentage(rule.match_share_of_evaluations)}</span>
                    <span className={rule.conflict_involvement_count ? "tier1-conflict-count" : ""}>{rule.conflict_involvement_count}</span>
                    <code title={rule.artifact_digest || ""}>{shortDigest(rule.artifact_digest)}</code>
                  </div>
                ))}
              </div>
            )}
          </section>

          {noObservationRules.length > 0 && (
            <section className="tier1-unobserved">
              <Layers3 size={15} />
              <span>当前工作树中已启用、但本窗口未产生观察记录的规则：</span>
              <code>{noObservationRules.map((rule) => `${rule.rule_id} v${rule.rule_version}`).join(" · ")}</code>
            </section>
          )}
        </>
      )}
    </div>
  );
}
