import React from "react";
import { Activity, Orbit, RefreshCw, ShieldCheck } from "lucide-react";

export function Topbar({ lastUpdated, onRefresh, refreshing }) {
  return (
    <header className="topbar">
      <div className="brand">
        <div className="brand-mark"><Orbit size={19} /></div>
        <div>
          <div className="brand-name">AI 邮件助手</div>
          <div className="brand-subtitle">运行监控</div>
        </div>
      </div>
      <div className="environment-chip"><span className="pulse-dot" /> 本地环境 · 只读</div>
      <div className="topbar-meta">
        <span><ShieldCheck size={12} /> 数据库只读投影</span>
        <span className="meta-divider" />
        <span>最后更新：{lastUpdated || "—"}</span>
        {onRefresh && (
          <button type="button" className="topbar-refresh" onClick={onRefresh} disabled={refreshing}>
            <RefreshCw className={refreshing ? "spin" : ""} size={13} /> 立即刷新
          </button>
        )}
      </div>
    </header>
  );
}

export function PageHeading({ eyebrow, title, description, action }) {
  return (
    <section className="content-header">
      <div>
        <div className="eyebrow"><span className="eyebrow-line" /> {eyebrow}</div>
        <h1>{title}</h1>
        {description && <p>{description}</p>}
      </div>
      {action}
    </section>
  );
}

export const traceIcon = Activity;
