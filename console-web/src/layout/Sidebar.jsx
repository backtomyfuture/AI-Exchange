import React from "react";
import { Activity, ChevronLeft, ChevronRight, Database, SlidersHorizontal } from "lucide-react";

export function Sidebar({ collapsed, onToggle, view, onViewChange, emailCount, ruleCount }) {
  return (
    <aside className={`sidebar ${collapsed ? "sidebar-collapsed" : ""}`}>
      <div className="sidebar-topline">
        {!collapsed && <div className="sidebar-label">工作区</div>}
        <button
          type="button"
          className="sidebar-toggle"
          onClick={onToggle}
          aria-label={collapsed ? "展开工作区导航" : "收起工作区导航"}
          title={collapsed ? "展开工作区导航" : "收起工作区导航"}
        >
          {collapsed ? <ChevronRight size={15} /> : <ChevronLeft size={15} />}
        </button>
      </div>
      <button
        type="button"
        className={`nav-item ${view === "trace" ? "active" : ""}`}
        onClick={() => onViewChange("trace")}
        aria-label="处理追踪"
        title={collapsed ? "处理追踪" : undefined}
      >
        <Activity size={17} /><span>处理追踪</span><span className="nav-count">{emailCount || "—"}</span>
      </button>
      <button
        type="button"
        className={`nav-item ${view === "rules" ? "active" : ""}`}
        onClick={() => onViewChange("rules")}
        aria-label="规则草稿"
        title={collapsed ? "规则草稿" : undefined}
      >
        <SlidersHorizontal size={17} /><span>规则草稿</span><span className="nav-count">{ruleCount || "—"}</span>
      </button>
      <div className="sidebar-spacer" />
      {!collapsed && (
        <div className="system-card">
          <div className="system-card-title"><Database size={14} /> 数据访问</div>
          <div className="system-status"><span className="status-orb" /> PostgreSQL 投影</div>
          <div className="system-muted">只读角色 · 业务事实表</div>
        </div>
      )}
    </aside>
  );
}
