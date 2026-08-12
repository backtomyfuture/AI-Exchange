import React from "react";
import { AlertTriangle, ChevronLeft, ChevronRight, Filter, Inbox, Search } from "lucide-react";
import { formatSender, formatTime, labelForRoute, labelForStatus, labelForTier } from "../lib/formatters";

const filterOptions = {
  status: [
    ["", "全部状态"],
    ["pending", "待处理"],
    ["processing", "处理中"],
    ["waiting", "等待中"],
    ["waiting_approval", "等待审批"],
    ["human_action", "人工处理中"],
    ["completed", "已完成"],
    ["sent", "已发送"],
    ["draft_saved", "草稿已保存"],
    ["manual_review", "人工审核"],
    ["no_action", "无需处理"],
    ["not_triggered", "未触发"],
    ["skipped", "已跳过"],
    ["send_failed", "发送失败"],
    ["failed", "失败"],
    ["dead_letter", "处理失败"],
    ["unknown", "数据异常"]
  ],
  route: [
    ["", "全部路由"],
    ["reply", "回复"],
    ["forward", "转发"],
    ["read_only", "仅阅读"],
    ["no_action", "无需处理"],
    ["manual_review", "人工审核"]
  ],
  tier: [["", "全部层级"], ["tier1", "Tier 1"], ["tier2", "Tier 2"], ["tier3", "Tier 3"]],
  requiresHuman: [["", "人工介入不限"], ["true", "需要人工介入"], ["false", "无需人工介入"]],
  hasAnomaly: [["", "异常不限"], ["true", "仅异常"], ["false", "无异常"]]
};

function SelectFilter({ name, value, onChange, options, label }) {
  return (
    <label className="filter-control">
      <span className="sr-only">{label}</span>
      <select aria-label={label} value={value} onChange={(event) => onChange(name, event.target.value)}>
        {options.map(([option, text]) => <option value={option} key={option}>{text}</option>)}
      </select>
    </label>
  );
}

function DateFilter({ name, value, onChange, label }) {
  return (
    <label className="filter-control">
      <span className="sr-only">{label}</span>
      <input
        aria-label={label}
        type="date"
        value={value ? value.slice(0, 10) : ""}
        onChange={(event) => onChange(name, event.target.value)}
      />
    </label>
  );
}

export function EmailList({ emails, filters, onFilterChange, selectedId, onSelect, loading, collapsed, onToggle }) {
  if (collapsed) {
    return (
      <section className="email-list panel email-list-collapsed">
        <button type="button" className="email-list-expand" onClick={onToggle} aria-label="展开邮件列表" title="展开邮件列表">
          <ChevronRight size={17} /><span>{emails.total || 0}</span>
        </button>
      </section>
    );
  }

  return (
    <section className="email-list panel">
      <div className="panel-header">
        <div><div className="panel-title">邮件列表</div><div className="panel-caption">{emails.total || 0} 条 Durable Inbox 记录</div></div>
        <div className="panel-header-actions"><Search size={16} className="muted-icon" /><button type="button" className="icon-button" onClick={onToggle} aria-label="收起邮件列表" title="收起邮件列表"><ChevronLeft size={15} /></button></div>
      </div>
      <div className="filter-stack">
        <label className="search-control">
          <Search size={14} />
          <input aria-label="关键词搜索" placeholder="搜索主题或发件人" value={filters.query} onChange={(event) => onFilterChange("query", event.target.value)} />
        </label>
        <label className="search-control">
          <Inbox size={14} />
          <input aria-label="发件人筛选" placeholder="发件人姓名或邮箱" value={filters.sender} onChange={(event) => onFilterChange("sender", event.target.value)} />
        </label>
        <div className="filter-grid">
          <SelectFilter name="status" value={filters.status} onChange={onFilterChange} options={filterOptions.status} label="状态筛选" />
          <SelectFilter name="route" value={filters.route} onChange={onFilterChange} options={filterOptions.route} label="路由筛选" />
          <SelectFilter name="tier" value={filters.tier} onChange={onFilterChange} options={filterOptions.tier} label="层级筛选" />
          <SelectFilter name="requiresHuman" value={filters.requiresHuman} onChange={onFilterChange} options={filterOptions.requiresHuman} label="人工介入筛选" />
          <SelectFilter name="hasAnomaly" value={filters.hasAnomaly} onChange={onFilterChange} options={filterOptions.hasAnomaly} label="异常筛选" />
          <DateFilter name="receivedFrom" value={filters.receivedFrom} onChange={onFilterChange} label="开始日期" />
          <DateFilter name="receivedTo" value={filters.receivedTo} onChange={onFilterChange} label="结束日期" />
        </div>
      </div>
      <div className="email-list-scroll">
        {loading && <div className="empty-state"><span className="loading-ring" />正在加载邮件投影…</div>}
        {!loading && !emails.items.length && <div className="empty-state"><Inbox size={24} /><span>没有找到邮件记录</span><small>请确认本地 Console 使用了只读 DSN。</small></div>}
        {emails.items.map((email) => {
          const sender = formatSender(email.sender);
          return (
            <button key={email.external_email_id} className={`email-row ${selectedId === email.external_email_id ? "selected" : ""}`} onClick={() => onSelect(email.external_email_id)}>
              <div className="email-row-top">
                <span className={`status-dot status-${email.status}`} />
                <span className="email-status">{labelForStatus(email.status)}</span>
                {email.has_anomaly && <span className="anomaly-mini" title="存在数据异常"><AlertTriangle size={11} /></span>}
                <span className="email-time">{formatTime(email.received_at)}</span>
              </div>
              <div className="email-subject">{email.subject || "无主题"}</div>
              <div className="email-sender"><strong>{sender.name}</strong>{sender.address && <span>{sender.address}</span>}</div>
              <div className="email-row-bottom">
                {email.route && <span className="mini-tag">{labelForRoute(email.route)}</span>}
                {email.tier && <span className="mini-tag tier">{labelForTier(email.tier)}</span>}
                {email.matched_rule_count !== null && email.matched_rule_count !== undefined && <span className="rule-count">命中规则 {email.matched_rule_count}</span>}
                {email.requires_human && <span className="human-mini">需人工</span>}
              </div>
            </button>
          );
        })}
      </div>
    </section>
  );
}
