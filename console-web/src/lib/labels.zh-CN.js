export const stageLabels = {
  ingestion: "邮件接入",
  intake_guard: "接入检查",
  route_decision: "路由决策",
  handoff: "处理计划与证据",
  draft: "草稿与修订",
  approval: "人工审批",
  send: "执行结果"
};

export const statusLabels = {
  pending: "等待中",
  active: "处理中",
  waiting: "等待中",
  human_action: "人工处理中",
  completed: "已完成",
  not_triggered: "未触发",
  skipped: "已跳过",
  failed: "失败",
  unknown: "数据异常",
  leased: "处理中",
  retry_wait: "等待重试",
  manual_review: "人工审核",
  waiting_approval: "等待审批",
  sent: "已发送",
  send_failed: "发送失败",
  dead_letter: "处理失败",
  processing: "处理中",
  no_action: "无需处理",
  draft_saved: "草稿已保存",
  approved: "已批准",
  rejected: "已拒绝",
  completed_with_anomaly: "已完成但数据异常"
};

export const routeLabels = {
  reply: "回复",
  forward: "转发",
  read_only: "仅阅读",
  no_action: "无需处理",
  manual_review: "人工审核",
  unknown: "未确定"
};

export const tierLabels = {
  tier1: "Tier 1",
  tier2: "Tier 2",
  tier3: "Tier 3",
  system: "系统"
};

export const tabLabels = {
  business: "处理说明",
  data: "输入与输出",
  technical: "技术记录"
};

export const labelForStage = (value) => stageLabels[value] || value || "未知阶段";
export const labelForStatus = (value) => statusLabels[value] || value || "未知状态";
export const labelForRoute = (value) => routeLabels[value] || value || "未确定";
export const labelForTier = (value) => tierLabels[value] || value || "未确定";
