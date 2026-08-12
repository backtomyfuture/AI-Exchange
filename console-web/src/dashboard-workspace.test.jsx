import React from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { EmailList } from "./trace/EmailList";
import { StageDetail } from "./trace/StageDetail";
import { labelForStage, labelForStatus } from "./lib/labels.zh-CN";

const emails = {
  total: 1,
  items: [{
    external_email_id: "mail-1",
    subject: "季度报告",
    sender: { name: "财务机器人", address: "finance@example.com" },
    received_at: "2026-08-12T08:00:00Z",
    status: "waiting_approval",
    route: "reply",
    tier: "tier2",
    matched_rule_count: 2,
    requires_human: true,
    has_anomaly: true
  }]
};

const filters = {
  query: "",
  sender: "",
  status: "",
  route: "",
  tier: "",
  requiresHuman: "",
  hasAnomaly: ""
};

describe("dashboard workspace", () => {
  it("uses Chinese labels while keeping tier labels explicit", () => {
    expect(labelForStage("route_decision")).toBe("路由决策");
    expect(labelForStatus("waiting_approval")).toBe("等待审批");

    const onFilterChange = vi.fn();
    render(
      <EmailList
        emails={emails}
        filters={filters}
        onFilterChange={onFilterChange}
        selectedId="mail-1"
        onSelect={vi.fn()}
        loading={false}
        collapsed={false}
        onToggle={vi.fn()}
      />
    );

    expect(screen.getAllByText("等待审批").length).toBeGreaterThan(0);
    expect(screen.getByText("财务机器人")).toBeTruthy();
    expect(screen.getAllByText("Tier 2").length).toBeGreaterThan(0);
    expect(screen.getByText("命中规则 2")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("异常筛选"), { target: { value: "true" } });
    expect(onFilterChange).toHaveBeenCalledWith("hasAnomaly", "true");
  });

  it("renders route evidence as a structured stepper without content fields", () => {
    render(
      <StageDetail
        node={{
          id: "route_decision",
          kind: "route_decision",
          label: "Route Decision",
          status: "completed",
          summary: "已形成最终路由",
          data_quality: "ok",
          started_at: "2026-08-12T08:00:00Z",
          finished_at: "2026-08-12T08:00:01Z",
          duration_ms: 1000,
          business_detail: {},
          input_output: { body: "不应展示" },
          technical_detail: { decision_digest: "digest-1" }
        }}
        routeDecision={{
          final_route: "reply",
          final_tier: "tier1",
          confidence: 1,
          decision_data_quality: "ok",
          steps: [
            { tier: "tier1", status: "completed", summary: "命中规则", matched_rules: [{ rule_id: "rule-1" }] },
            { tier: "tier2", status: "not_triggered", summary: "Tier 2 未触发" },
            { tier: "tier3", status: "not_triggered", summary: "Tier 3 未触发" }
          ]
        }}
      />
    );

    expect(screen.getByText("最终路由")).toBeTruthy();
    expect(screen.getAllByText("命中规则").length).toBeGreaterThan(0);
    expect(screen.queryByText("不应展示")).toBeNull();
  });
});
