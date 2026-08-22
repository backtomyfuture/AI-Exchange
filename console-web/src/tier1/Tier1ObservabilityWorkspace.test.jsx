import React from "react";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Tier1ObservabilityWorkspace } from "./Tier1ObservabilityWorkspace";

describe("Tier1ObservabilityWorkspace", () => {
  it("renders only safe aggregate and rule-version observations", () => {
    render(
      <Tier1ObservabilityWorkspace
        window="30d"
        onWindowChange={vi.fn()}
        loading={false}
        rules={[{
          rule_id: "rule-unobserved",
          rule_version: 1,
          status: "enabled"
        }]}
        observation={{
          window: "30d",
          window_started_at: "2026-08-22T00:00:00Z",
          summary: {
            evaluated_count: 10,
            matched_count: 6,
            abstained_count: 3,
            conflict_count: 1,
            error_count: 0,
            artifact_count: 1,
            match_rate: 0.6
          },
          rules: [{
            artifact_digest: "a".repeat(64),
            rule_id: "rule-1",
            rule_version: 2,
            route: "reply",
            match_count: 6,
            match_share_of_evaluations: 0.6,
            conflict_involvement_count: 1
          }]
        }}
      />
    );

    expect(screen.getByText("Tier 1 观察")).toBeTruthy();
    expect(screen.getAllByText("60.0%")).toHaveLength(2);
    expect(screen.getByText("rule-1")).toBeTruthy();
    expect(screen.getByText("rule-unobserved v1")).toBeTruthy();
    expect(screen.queryByText("邮件正文")).toBeNull();
  });
});
