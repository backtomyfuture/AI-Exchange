import React, { useState } from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { RulesWorkspace } from "./RulesWorkspace";

const { getRule } = vi.hoisted(() => ({
  getRule: vi.fn()
}));

vi.mock("../lib/api", () => ({
  getRule,
  request: vi.fn()
}));

function ControlledRulesWorkspace(props) {
  const [activeRule, setActiveRule] = useState(null);
  return <RulesWorkspace {...props} activeRule={activeRule} setActiveRule={setActiveRule} />;
}

describe("RulesWorkspace", () => {
  it("loads the existing manifest before opening a rule for editing", async () => {
    getRule.mockResolvedValue({
      rule_id: "sender-finance-001",
      rule_version: 3,
      status: "enabled",
      route: "forward",
      owner: "finance-ops",
      purpose: "Forward finance invoices",
      filename: "sender-finance-001.yaml",
      manifest: {
        schema_version: 1,
        rule_id: "sender-finance-001",
        rule_version: 3,
        status: "enabled",
        owner: "finance-ops",
        purpose: "Forward finance invoices",
        match: {
          anchor: {
            any: [{
              field: "sender.address",
              op: "eq",
              value: "finance@example.com"
            }]
          },
          conditions: {
            field: "subject",
            op: "contains",
            value: "invoice"
          }
        },
        decision: {
          route: "forward",
          params: {
            fixed_recipients: ["billing@example.com"],
            cc: [],
            allow_recipient_edit: true,
            include_attachments: false
          }
        },
        governance: {
          criticality: "P1",
          positive_cases: [],
          negative_cases: [],
          external_recipient_acknowledged: true,
          full_text_match_acknowledged: false
        }
      }
    });

    render(
      <ControlledRulesWorkspace
        rules={[{
          rule_id: "sender-finance-001",
          rule_version: 3,
          status: "enabled",
          route: "forward",
          owner: "finance-ops",
          purpose: "Forward finance invoices",
          filename: "sender-finance-001.yaml"
        }]}
        onSaved={vi.fn()}
        onError={vi.fn()}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: /sender-finance-001/ }));

    await waitFor(() => expect(getRule).toHaveBeenCalledWith("sender-finance-001"));
    expect(await screen.findByDisplayValue("finance@example.com")).toBeTruthy();
    expect(screen.getByDisplayValue("invoice")).toBeTruthy();
    expect(screen.getByDisplayValue("billing@example.com")).toBeTruthy();
    expect(screen.getByDisplayValue("finance-ops")).toBeTruthy();
    expect(screen.getByLabelText("版本").value).toBe("3");
  });
});
