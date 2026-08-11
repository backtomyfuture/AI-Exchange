import React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

vi.mock("@xyflow/react", async (importOriginal) => {
  const actual = await importOriginal();
  const ReactModule = await import("react");
  return {
    ...actual,
    Handle: ({ children, ...props }) => ReactModule.createElement("span", props, children)
  };
});

import { TraceNode } from "./main";

const node = {
  id: "route_decision",
  kind: "route_decision",
  label: "Route Decision",
  status: "completed",
  index: 2,
  detail: {
    route: "reply",
    tier: "tier1",
    decision_digest: "bounded-digest"
  }
};

describe("TraceNode details", () => {
  it("opens and closes safe stage details on hover", async () => {
    render(<TraceNode data={node} />);
    const trigger = screen.getByTestId("trace-node-route_decision");

    fireEvent.pointerEnter(trigger);
    await screen.findByRole("dialog", { name: "Route Decision details" });

    fireEvent.pointerLeave(trigger);
    await waitFor(() => {
      expect(screen.queryByRole("dialog", { name: "Route Decision details" })).toBeNull();
    });
  });

  it("opens safe stage details from keyboard focus", async () => {
    render(<TraceNode data={node} />);
    const trigger = screen.getByTestId("trace-node-route_decision");

    fireEvent.focus(trigger);

    const dialog = await screen.findByRole("dialog", { name: "Route Decision details" });
    expect(dialog.textContent).toContain("reply");
    expect(dialog.textContent).toContain("tier1");
    expect(dialog.textContent).toContain("bounded-digest");
    expect(trigger.getAttribute("aria-expanded")).toBe("true");
  });

  it("pins details on click and closes them with Escape", async () => {
    render(<TraceNode data={node} />);
    const trigger = screen.getByTestId("trace-node-route_decision");

    fireEvent.click(trigger);
    await screen.findByRole("dialog", { name: "Route Decision details" });

    fireEvent.keyDown(document, { key: "Escape", code: "Escape" });

    await waitFor(() => {
      expect(screen.queryByRole("dialog", { name: "Route Decision details" })).toBeNull();
    });
    expect(trigger.getAttribute("aria-expanded")).toBe("false");
  });

  it("supports touch pinning without relying on hover", async () => {
    render(<TraceNode data={node} />);
    const trigger = screen.getByTestId("trace-node-route_decision");

    const touchDown = new Event("pointerdown", { bubbles: true });
    Object.defineProperty(touchDown, "pointerType", { value: "touch" });
    trigger.dispatchEvent(touchDown);
    await screen.findByRole("dialog", { name: "Route Decision details" });

    fireEvent.click(trigger);
    expect(trigger.getAttribute("aria-expanded")).toBe("true");

    const secondTouchDown = new Event("pointerdown", { bubbles: true });
    Object.defineProperty(secondTouchDown, "pointerType", { value: "touch" });
    trigger.dispatchEvent(secondTouchDown);
    await waitFor(() => {
      expect(screen.queryByRole("dialog", { name: "Route Decision details" })).toBeNull();
    });
  });

  it("closes a pinned panel from its close control", async () => {
    render(<TraceNode data={node} />);
    const trigger = screen.getByTestId("trace-node-route_decision");

    fireEvent.click(trigger);
    const closeButton = await screen.findByRole("button", { name: "Close Route Decision details" });
    fireEvent.click(closeButton);

    await waitFor(() => {
      expect(screen.queryByRole("dialog", { name: "Route Decision details" })).toBeNull();
    });
  });
});
