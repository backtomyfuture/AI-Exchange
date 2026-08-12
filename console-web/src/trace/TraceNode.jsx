import React, { useId, useRef, useState } from "react";
import { CheckCircle2, CircleAlert, FileCode2, GitBranch, Inbox, Send, ShieldCheck, Workflow, X } from "lucide-react";
import { AnimatePresence, motion } from "framer-motion";
import { HoverCard, HoverCardContent, HoverCardPortal, HoverCardTrigger } from "../components/ui/hover-card";
import { Handle, Position } from "@xyflow/react";
import { formatDuration, safeJson } from "../lib/formatters";
import { labelForStage, labelForStatus } from "../lib/labels.zh-CN";

const stageIcons = {
  ingestion: Inbox,
  intake_guard: ShieldCheck,
  route_decision: GitBranch,
  handoff: Workflow,
  draft: FileCode2,
  approval: CheckCircle2,
  send: Send
};

function PreviewSummary({ node }) {
  return (
    <div className="node-preview-summary">
      <div className="node-preview-status">
        <span className={`status-icon status-icon-${node.status}`} aria-hidden="true">{node.status === "failed" ? "!" : "·"}</span>
        <strong>{labelForStatus(node.status)}</strong>
        {node.duration_ms !== undefined && <span>{formatDuration(node.duration_ms)}</span>}
      </div>
      <p>{node.summary || "暂无处理摘要"}</p>
      {node.safe_error_code && <div className="preview-error"><CircleAlert size={13} /> {node.safe_error_code}</div>}
      <small>点击节点查看完整详情</small>
    </div>
  );
}

export function TraceNode({ data }) {
  const Icon = stageIcons[data.kind] || Workflow;
  const detailsId = `trace-node-preview-${useId().replaceAll(":", "")}`;
  const [hoverOpen, setHoverOpen] = useState(false);
  const lastTouchToggleAt = useRef(0);
  const interactive = typeof data.onSelect === "function";

  const select = (event) => {
    event?.stopPropagation?.();
    data.onSelect?.(data.id);
  };

  const handlePointerDown = (event) => {
    if (event.pointerType === "touch") {
      event.preventDefault();
      lastTouchToggleAt.current = Date.now();
      select(event);
    }
  };

  const handleKeyDown = (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      select(event);
    }
    if (event.key === "Escape") {
      setHoverOpen(false);
      data.onPreviewClose?.();
    }
  };

  if (interactive) {
    return (
      <HoverCard open={hoverOpen} onOpenChange={setHoverOpen} openDelay={240} closeDelay={180}>
        <HoverCardTrigger asChild>
          <motion.div
            data-testid={`trace-node-${data.id}`}
            role="button"
            tabIndex={0}
            data-trace-node-trigger={data.id}
            className={`flow-node flow-${data.status} nodrag nopan ${data.selected ? "flow-selected" : ""}`}
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: data.index * 0.08 }}
            aria-label={`${data.label}，${labelForStatus(data.status)}`}
            aria-pressed={Boolean(data.selected)}
            onPointerDown={handlePointerDown}
            onClick={(event) => {
              if (event.type === "click" && Date.now() - lastTouchToggleAt.current < 500) return;
              select(event);
            }}
            onKeyDown={handleKeyDown}
          >
            <Handle type="target" position={Position.Left} className="flow-handle" />
            <div className="flow-node-top"><div className="flow-icon"><Icon size={15} /></div><span>{String(data.index + 1).padStart(2, "0")}</span></div>
            <div className="flow-node-label">{data.label}</div>
            <div className="flow-node-status"><span className="tiny-status" />{labelForStatus(data.status)}</div>
            <Handle type="source" position={Position.Right} className="flow-handle" />
          </motion.div>
        </HoverCardTrigger>
        <HoverCardPortal>
          <HoverCardContent
            id={detailsId}
            className="hover-card-content node-preview-card"
            side="bottom"
            align="start"
            sideOffset={10}
            collisionPadding={12}
          >
            <div className="hover-card-kicker">阶段 {String(data.index + 1).padStart(2, "0")}</div>
            <strong>{data.label}</strong>
            <PreviewSummary node={data} />
          </HoverCardContent>
        </HoverCardPortal>
      </HoverCard>
    );
  }

  // Compatibility path for the original standalone TraceNode contract. The
  // dashboard passes onSelect, so production UI remains summary-on-hover and
  // detail-below; this path keeps old local consumers from breaking.
  return <LegacyTraceNode data={data} />;
}

function LegacyTraceNode({ data }) {
  const detailsId = `trace-node-details-${useId().replaceAll(":", "")}`;
  const [hoverOpen, setHoverOpen] = useState(false);
  const [pinned, setPinned] = useState(false);
  const lastTouchToggleAt = useRef(0);
  const open = hoverOpen || pinned;
  const closeDetails = () => { setPinned(false); setHoverOpen(false); };
  const togglePinned = (event) => {
    event.stopPropagation();
    if (event.type === "click" && Date.now() - lastTouchToggleAt.current < 500) return;
    setPinned((current) => {
      const next = !current;
      setHoverOpen(next);
      return next;
    });
  };
  const handlePointerDown = (event) => {
    if (event.pointerType === "touch") {
      event.preventDefault();
      lastTouchToggleAt.current = Date.now();
      togglePinned(event);
    }
  };
  const handleKeyDown = (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      togglePinned(event);
    }
  };
  const Icon = stageIcons[data.kind] || Workflow;
  return (
    <HoverCard open={open} onOpenChange={(nextOpen) => { if (!pinned) setHoverOpen(nextOpen); }} openDelay={180} closeDelay={120}>
      <HoverCardTrigger asChild>
        <motion.div
          data-testid={`trace-node-${data.id}`}
          role="button"
          tabIndex={0}
          data-trace-node-trigger={data.id}
          className={`flow-node flow-${data.status} nodrag nopan`}
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          aria-controls={detailsId}
          aria-expanded={open}
          onPointerDown={handlePointerDown}
          onClick={togglePinned}
          onKeyDown={handleKeyDown}
        >
          <Handle type="target" position={Position.Left} className="flow-handle" />
          <div className="flow-node-top"><div className="flow-icon"><Icon size={15} /></div><span>{String(data.index + 1).padStart(2, "0")}</span></div>
          <div className="flow-node-label">{data.label}</div>
          <div className="flow-node-status"><span className="tiny-status" />{labelForStatus(data.status)}</div>
          <Handle type="source" position={Position.Right} className="flow-handle" />
        </motion.div>
      </HoverCardTrigger>
      <HoverCardPortal>
        <HoverCardContent
          id={detailsId}
          className="hover-card-content"
          side="bottom"
          align="start"
          sideOffset={10}
          collisionPadding={12}
          onPointerDownOutside={closeDetails}
          onEscapeKeyDown={closeDetails}
        >
          <TraceNodeDetails node={data} onClose={closeDetails} />
        </HoverCardContent>
      </HoverCardPortal>
    </HoverCard>
  );
}

export function TraceNodeDetails({ node, onClose }) {
  const detail = node.detail || {};
  const useful = Object.entries(detail).filter(([key, value]) => !isProtectedKey(key) && value !== null && value !== undefined && value !== "" && !(Array.isArray(value) && !value.length));
  return (
    <div className="trace-node-details" role="dialog" aria-label={`${node.label} details`}>
      <div className="hover-card-header">
        <div><div className="hover-card-kicker">阶段 {String((node.index || 0) + 1).padStart(2, "0")}</div><strong>{node.label}</strong></div>
        <button type="button" className="hover-card-close" aria-label={`Close ${node.label} details`} onClick={onClose}><X size={14} /></button>
      </div>
      <div className={`hover-card-status hover-card-status-${node.status}`}><span className="tiny-status" /> {labelForStatus(node.status)}</div>
      {node.safe_error_code && <div className="hover-card-error"><CircleAlert size={13} /> {node.safe_error_code}</div>}
      <div className="hover-card-details">
        {useful.length ? useful.map(([key, value]) => <div className="hover-card-detail" key={key}><span>{key.replaceAll("_", " ")}</span><strong>{safeJson(value)}</strong></div>) : <span className="hover-card-empty">暂无安全详情</span>}
      </div>
      <div className="hover-card-hint">点击节点可保持详情打开</div>
    </div>
  );
}

function isProtectedKey(key) {
  return ["attachment", "body", "content", "draft", "html", "prompt", "snippet", "text"].some((part) => key.toLowerCase().includes(part));
}

export { stageIcons };
