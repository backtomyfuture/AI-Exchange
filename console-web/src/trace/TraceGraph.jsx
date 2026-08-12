import React, { useMemo } from "react";
import { Background, Controls, MarkerType, MiniMap, ReactFlow } from "@xyflow/react";
import { TraceNode } from "./TraceNode";
import { labelForStage } from "../lib/labels.zh-CN";

const nodeTypes = { trace: TraceNode };
const FIXED_STAGE_IDS = ["ingestion", "intake_guard", "route_decision", "handoff", "draft", "approval", "send"];
const FIXED_EDGES = FIXED_STAGE_IDS.slice(0, -1).map((source, index) => ({
  id: `${source}-${FIXED_STAGE_IDS[index + 1]}`,
  source,
  target: FIXED_STAGE_IDS[index + 1]
}));

function colorForStatus(status) {
  return {
    completed: "#51d6a3",
    active: "#78a9ff",
    waiting: "#71809a",
    human_action: "#c28bff",
    not_triggered: "#56627a",
    skipped: "#56627a",
    failed: "#f26d78",
    unknown: "#f5b86d"
  }[status] || "#647aa8";
}

export function TraceGraph({ trace, selectedStage, onSelectStage }) {
  const nodes = useMemo(() => {
    const source = new Map((trace?.nodes || []).map((node) => [node.id, node]));
    return FIXED_STAGE_IDS.map((id, index) => {
      const node = source.get(id) || {
        id,
        kind: id,
        label: labelForStage(id),
        status: "unknown",
        summary: "历史数据不足",
        data_quality: "missing",
        business_detail: {},
        input_output: {},
        technical_detail: {}
      };
      return {
        id,
        type: "trace",
        position: { x: index * 182, y: 78 },
        data: {
          ...node,
          label: labelForStage(node.kind || id),
          index,
          selected: selectedStage === id,
          onSelect: onSelectStage,
          onPreviewClose: () => {}
        }
      };
    });
  }, [onSelectStage, selectedStage, trace]);

  const edges = useMemo(() => FIXED_EDGES.map((edge) => ({
    ...edge,
    type: "smoothstep",
    animated: trace?.nodes?.find((node) => node.id === edge.target)?.status === "active",
    markerEnd: { type: MarkerType.ArrowClosed, color: "#5b6b8e" },
    style: { stroke: "#33415e", strokeWidth: 1.5 }
  })), [trace]);

  return (
    <div className="graph-frame" aria-label="七阶段处理路径">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.12 }}
        nodesDraggable={false}
        nodesConnectable={false}
        zoomOnDoubleClick={false}
        panOnDrag
        aria-label="七阶段处理路径"
      >
        <Background color="#182238" gap={24} size={1} />
        <MiniMap pannable zoomable nodeColor={(node) => colorForStatus(node.data.status)} maskColor="rgba(8, 11, 18, 0.72)" />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}

export { FIXED_STAGE_IDS };
