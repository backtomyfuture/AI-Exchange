import { labelForRoute, labelForStatus, labelForTier } from "./labels.zh-CN";

export function formatTime(value, options = {}) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    ...options
  }).format(date);
}

export function formatDateTime(value) {
  return formatTime(value, { year: "numeric", second: "2-digit" });
}

export function formatDuration(durationMs) {
  if (typeof durationMs !== "number" || !Number.isFinite(durationMs)) return "—";
  if (durationMs < 1000) return `${Math.round(durationMs)} ms`;
  return `${(durationMs / 1000).toFixed(1)} s`;
}

export function formatSender(sender) {
  if (!sender) return { name: "未知发件人", address: "" };
  if (typeof sender === "object") {
    return {
      name: sender.name || sender.address || "未知发件人",
      address: sender.address || ""
    };
  }
  const text = String(sender).trim();
  const mailbox = text.match(/name=['"]([^'"]*)['"].*?(?:email_address|address)=['"]([^'"]*)['"]/i);
  if (mailbox) return { name: mailbox[1] || mailbox[2], address: mailbox[2] };
  const angle = text.match(/^(.*?)\s*<([^<>]+@[^<>]+)>$/);
  if (angle) return { name: angle[1].trim() || angle[2], address: angle[2] };
  return { name: text.includes("@") ? text.split("@")[0] : text, address: text.includes("@") ? text : "" };
}

export function safeJson(value) {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "string") return value.slice(0, 512);
  try {
    return JSON.stringify(safeDisplayProjection(value), null, 2);
  } catch {
    return "数据不可展示";
  }
}

const PROTECTED_KEY_PARTS = ["attachment", "body", "content", "draft", "html", "prompt", "snippet", "text"];

function safeDisplayProjection(value, depth = 0) {
  if (depth > 4) return "[已限制深度]";
  if (Array.isArray(value)) return value.slice(0, 32).map((item) => safeDisplayProjection(item, depth + 1));
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([key]) => !PROTECTED_KEY_PARTS.some((part) => key.toLowerCase().includes(part)))
        .map(([key, item]) => [key, safeDisplayProjection(item, depth + 1)])
    );
  }
  if (typeof value === "string") return value.slice(0, 512);
  return value;
}

export { labelForRoute, labelForStatus, labelForTier };
