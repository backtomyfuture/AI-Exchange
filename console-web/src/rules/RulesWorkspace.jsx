import React, { useState } from "react";
import { ArrowRight, Beaker, CheckCircle2, ChevronLeft, CircleAlert, Code2, FileCode2, LoaderCircle, Save, ShieldCheck, SlidersHorizontal, Sparkles } from "lucide-react";
import { request } from "../lib/api";
import { labelForRoute } from "../lib/labels.zh-CN";

const formLabels = {
  Identity: "基本信息",
  "Match anchor": "匹配锚点",
  Decision: "决策",
  "Governance fixtures": "治理样例",
  "Rule ID": "规则 ID",
  Version: "版本",
  Status: "状态",
  Criticality: "重要级别",
  Owner: "负责人",
  Purpose: "用途",
  "Anchor field": "锚点字段",
  "Anchor operator": "锚点操作符",
  "Value(s), comma-separated": "值（逗号分隔）",
  Field: "字段",
  Operator: "操作符",
  "Value(s), optional": "值（可选）",
  "Canonical route": "规范路由",
  "Reply mode": "回复模式",
  "Fixed recipients (comma-separated)": "固定收件人（逗号分隔）",
  "Reason code": "原因代码",
  "Positive cases JSON": "正例 JSON",
  "Negative cases JSON": "反例 JSON"
};
const optionLabels = {
  proposed: "待审核",
  enabled: "已启用",
  retired: "已停用",
  reply: "回复",
  forward: "转发",
  read_only: "仅阅读",
  no_action: "无需处理",
  manual_review: "人工审核",
  sender_only: "仅发件人",
  sender_and_original_cc: "发件人及原 CC",
  contains: "包含",
  contains_any: "包含任一",
  regex: "正则",
  eq: "等于",
  in: "属于",
  has_any: "包含任一",
  has_all: "包含全部"
};

function RulesWorkspace({ rules, activeRule, setActiveRule, onSaved, onError }) {
  const [showEditor, setShowEditor] = useState(false);
  return (
    <div className="rules-layout">
      <section className="content-header">
        <div><div className="eyebrow"><span className="eyebrow-line" /> 治理 / TIER 1 注册表</div><h1>规则草稿</h1><p>编写确定性路由意图，使用生产编译器校验后，再按既有流程人工发布。</p></div>
        <button className="primary-button" onClick={() => { setActiveRule(null); setShowEditor(true); }}><Sparkles size={15} /> 新建规则草稿</button>
      </section>
      <div className="rule-warning"><ShieldCheck size={16} /><div><strong>仅写入本地文件。</strong> 保存只写入 <code>tier1_rules/</code>，Console 不会提交、重启、热加载或部署生产服务。</div></div>
      {showEditor ? <RuleEditor rule={activeRule} onClose={() => setShowEditor(false)} onSaved={(message) => { setShowEditor(false); onSaved(message); }} onError={onError} /> : <RuleTable rules={rules} onEdit={(rule) => { setActiveRule(rule); setShowEditor(true); }} />}
    </div>
  );
}

function RuleTable({ rules, onEdit }) {
  return (
    <section className="panel rule-table-panel">
      <div className="panel-header"><div><div className="panel-title">规则注册表</div><div className="panel-caption">{rules.length} 个本地清单</div></div><Code2 size={16} className="muted-icon" /></div>
      {!rules.length ? <div className="empty-state rule-empty"><FileCode2 size={26} /><span>没有加载规则清单</span><small>新建草稿后即可开始编写。</small></div> : <div className="rule-table"><div className="rule-table-row rule-table-head"><span>规则 ID</span><span>路由</span><span>状态</span><span>负责人</span><span /></div>{rules.map((rule) => <button className="rule-table-row" key={rule.rule_id} onClick={() => onEdit(rule)}><span className="rule-id"><span className="rule-glyph" />{rule.rule_id}<small>v{rule.rule_version} · {rule.filename}</small></span><span className={`route-text route-text-${rule.route}`}>{labelForRoute(rule.route)}</span><span><span className={`status-pill status-pill-${rule.status}`}>{rule.status === "proposed" ? "待审核" : rule.status === "enabled" ? "已启用" : "已停用"}</span></span><span className="owner-text">{rule.owner || "—"}</span><ArrowRight size={15} /></button>)}</div>}
    </section>
  );
}

function RuleEditor({ rule, onClose, onSaved, onError }) {
  const [rawMode, setRawMode] = useState(false);
  const [rawYaml, setRawYaml] = useState(rule?.manifest ? toYaml(rule.manifest) : defaultRuleYaml());
  const [fields, setFields] = useState(() => rule?.manifest ? fieldsFromManifest(rule.manifest) : defaultFields());
  const [validation, setValidation] = useState(null);
  const [saving, setSaving] = useState(false);
  const [testEmailId, setTestEmailId] = useState("");
  const [matchTest, setMatchTest] = useState(null);
  const update = (key, value) => setFields((current) => ({ ...current, [key]: value }));
  const buildManifest = () => ({
    schema_version: 1,
    rule_id: fields.rule_id,
    rule_version: Number(fields.rule_version || 1),
    status: fields.status,
    owner: fields.owner || undefined,
    purpose: fields.purpose || undefined,
    validity: { effective_from: fields.effective_from || undefined, expires_at: fields.expires_at || undefined },
    match: {
      anchor: fields.anchor_field === "sender.address"
        ? { any: [{ field: fields.anchor_field, op: "eq", value: fields.anchor_value }] }
        : { any: [{ field: fields.anchor_field, op: fields.anchor_op, values: fields.anchor_value.split(",").map((value) => value.trim()).filter(Boolean) }] },
      ...(fields.condition_value ? {
        conditions: fields.condition_op === "contains_any"
          ? { field: fields.condition_field, op: fields.condition_op, values: fields.condition_value.split(",").map((value) => value.trim()).filter(Boolean) }
          : { field: fields.condition_field, op: fields.condition_op, value: fields.condition_value }
      } : {})
    },
    decision: { route: fields.route, params: routeParams(fields) },
    governance: {
      criticality: fields.criticality || undefined,
      risk_notes: fields.risk_notes || undefined,
      positive_cases: fields.positive_cases ? JSON.parse(fields.positive_cases) : [],
      negative_cases: fields.negative_cases ? JSON.parse(fields.negative_cases) : [],
      external_recipient_acknowledged: fields.external_ack,
      full_text_match_acknowledged: fields.full_text_ack
    }
  });
  const payload = () => rawMode ? { raw_yaml: rawYaml } : { rule_id: fields.rule_id, manifest: buildManifest() };
  const validate = async () => {
    try { setValidation(await request("/api/rules/validate", { method: "POST", body: JSON.stringify(payload()) })); } catch (cause) { onError(cause.message); }
  };
  const save = async () => {
    setSaving(true);
    try {
      const response = await request("/api/rules", { method: "POST", body: JSON.stringify(payload()) });
      onSaved(`规则草稿已保存：${response.rule?.rule_id || fields.rule_id}`);
    } catch (cause) { onError(cause.message); } finally { setSaving(false); }
  };
  const runMatch = async (saveAs = null) => {
    try {
      const response = await request(`/api/rules/${encodeURIComponent(fields.rule_id)}/test-match`, {
        method: "POST",
        body: JSON.stringify({ external_email_id: testEmailId, save_as: saveAs })
      });
      setMatchTest(response);
      if (response.saved_as) onSaved(`已将 ${response.case_id} 保存为${response.saved_as === "positive_cases" ? "正例" : "反例"}。`);
    } catch (cause) { onError(cause.message); }
  };
  return (
    <section className="editor-panel panel">
      <div className="editor-header"><button className="back-button" onClick={onClose}><ChevronLeft size={16} /> 返回注册表</button><div className="editor-actions"><button className="ghost-button compact" onClick={() => setRawMode(!rawMode)}>{rawMode ? <SlidersHorizontal size={14} /> : <Code2 size={14} />}{rawMode ? "结构化表单" : "原始 YAML"}</button><button className="primary-button compact" disabled={saving} onClick={save}>{saving ? <LoaderCircle className="spin" size={14} /> : <Save size={14} />} 保存草稿</button></div></div>
      <div className="editor-title"><div className="eyebrow"><span className="eyebrow-line" /> 规则草稿 / {fields.rule_id || "未命名"}</div><h2>{rule ? "编辑规则清单" : "编写路由规则"}</h2><p>启用前必须通过同一套 schema、fixture、正则、冲突和外部收件人校验。</p></div>
      {rawMode ? <textarea className="yaml-editor" value={rawYaml} onChange={(event) => setRawYaml(event.target.value)} spellCheck="false" /> : <div className="structured-form"><FormSection title="Identity"><div className="form-grid"><Field label="Rule ID" value={fields.rule_id} onChange={(value) => update("rule_id", value)} placeholder="sender-finance-001" /><Field label="Version" value={fields.rule_version} onChange={(value) => update("rule_version", value)} /><Field label="Status" type="select" options={["proposed", "enabled", "retired"]} value={fields.status} onChange={(value) => update("status", value)} /><Field label="Criticality" type="select" options={["", "P0", "P1", "P2", "P3"]} value={fields.criticality} onChange={(value) => update("criticality", value)} /><Field label="Owner" value={fields.owner} onChange={(value) => update("owner", value)} /><Field label="Purpose" value={fields.purpose} onChange={(value) => update("purpose", value)} wide /></div></FormSection><FormSection title="Match anchor"><div className="form-grid"><Field label="Anchor field" type="select" options={["sender.address", "to.addresses", "cc.addresses"]} value={fields.anchor_field} onChange={(value) => update("anchor_field", value)} /><Field label="Anchor operator" type="select" options={fields.anchor_field === "sender.address" ? ["eq", "in"] : ["has_any", "has_all"]} value={fields.anchor_op} onChange={(value) => update("anchor_op", value)} /><Field label="Value(s), comma-separated" value={fields.anchor_value} onChange={(value) => update("anchor_value", value)} placeholder="sender@example.com" /></div><div className="condition-row"><span className="condition-prefix">并且匹配内容</span><Field label="Field" type="select" options={["subject", "body.current_text", "body.full_text"]} value={fields.condition_field} onChange={(value) => update("condition_field", value)} /><Field label="Operator" type="select" options={["contains", "contains_any", "regex"]} value={fields.condition_op} onChange={(value) => update("condition_op", value)} /><Field label="Value(s), optional" value={fields.condition_value} onChange={(value) => update("condition_value", value)} /></div></FormSection><FormSection title="Decision"><div className="form-grid"><Field label="Canonical route" type="select" options={["reply", "forward", "read_only", "no_action", "manual_review"]} value={fields.route} onChange={(value) => update("route", value)} /><Field label="Reply mode" type="select" options={["sender_only", "sender_and_original_cc"]} value={fields.reply_mode} onChange={(value) => update("reply_mode", value)} /><Field label="Fixed recipients (comma-separated)" value={fields.fixed_recipients} onChange={(value) => update("fixed_recipients", value)} wide /><Field label="Reason code" value={fields.reason_code} onChange={(value) => update("reason_code", value)} /></div></FormSection><FormSection title="Governance fixtures"><div className="form-grid"><Field label="Positive cases JSON" value={fields.positive_cases} onChange={(value) => update("positive_cases", value)} wide placeholder='[{"case_id":"p1","email":{...}}]' /><Field label="Negative cases JSON" value={fields.negative_cases} onChange={(value) => update("negative_cases", value)} wide placeholder='[{"case_id":"n1","email":{...}}]' /><label className="check-field"><input type="checkbox" checked={fields.external_ack} onChange={(event) => update("external_ack", event.target.checked)} /> 已确认外部收件人</label><label className="check-field"><input type="checkbox" checked={fields.full_text_ack} onChange={(event) => update("full_text_ack", event.target.checked)} /> 已确认完整文本匹配</label></div></FormSection></div>}
      <div className="validation-row"><button className="validate-button" onClick={validate}><Beaker size={15} /> 使用编译器校验</button>{validation && <ValidationResult result={validation} />}</div>
      <div className="sandbox-row"><div><div className="form-section-title"><Beaker size={12} /> 规则匹配测试</div><p>使用历史邮件投影运行真实匹配器，不返回正文或附件。</p></div><div className="sandbox-actions"><input value={testEmailId} onChange={(event) => setTestEmailId(event.target.value)} placeholder="邮件 ID" /><button className="ghost-button compact" disabled={!testEmailId || !fields.rule_id} onClick={() => runMatch()}>运行匹配</button>{matchTest && <span className={`match-result match-${matchTest.result.toLowerCase()}`}>{matchTest.result === "MATCHED" ? "已命中" : matchTest.result === "NOT_MATCHED" ? "未命中" : "无法判断"}</span>}{matchTest?.result === "MATCHED" && <button className="mini-action" onClick={() => runMatch("positive_cases")}>保存为正例</button>}{matchTest?.result === "NOT_MATCHED" && <button className="mini-action" onClick={() => runMatch("negative_cases")}>保存为反例</button>}</div></div>
      <div className="editor-footnote"><ShieldCheck size={14} /> 仅写入本地文件。保存后仍需人工提交并通过既有发布流程和计划重启使其生效。</div>
    </section>
  );
}

function FormSection({ title, children }) { return <div className="form-section"><div className="form-section-title">{formLabels[title] || title}</div>{children}</div>; }
function Field({ label, value, onChange, type = "text", options = [], wide = false, placeholder = "" }) {
  return <label className={`field ${wide ? "field-wide" : ""}`}><span>{formLabels[label] || label}</span>{type === "select" ? <select aria-label={formLabels[label] || label} value={value} onChange={(event) => onChange(event.target.value)}>{options.map((option) => <option key={option} value={option}>{optionLabels[option] || option || "—"}</option>)}</select> : <input aria-label={formLabels[label] || label} value={value} onChange={(event) => onChange(event.target.value)} placeholder={placeholder} />}</label>;
}
function ValidationResult({ result }) { return <div className={`validation-result ${result.valid ? "valid" : "invalid"}`}><span>{result.valid ? <CheckCircle2 size={15} /> : <CircleAlert size={15} />} {result.valid ? `校验通过 · ${result.enabled_rule_count} 条已启用规则` : `${result.errors.length} 个编译问题`}</span>{!result.valid && <details><summary>查看问题</summary>{result.errors.map((issue) => <div key={`${issue.code}-${issue.rule_id}`}><strong>{issue.code}</strong> {issue.message}</div>)}</details>}</div>; }

function defaultFields() { return { rule_id: "", rule_version: "1", status: "proposed", owner: "", purpose: "", criticality: "P2", anchor_field: "sender.address", anchor_op: "eq", anchor_value: "", condition_field: "subject", condition_op: "contains", condition_value: "", route: "reply", reply_mode: "sender_only", fixed_recipients: "", reason_code: "console_rule", positive_cases: "[]", negative_cases: "[]", external_ack: false, full_text_ack: false, effective_from: "", expires_at: "", risk_notes: "" }; }
function fieldsFromManifest(manifest) { const fields = defaultFields(); const anchor = manifest.match?.anchor?.any?.[0] || manifest.match?.anchor?.all?.[0] || {}; const condition = manifest.match?.conditions || {}; const params = manifest.decision?.params || {}; return { ...fields, rule_id: manifest.rule_id || "", rule_version: String(manifest.rule_version || 1), status: manifest.status || "proposed", owner: manifest.owner || "", purpose: manifest.purpose || "", criticality: manifest.governance?.criticality || "", anchor_field: anchor.field || fields.anchor_field, anchor_op: anchor.op || fields.anchor_op, anchor_value: anchor.value || (anchor.values || []).join(", "), condition_field: condition.field || fields.condition_field, condition_op: condition.op || fields.condition_op, condition_value: condition.value || (condition.values || []).join(", "), route: manifest.decision?.route || "reply", reply_mode: params.reply_mode || fields.reply_mode, fixed_recipients: (params.fixed_recipients || []).join(", "), reason_code: params.reason_code || "", positive_cases: JSON.stringify(manifest.governance?.positive_cases || []), negative_cases: JSON.stringify(manifest.governance?.negative_cases || []), external_ack: Boolean(manifest.governance?.external_recipient_acknowledged), full_text_ack: Boolean(manifest.governance?.full_text_match_acknowledged), effective_from: manifest.validity?.effective_from || "", expires_at: manifest.validity?.expires_at || "" }; }
function routeParams(fields) { if (fields.route === "forward") return { fixed_recipients: fields.fixed_recipients.split(",").map((value) => value.trim()).filter(Boolean), cc: [], allow_recipient_edit: true, include_attachments: false }; if (fields.route === "reply") return { reply_mode: fields.reply_mode }; if (fields.route === "no_action" || fields.route === "manual_review") return { reason_code: fields.reason_code || "console_rule" }; return {}; }
function defaultRuleYaml() { return `schema_version: 1\nrule_id: example-rule-001\nrule_version: 1\nstatus: proposed\nowner: operator\npurpose: Describe why this rule exists.\nmatch:\n  anchor:\n    any:\n      - field: sender.address\n        op: eq\n        value: sender@example.com\ndecision:\n  route: reply\n  params:\n    reply_mode: sender_only\ngovernance:\n  positive_cases: []\n  negative_cases: []\n`; }
function toYaml(value) { return Object.entries(value).map(([key, item]) => `${key}: ${typeof item === "object" ? JSON.stringify(item) : item}`).join("\n"); }
function formatTime(value) { if (!value) return "—"; return new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }).format(new Date(value)); }
function formatDetail(value) { if (typeof value === "object") return JSON.stringify(value); return String(value); }


export { RulesWorkspace };
