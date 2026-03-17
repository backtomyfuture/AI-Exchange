#!/usr/bin/env python3
"""
Skill 自动发现工具 —— 分析历史邮件，发现处理模式，自动生成 Skill。

快速开始 (使用 uv，无需手动建虚拟环境):
    uv run scripts/discover_skills.py --source eml --pst-path ./emails/ --no-llm

完整说明见 docs/history-import-and-skill-discovery.md
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("skill_discovery")

from src.skills_discovery.analyzer import (
    DiscoveredPattern,
    EmailHistoryCollector,
    EmailRecord,
    PatternAnalyzer,
)
from src.skills_discovery.generator import write_skill


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def _confidence_stars(confidence: float) -> str:
    filled = round(confidence * 5)
    return "★" * filled + "☆" * (5 - filled)


def display_pattern(idx: int, pattern: DiscoveredPattern):
    """Print a single pattern as an ASCII chain diagram."""
    reply_icon = "✅" if pattern.suggested_need_reply else "❌"
    priority_color = {
        "P0": "\033[91m",  # red
        "P1": "\033[93m",  # yellow
        "P2": "\033[94m",  # blue
        "P3": "\033[90m",  # gray
    }.get(pattern.suggested_priority, "")
    reset = "\033[0m"

    print(f"\n{'━' * 58}")
    print(f"  📧 Pattern #{idx}: {pattern.name}")
    print(f"{'━' * 58}")
    print()
    print("  触发链路:")

    for cond in pattern.conditions:
        cond_type = cond.get("type", "unknown")
        operator = cond.get("operator", "")
        value = cond.get("value", "")
        if isinstance(value, list):
            value = ", ".join(str(v) for v in value)

        type_labels = {
            "sender_match": "发件人",
            "subject_match": "主题含",
            "to_match": "收件人",
            "cc_match": "抄送含",
            "body_match": "正文含",
            "recipient_role": "收件角色",
            "thread_depth": "线程深度",
        }
        label = type_labels.get(cond_type, cond_type)
        op_map = {
            "in": "属于", "contains": "包含",
            "regex": "正则", "eq": "等于",
        }
        op_label = op_map.get(operator, operator)

        display_val = (
            value if len(str(value)) < 40
            else str(value)[:37] + "..."
        )
        print("    ┌─────────────────────────────────────┐")
        print(f"    │ [{label}] {op_label}: {display_val:<23}│")
        print("    └──────────────┬──────────────────────┘")
        print("                   ↓")

    print("    ┌─────────────────────────────────────┐")
    print(f"    │ {reply_icon} 回复率: {pattern.reply_rate:.0%} ({pattern.sample_count} 封)      │")
    print(f"    │ {priority_color}🔴 优先级: {pattern.suggested_priority}{reset}                       │")
    need_reply_text = "是" if pattern.suggested_need_reply else "否"
    print(f"    │ 📝 需要回复: {need_reply_text}                      │")
    if pattern.suggested_tone:
        tone_display = pattern.suggested_tone[:20]
        print(f"    │ 💼 语气: {tone_display:<27}│")
    print("    └─────────────────────────────────────┘")

    print()
    print(f"  置信度: {_confidence_stars(pattern.confidence)} ({pattern.confidence:.2f})")
    print(f"  {pattern.description}")

    if pattern.example_subjects:
        print("  示例:")
        for subj in pattern.example_subjects[:3]:
            print(f"    • \"{subj[:50]}\"")


def display_all_patterns(patterns: list[DiscoveredPattern]):
    """Display all discovered patterns."""
    print(f"\n{'═' * 58}")
    print(f"  🔍 发现了 {len(patterns)} 个邮件路由模式")
    print(f"{'═' * 58}")

    for i, pattern in enumerate(patterns, 1):
        display_pattern(i, pattern)


def interactive_select(patterns: list[DiscoveredPattern]) -> list[DiscoveredPattern]:
    """Let user select patterns interactively."""
    if not patterns:
        return []

    print(f"\n{'═' * 58}")
    print("  请选择要生成 Skill 的模式")
    print(f"{'═' * 58}")
    print()
    print("  输入模式编号 (逗号分隔), 例如: 1,3,5")
    print("  输入 'all' 选择全部")
    print("  输入 'q' 退出")
    print()

    for i, p in enumerate(patterns, 1):
        reply_icon = "✅" if p.suggested_need_reply else "❌"
        print(f"  [{i}] {p.name:<25} "
              f"{reply_icon} 回复率={p.reply_rate:.0%} "
              f"({p.sample_count}封) "
              f"置信度={_confidence_stars(p.confidence)}")

    print()
    try:
        user_input = input("  > 请输入选择: ").strip()
    except (EOFError, KeyboardInterrupt):
        print("\n  已取消")
        return []

    if user_input.lower() == "q":
        return []
    if user_input.lower() == "all":
        return patterns

    selected = []
    try:
        indices = [int(x.strip()) for x in user_input.split(",") if x.strip()]
        for idx in indices:
            if 1 <= idx <= len(patterns):
                selected.append(patterns[idx - 1])
            else:
                print(f"  ⚠️ 忽略无效编号: {idx}")
    except ValueError:
        print("  ⚠️ 输入格式有误，请输入数字")
        return interactive_select(patterns)

    return selected


# ---------------------------------------------------------------------------
# Data collection from PST (direct analysis)
# ---------------------------------------------------------------------------

def _strip_body(body: str) -> str:
    """截取 body 到 1000 字符并移除图片标签。"""
    from src.skills_discovery.analyzer import strip_images_from_body
    if not body:
        return ""
    return strip_images_from_body(body)[:1000]


def _parsed_to_record(parsed) -> EmailRecord:
    return EmailRecord(
        id=parsed.id,
        subject=parsed.subject,
        sender=parsed.sender,
        to=parsed.to,
        cc=parsed.cc,
        received_at=parsed.received_at,
        message_type=parsed.message_type,
        source_folder=parsed.source_folder,
        body_preview=_strip_body(parsed.body),
        in_reply_to=parsed.in_reply_to,
        thread_id=parsed.conversation_id,
    )


def collect_from_pst(pst_path: Path, limit: int = 5000) -> list[EmailRecord]:
    """Parse a PST file directly and return EmailRecords for analysis."""
    from scripts.import_pst import iter_from_pst

    records = []
    for parsed in iter_from_pst(pst_path):
        if len(records) >= limit:
            break
        records.append(_parsed_to_record(parsed))
    return records


def collect_from_eml_dir(
    dir_path: Path, limit: int = 5000,
) -> list[EmailRecord]:
    """Parse EML files from a directory tree."""
    from scripts.import_pst import iter_from_eml_dir

    records = []
    for parsed in iter_from_eml_dir(dir_path):
        if len(records) >= limit:
            break
        records.append(_parsed_to_record(parsed))
    return records


def collect_from_qdrant(limit: int = 5000) -> list[EmailRecord]:
    """Collect email records from Qdrant."""
    from qdrant_client import QdrantClient
    from src.config import get_settings

    settings = get_settings()
    client = QdrantClient(url=settings.QDRANT_URL)
    collector = EmailHistoryCollector(client)
    return collector.collect(limit=limit)


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------

async def run_discovery(
    source: str = "qdrant",
    pst_path: str | None = None,
    use_llm: bool = True,
    limit: int = 5000,
    auto_confirm: bool = False,
    my_email: str | None = None,
) -> list[str]:
    """Run the full discovery workflow. Returns list of generated skill paths."""

    print("\n📊 Skill 自动发现工具")
    print(f"   数据源: {source}")
    print(f"   LLM 分析: {'是' if use_llm else '否 (启发式)'}")
    print()

    # --- 1. Collect data ---
    print("⏳ 正在收集邮件数据...")
    if source == "pst" and pst_path:
        records = collect_from_pst(Path(pst_path), limit=limit)
    elif source == "eml" and pst_path:
        records = collect_from_eml_dir(Path(pst_path), limit=limit)
    else:
        records = collect_from_qdrant(limit=limit)

    if not records:
        print("❌ 未找到任何邮件数据")
        print("   提示: 先使用 import_pst.py 导入历史邮件，或确认 Qdrant 中已有数据")
        return []

    received = [r for r in records if r.message_type != "sent"]
    sent = [r for r in records if r.message_type == "sent"]
    print(f"   收集完成: {len(received)} 封收件, {len(sent)} 封已发送")

    # --- 2. Analyze patterns ---
    print("\n⏳ 正在分析邮件模式...")
    analyzer = PatternAnalyzer(records, my_email=my_email or "")

    stats = analyzer.compute_statistics()
    print(f"   发件人: {stats['unique_senders']} 个")
    print(f"   高频词: {', '.join(w for w, _ in stats['top_subject_words'][:10])}")

    if use_llm:
        print("\n⏳ 正在使用 LLM 深度分析...")
        patterns = await analyzer.discover_with_llm()
    else:
        patterns = analyzer._discover_heuristic()

    if not patterns:
        print("❌ 未发现有意义的模式")
        return []

    # --- 3. Display patterns ---
    display_all_patterns(patterns)

    # --- 4. Interactive selection ---
    if auto_confirm:
        selected = patterns
        print(f"\n  自动确认: 选择全部 {len(selected)} 个模式")
    else:
        selected = interactive_select(patterns)

    if not selected:
        print("\n  未选择任何模式，退出。")
        return []

    # --- 5. Generate skills ---
    print(f"\n⏳ 正在生成 {len(selected)} 个 Skill...")
    generated_paths = []
    for pattern in selected:
        path = write_skill(pattern)
        generated_paths.append(path)
        print(f"  ✅ {pattern.name} → {path}")

    print(f"\n{'═' * 58}")
    print(f"  🎉 成功生成 {len(generated_paths)} 个 Skill!")
    print("     位置: skills_registry/")
    print("     重启服务后自动加载。")
    print(f"{'═' * 58}")

    return generated_paths


def main():
    parser = argparse.ArgumentParser(
        description="Skill 自动发现 —— 分析历史邮件，生成处理规则",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source",
        choices=["qdrant", "pst", "eml"],
        default="qdrant",
        help="数据来源: qdrant, pst, eml (默认: qdrant)",
    )
    parser.add_argument(
        "--pst-path",
        help="PST 文件或 EML 目录路径 (当 --source pst/eml 时必需)",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="不使用 LLM，仅用启发式算法分析",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="最大分析邮件数 (默认: 5000)",
    )
    parser.add_argument(
        "--auto-confirm",
        action="store_true",
        help="自动确认所有发现的模式 (跳过交互选择)",
    )
    parser.add_argument(
        "--my-email",
        help="你的邮箱地址 (用于识别 TO/CC 角色，默认从已发送邮件推断)",
    )

    args = parser.parse_args()

    if args.source in ("pst", "eml") and not args.pst_path:
        parser.error("使用 --source pst/eml 时需要提供 --pst-path")

    asyncio.run(run_discovery(
        source=args.source,
        pst_path=args.pst_path,
        use_llm=not args.no_llm,
        limit=args.limit,
        auto_confirm=args.auto_confirm,
        my_email=args.my_email,
    ))


if __name__ == "__main__":
    main()
