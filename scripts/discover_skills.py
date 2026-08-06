#!/usr/bin/env python3
"""Discover historical-email skill candidates without enabling them.

The normal flow is deliberately two conversational phases:

1. discover from the earliest 80% of the selected history and write a local
   review artifact containing newest-20% replay evidence;
2. after an operator explicitly selects/edits candidates in a conversation,
   use this same tool's promotion mode with that selection artifact.

Discovery never writes ``skills_registry`` and has no auto-confirm option.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("skill_discovery")

from src.skills_discovery.analyzer import (  # noqa: E402
    EmailHistoryCollector,
    EmailRecord,
    PatternAnalyzer,
    strip_images_from_body,
)
from src.skills_discovery.generator import (  # noqa: E402
    SkillPromotionConflict,
    SkillPromotionValidationError,
    promote_selected_candidates,
)
from src.skills_discovery.review import (  # noqa: E402
    CandidateReviewError,
    CandidateSelectionError,
    apply_conversational_selections,
    create_candidate_review,
    load_review,
    render_review,
    split_records_chronologically,
    write_review,
)


def _strip_body(body: str) -> str:
    """Keep the same bounded body representation used for review replay."""
    return strip_images_from_body(body or "")[:1000]


def _parsed_to_record(parsed: Any) -> EmailRecord:
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
    """Parse a PST directly for discovery; it does not import data into RAG."""
    from scripts.import_pst import iter_from_pst

    records = []
    for parsed in iter_from_pst(pst_path):
        if len(records) >= limit:
            break
        records.append(_parsed_to_record(parsed))
    return records


def collect_from_eml_dir(dir_path: Path, limit: int = 5000) -> list[EmailRecord]:
    """Parse EML files directly for discovery; it does not import data into RAG."""
    from scripts.import_pst import iter_from_eml_dir

    records = []
    for parsed in iter_from_eml_dir(dir_path):
        if len(records) >= limit:
            break
        records.append(_parsed_to_record(parsed))
    return records


def collect_from_qdrant(limit: int = 5000) -> list[EmailRecord]:
    """Collect the shared historical-RAG corpus from Qdrant."""
    from qdrant_client import QdrantClient
    from src.config import get_settings

    settings = get_settings()
    client = QdrantClient(url=settings.QDRANT_URL)
    return EmailHistoryCollector(client).collect(limit=limit)


def _default_review_output() -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("artifacts/skill-discovery") / f"review-{timestamp}.json"


async def run_discovery(
    source: str = "qdrant",
    pst_path: str | None = None,
    use_llm: bool = True,
    limit: int = 5000,
    my_email: str | None = None,
    review_output: str | Path | None = None,
) -> Path | None:
    """Create candidates and a review artifact; never promote a rule."""
    print("\n📊 历史邮件 Skill 候选发现")
    print(f"   数据源: {source}")
    print(f"   LLM 分析: {'是（默认）' if use_llm else '否（启发式）'}")
    print("   结果仅为候选，不会写入 skills_registry。")

    print("\n⏳ 正在收集邮件数据...")
    if source == "pst" and pst_path:
        records = collect_from_pst(Path(pst_path), limit=limit)
        print("   提示：直接 PST 分析不导入 RAG；如需在线历史背景，请先运行 import_pst.py。")
    elif source == "eml" and pst_path:
        records = collect_from_eml_dir(Path(pst_path), limit=limit)
        print("   提示：直接 EML 分析不导入 RAG；如需在线历史背景，请先运行 import_pst.py。")
    else:
        records = collect_from_qdrant(limit=limit)

    if not records:
        print("❌ 未找到任何邮件数据")
        print("   提示：先手工使用 import_pst.py 导入历史邮件，或确认 Qdrant 中已有数据。")
        return None

    training_records, validation_records = split_records_chronologically(records)
    training_received = sum(record.message_type != "sent" for record in training_records)
    training_sent = len(training_records) - training_received
    held_out_received = sum(record.message_type != "sent" for record in validation_records)
    print(
        "   时间切分完成："
        f"最早 80% {len(training_records)} 封（收件 {training_received}，已发送 {training_sent}），"
        f"最新 20% {len(validation_records)} 封（收件 {held_out_received}）用于回放。"
    )

    print("\n⏳ 正在从最早 80% 分析候选...")
    analyzer = PatternAnalyzer(training_records, my_email=my_email or "")
    stats = analyzer.compute_statistics()
    print(f"   发件人: {stats['unique_senders']} 个")
    if stats["top_subject_words"]:
        print("   高频词: " + ", ".join(word for word, _ in stats["top_subject_words"][:10]))

    if use_llm:
        print("\n⏳ 正在使用已配置的 LLM 发现候选...")
        patterns = await analyzer.discover_with_llm()
    else:
        patterns = analyzer._discover_heuristic()
    if not patterns:
        print("❌ 未发现有意义的候选")
        return None

    review = create_candidate_review(
        patterns,
        training_records=training_records,
        validation_records=validation_records,
        source=source,
        my_email=analyzer.my_email,
    )
    print("\n" + render_review(review))
    output = write_review(review, review_output or _default_review_output())
    print(f"\n✅ 已写入候选审阅文件：{output}")
    print("   请在对话中明确选择候选并按需编辑全部字段；选择后才会提升，并在计划重启后生效。")
    return output


def _load_selection_file(path: str | Path) -> object:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CandidateSelectionError("无法读取选择文件") from exc


def run_promotion(
    review_path: str | Path,
    selections_path: str | Path,
    *,
    registry_path: str = "skills_registry",
) -> list[str]:
    """Apply explicit conversational selections and create declarative rules."""
    review = load_review(review_path)
    selected = apply_conversational_selections(review, _load_selection_file(selections_path))
    print("\n" + render_review(replace_candidates(review, selected)))
    paths = promote_selected_candidates(selected, registry_path=registry_path)
    print("\n✅ 已提升以下声明式 Skill：")
    for path in paths:
        print(f"   - {path}")
    print("   不会热加载；请在计划的服务重启后使其生效。")
    return paths


def replace_candidates(review, candidates):
    """Render the effective selected values without mutating the review artifact."""
    from dataclasses import replace

    return replace(review, candidates=list(candidates))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="历史邮件 Skill 发现与对话确认后的声明式提升",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source",
        choices=["qdrant", "pst", "eml"],
        default="qdrant",
        help="发现数据来源：qdrant（默认）、pst 或 eml",
    )
    parser.add_argument(
        "--pst-path",
        help="PST 文件或 EML 目录路径（--source pst/eml 时必填）",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="不调用已配置的 LLM，仅用启发式算法",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="最大分析邮件数（默认：5000）",
    )
    parser.add_argument(
        "--my-email",
        help="你的邮箱地址；默认从早期已发送邮件推断",
    )
    parser.add_argument(
        "--review-output",
        help="候选审阅 JSON 输出路径（默认：artifacts/skill-discovery/）",
    )
    parser.add_argument(
        "--promote-review",
        help="由对话工作流传入的候选审阅 JSON；与 --selections 一起提升",
    )
    parser.add_argument(
        "--selections",
        help="对话确认后的选择 JSON，格式为 {\"selections\":[{\"candidate_id\":...,\"overrides\":{...}}]}",
    )
    parser.add_argument(
        "--registry-path",
        default="skills_registry",
        help="仅提升模式使用的目标注册表路径（默认：skills_registry）",
    )
    args = parser.parse_args()

    if args.promote_review:
        if not args.selections:
            parser.error("--promote-review 需要同时提供 --selections")
        try:
            run_promotion(
                args.promote_review,
                args.selections,
                registry_path=args.registry_path,
            )
        except SkillPromotionConflict as exc:
            print("❌ 提升冲突：目标规则已存在，未覆盖也未合并：" + ", ".join(exc.skill_ids))
            return 2
        except (CandidateReviewError, CandidateSelectionError, SkillPromotionValidationError) as exc:
            print(f"❌ 无法提升：{exc}")
            return 2
        return 0

    if args.selections:
        parser.error("--selections 只能与 --promote-review 一起使用")
    if args.source in ("pst", "eml") and not args.pst_path:
        parser.error("使用 --source pst/eml 时需要提供 --pst-path")
    asyncio.run(
        run_discovery(
            source=args.source,
            pst_path=args.pst_path,
            use_llm=not args.no_llm,
            limit=args.limit,
            my_email=args.my_email,
            review_output=args.review_output,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
