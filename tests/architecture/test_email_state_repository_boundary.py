from __future__ import annotations

import ast
import hashlib
import inspect
import re
from pathlib import Path

_SQL_EXECUTION_METHODS = frozenset(
    {"copy", "copy_expert", "execute", "executemany", "exec_driver_sql"}
)
_DYNAMIC_CODE_NAME_CALLS = frozenset({"compile", "eval", "exec"})
_REFLECTION_NAME_PRIMITIVES = frozenset(
    {"compile", "eval", "exec", "getattr", "globals", "locals", "vars"}
)
_REFLECTION_ATTRIBUTE_PRIMITIVE_PATHS = frozenset(
    {
        "dict.__getitem__",
        "object.__getattribute__",
        "operator.attrgetter",
        "operator.methodcaller",
    }
)
_SQL_TEXT_CONSTRUCTORS = frozenset({"SQL", "text"})
_SQL_IDENTIFIER_CONSTRUCTORS = frozenset({"Identifier"})
_TRUSTED_TABLE_CONSTRUCTORS = frozenset({"_table"})
_DYNAMIC_IDENTIFIER = "dynamic_identifier"
_TRUSTED_TABLE_PREFIX = "__task5_trusted_schema__"
_TRUSTED_TABLE_MARKER_ATTRIBUTE = "_task5_trusted_table_marker"
_NON_SQL_EXECUTION_CALLS = frozenset(
    {
        (
            "src/router/engine.py",
            "RoutingEngine._apply_skills",
            "skill.execute",
        ),
        (
            "src/utils/email_processor.py",
            "EmailProcessor.process_batch",
            "email.copy",
        ),
    }
)
_TRUSTED_DYNAMIC_SQL_EXECUTION_SHAPES = {
    (
        "scripts/reprocess_email.py",
        "list_stuck_emails",
        "cur.execute",
    ): {
        # Status placeholders are generated separately from a fixed SELECT.
        "a3f6bd7bf63fced2fa8eee95a16b6a0ab645871580da8064c1689031d88cccaa": 1,
    },
    (
        "src/db/auditor.py",
        "require_checkpoint_auditor_database_role",
        "conn.execute",
    ): {
        # Fixed catalog query with three separately tested SQL fragments.
        "62931e64d649ee962ce4ff4508f44d80481eaa183ab0d1fd3770695481ac6eb9": 1,
    },
    (
        "src/db/bootstrap.py",
        "_apply_database_access_contract",
        "cursor.execute",
    ): {
        # GRANT USAGE ON SCHEMA.
        "09154950a018e8194476538bcf73fe2d3fdd55a8f450fcd719e0b296ef59179a": 1,
        # REVOKE ALL PRIVILEGES ON DATABASE.
        "499bce4168e130b8108f3f97bea7d608983c7865906ab3746557b8334586a48d": 1,
        # REVOKE ALL PRIVILEGES ON SCHEMA.
        "6eb7cf6d70cf42bf8a4f2974661da228538775770e8f44d3a91345db9365c11a": 1,
        # GRANT CONNECT ON DATABASE.
        "b7121817f7e4b54394a61e7121bf9420f186c60c160a35df1a6b1f6fd7c751d5": 1,
    },
    (
        "src/db/bootstrap.py",
        "_apply_checkpoint_migrations",
        "cur.execute",
    ): {
        # migration and migrations[0] are the two approved fully dynamic nodes.
        "2d89eba3bea05c135c362fa01ea80279726b42d2fcc8b0dc8396d15e320984b0": 1,
        "504f5cab3f094a3bf34e3e23823532676b73bfefc75b139fbcd8b3c15af43365": 1,
    },
    (
        "src/db/bootstrap.py",
        "_ensure_checkpoint_index",
        "cur.execute",
    ): {
        # migration and spec.drop_sql are the two approved fully dynamic nodes.
        "2d89eba3bea05c135c362fa01ea80279726b42d2fcc8b0dc8396d15e320984b0": 1,
        "539735bf4fe5567cbb3f857eed8253b6db5a0395453d6a234bd26ae413d96d4d": 1,
    },
    (
        "src/db/bootstrap.py",
        "_grant_relation_access",
        "cursor.execute",
    ): {
        # DELETE, relation-level, and column-level GRANT shapes.
        "00f483e2606f67aab5e72c52d2999f76b6b754470180cd5a6fd6ee57e5bb6afa": 1,
        "40280c13a21b9df60f1e10bed351663e16388c2515bfe050c887368b5b9313aa": 1,
        "f4802b22fb4b3d028a2f0e014f3279950f45da1dbebcb0628bd62ae50e0a6fa3": 1,
    },
    (
        "src/db/bootstrap.py",
        "_grant_routine_access",
        "cursor.execute",
    ): {
        # GRANT EXECUTE is admitted only after the exact routine manifest is
        # validated and reconciled against pg_proc by the ratcheted function.
        "3058732c7773db93ea9a773938b1e51d44e7cf5692ab487f13a15fa0971e7a24": 1,
    },
    (
        "src/db/bootstrap.py",
        "_require_empty_event_inbox_for_0004",
        "cursor.execute",
    ): {
        "8cbc57e67cb1b2c3a36c8dd8b235d22403a5dfd243f227d363f431b27f68b943": 1,
    },
    (
        "src/db/bootstrap.py",
        "_revoke_relation_access",
        "cursor.execute",
    ): {
        # Column-level and relation-level REVOKE shapes.
        "41552e769455347793db4a5394928a7abe40942dbb33215c24517460489e320d": 1,
        "56c99b6192c33c5d72d39bfe60fd0f59c8572498518e53e7d994a1d7a1982086": 1,
    },
    (
        "src/db/bootstrap.py",
        "_revoke_routine_access",
        "cursor.execute",
    ): {
        # Routine-wide PUBLIC and role revocations use separately frozen AST
        # shapes so neither query can be substituted or repeated silently.
        "e02a23b8e16437cdb7a3cac51064988cfcbe49d44fe87d2ebf20d35ab52ddefb": 1,
        "1b6ccddfdbc744e8ebc31360889747fe8e3d4c368a1a52b3ec845723ba62808a": 1,
    },
    (
        "src/db/roles.py",
        "_fetch_snapshot",
        "cursor.execute",
    ): {
        # The single approved caller-supplied role snapshot query.
        "bcc1067e5aa1d30a2c57375165ce14348c9aeeacf6120797c200adb19c2a3fd8": 1,
    },
    (
        "src/db/provision.py",
        "_require_true_row",
        "cursor.execute",
    ): {"5091c5de978dfb9719b901b76a4ee97a380cbb9a177df621aba2748824992100": 1},
    (
        "src/db/provision.py",
        "_ensure_roles",
        "cursor.execute",
    ): {
        "8e237dcc90b345d7861be6faf3add5e67fdfe3156e86b9c6e132ed02edf80897": 1,
        "94a926f165f5517e7aa3739224dcf95eacf56bb4f5326906f1bb1ac97a199b86": 1,
    },
    (
        "src/db/provision.py",
        "_revoke_role_database_access",
        "cursor.execute",
    ): {
        "3d1bdff0d0ab46689b63e3b18a2a17e3ae2f90e18deac453e39559a0204753e9": 1,
        "170dfec3150b12f1396bc3b4884383d25fa88f67073f45abfa190445181714b2": 1,
    },
    (
        "src/db/provision.py",
        "_apply_database_boundary",
        "cursor.execute",
    ): {
        "526c4ee1a2557a2747242b12cf74cc7d553f9e2f26c470ac0baf6fcdd5548e31": 1,
        "3de6b940ab3bb2c205618dbbab726937678f21fcc75e812b8afc474cedff6a64": 1,
        "b928f40fb391879e46f84e39a33e84687d3edaeab3f63d64bbc73de819896bee": 1,
        "14e8a831c09a4d44cca72231e34e3a8e0da3cd5e9dd91dbc177bde3e450173ce": 1,
        "21a5488c705981733fa5bd22126c8850b3e98fe2fef4c21be500c8df1e962e94": 1,
        "ea94b1985d061b578955a5c83f450c752ccad23bde6fb8b360bd983874a936a1": 1,
        "aa4c5507f772dfb151f596d403c3b03dddcf98c7f5b88ff516a138039a578331": 1,
    },
    (
        "src/db/provision.py",
        "_apply_default_and_large_object_boundary",
        "cursor.execute",
    ): {
        "28c7f1e1810c405f05452964c9ebe4613b8e657f2033501a86a8a23e91eea302": 1,
        "434c9a979e4e11968769b3d504d798b39b6fad38d8fcffb5306b35cbec413ef1": 1,
        "27290b547b12aa239ed200f1674ac417bf11bdaf768e68a0d85ffd42971a7e42": 1,
    },
    (
        "src/ingestion/cold_start.py",
        "ColdStartService._commit_apply_page",
        "connection.execute",
    ): {
        # Reviewed Task-7 plan/Cursor CAS with the frozen RETURNING projection.
        "1f92743c0a4f73774dbec05979227b97903c3327eb2ba756b2d8b3408d628f25": 1,
    },
    (
        "src/ingestion/cold_start.py",
        "ColdStartService._approve_plan",
        "connection.execute",
    ): {
        # Reviewed Task-7 approval CAS with the frozen RETURNING projection.
        "28512f06da531290c246cffcee85e457dd90b264c8083e9bc7cda429713f3b3f": 1,
    },
    (
        "src/ingestion/cold_start.py",
        "ColdStartService._accept_preview",
        "connection.execute",
    ): {
        # Reviewed Task-7 first-plan INSERT with the frozen RETURNING projection.
        "ee486804ee288158a4c33e19a550b99db8197a0b4f5cb8022c3f6ba601bc62ab": 1,
    },
    (
        "src/ingestion/cold_start.py",
        "ColdStartService._write_cold_start_block",
        "connection.execute",
    ): {
        # Reviewed Task-7 blocked-plan CAS with the frozen RETURNING projection.
        "21e44f71cae4cb7a619342410497d0b0f3a9865cdd8552f67f766f6343acca34": 1,
    },
    (
        "src/ingestion/cold_start.py",
        "ColdStartService._write_preview_page_from_context",
        "connection.execute",
    ): {
        # Reviewed Task-7 preview-page CAS with the frozen RETURNING projection.
        "31dd2b5c040f2025e7830f476fcd721461dc14e6b6e886b784b6c0ed0789d345": 1,
    },
    (
        "src/ingestion/cold_start.py",
        "_read_cold_start_plan",
        "connection.execute",
    ): {
        # Reviewed Task-7 locked plan lookup with the frozen SELECT projection.
        "f6988938abbd60747fc07f113626f6de3e83a08022f053a6a93f3812b476b6ed": 1,
    },
    (
        "src/ingestion/command_receipts.py",
        "_CommandReceiptTransaction.insert",
        "self._connection.execute",
    ): {
        # Reviewed Task-7 INSERT through the fixed cold-start receipt view.
        "b65f3762c484dc47122d9932dd8580dd124e0653a0526ef2aae1e1c91fb771b2": 1,
    },
    (
        "src/ingestion/command_receipts.py",
        "_CommandReceiptTransaction._find",
        "self._connection.execute",
    ): {
        # Reviewed Task-7 lookup through the fixed cold-start receipt view.
        "f48c0956662d2e52c4ffaf83cdf4bb010bf49a1456419778340ac5de00b60b78": 1,
    },
}
_TRUSTED_DYNAMIC_NON_SQL_CALL_SHAPES = {
    ("src/server.py", "inject_test_email"): {
        "0e0dbb6d9029a6d2ce5b6f1d191c2b3825cd9061ca292b23b7a2c62d5f51b450": 1,
    },
}
_TASK5_REPOSITORY_STRUCTURAL_AST_SHA256 = {
    "src/domain/email_state.py": (
        "f6171bcd68eab46b13ce3590ada575780f93389bb0790092d669d8adca38e6a5"
    ),
    "src/domain/errors.py": (
        # Reviewed after the replay-safe Exchange detail retry-hint boundary.
        "7357c1746099ae16b71e2a5ad04866a8d1b8eeb4142e58002181163570d2877a"
    ),
    "src/ingestion/repository.py": (
        "a5b01672ba755da251a6fa6b54f7611e3e89555712ecba318e3f6dfc067fa969"
    ),
    "src/ingestion/email_events.py": (
        "a036b9cfbc9da22602155724bda167e5dd09313369d8f1998c76cd872e2d47f5"
    ),
    "src/ingestion/ownership.py": (
        "15ef5d20aba66b3f773d7a6988c4648fde06a3d031b56388ae7ea03603da58d1"
    ),
    "src/ingestion/models.py": (
        "de539221ec5f9829fde3f9078127c91b074d1ce4479233982de6414eebb2f2ab"
    ),
}
_TASK7_REVIEWED_STRUCTURAL_AST_SHA256 = {
    "src/db/bootstrap.py": (
        "f0b78331d0defb8c37921439f3d875992d6bad8baa0a0e84c9aeb3cbbbf3f135"
    ),
    "src/db/roles.py": (
        "20c9c0555184a442ec7cad235d48c3fd7dca36b544b9962cec637e91dc326c23"
    ),
    "src/ingestion/cold_start.py": (
        "399677f46dc6d8d55e5f5b2fdf7f29eaa3a40e1bd1828515687c593999c01e8c"
    ),
    "src/ingestion/command_receipts.py": (
        "7c518629be32d2de2bb212d60733374c9c899262f370f06805681bed4ae23b67"
    ),
    "src/ingestion/models.py": (
        "c11f6c40c7dec34f883a79b32ec5d2b77323e788452d3d106c8798d57d28c650"
    ),
    "src/ingestion/repository.py": (
        "8afdb29b8e6f41e929971e0ebf6eedeb3e7c0fc242851a9e66186eb2adca5392"
    ),
}
_TASK7_TASK5_SUCCESSOR_PATHS = frozenset(
    {
        "src/ingestion/models.py",
        "src/ingestion/repository.py",
    }
)
_TASK8_REVIEWED_STRUCTURAL_AST_SHA256 = {
    # AttachmentPolicy is the shared admission boundary for Drive uploads.
    "src/exchange_service.py": (
        "33eb53e00b17ed7d4870b7e9cab202d363c31599a1f87e6b5c9ed93f63530ed2"
    ),
    "src/ingestion/processing.py": (
        "5fa7573575386d597da77b5f27f59f7b79378d5f2318b2d5d81c83862616bbf5"
    ),
    "src/ingestion/legacy_adapter.py": (
        "4bc7bf8f98dc212f2712b3b861cf777369bba1d71d81bcb7d070304deb796be5"
    ),
    "src/ingestion/worker.py": (
        "625dbcb0a7fed84c81b7ec980716aa7d47144dec4fafd5bbcee763e479842be3"
    ),
    "src/ingestion/repository.py": (
        "a289982ac850c3066896da788b2fccd3f8ceccf8621eb59a6815cd81754f71bb"
    ),
}
_TASK8_TASK7_SUCCESSOR_PATHS = frozenset({"src/ingestion/repository.py"})
_TASK9G_TASK8_SUCCESSOR_PATHS = frozenset({"src/ingestion/repository.py"})
_TASK9G_NON_SQL_SUCCESSOR_PATHS = frozenset({"src/server.py"})
_TASK9G_REVIEWED_STRUCTURAL_AST_SHA256 = {
    "src/ingestion/normalization.py": (
        "cb6222ff4e5b31ea94ff02ccdd2b9efee275b8ec973dd0176016b58700cc03f6"
    ),
    "src/ingestion/webhook.py": (
        "c48ab2627177b2e155b687510c6d25a9a9d12f006223f5394fe302b2785b9aef"
    ),
    "src/ingestion/repository.py": (
        "16f120442ede35a86fd03674906f25cd870c6563eb51416f3dde769d68175d71"
    ),
    "src/server.py": (
        "63444311ebb01488cb6c230996f80c0e95ae40c9f8ded64a105b10ff00d287a3"
    ),
}
_TASK10G_TASK7_SUCCESSOR_PATHS = frozenset(
    {
        "src/db/bootstrap.py",
        "src/db/roles.py",
        "src/ingestion/models.py",
    }
)
_TASK10G_TASK8_SUCCESSOR_PATHS = frozenset({"src/ingestion/worker.py"})
_TASK10G_TASK9G_SUCCESSOR_PATHS = frozenset(
    {
        "src/ingestion/normalization.py",
        "src/ingestion/repository.py",
    }
)
_TASK10G_REVIEWED_STRUCTURAL_AST_SHA256 = {
    "src/db/bootstrap.py": (
        "4cf3eb4fc7f47581c24a80021faf461c57df49f3aadc402494c05ef8dce9984a"
    ),
    "src/db/roles.py": (
        "95ab9de4b7ba0f33b364c960ebbcf0497db820a6cb779324159f38d194df6849"
    ),
    "src/ingestion/models.py": (
        "8c12ee8abd5e8f501cddba941c8dabba9b4512ce6cdc68940a29808ed925b37f"
    ),
    "src/ingestion/normalization.py": (
        "c22d79b73a0aeaffd76046a1be28a10b66b0dc4f3040019ccec002f06050e688"
    ),
    "src/ingestion/repository.py": (
        "6087475a59cc3647d0330a8bd4117c2b2bbe8e84376bdf8aef74faabea387e55"
    ),
    "src/ingestion/worker.py": (
        "20e87718472f4a39e6f5277f17321b3ef5cad83f5b25c9a4ca92c5652827a32a"
    ),
}
_TASK11G_PREDECESSOR_PATHS = frozenset(
    {
        "src/ingestion/repository.py",
        "src/server.py",
    }
)
_TASK11G_REVIEWED_STRUCTURAL_AST_SHA256 = {
    "src/ingestion/repository.py": (
        "4f1e138bce5fceb6782dc94d4d66444754386b6883d2ce78eaf213a35044822a"
    ),
    "src/server.py": (
        "34af871f76b9827675d38fc909027425078f01153820e32c6e01e0f1b18adf26"
    ),
}
_PHASE4_LITE_TASK8_SUCCESSOR_PATHS = frozenset(
    {
        "src/ingestion/legacy_adapter.py",
        "src/ingestion/processing.py",
    }
)
_PHASE4_LITE_TASK10G_SUCCESSOR_PATHS = frozenset(
    {
        "src/db/roles.py",
        "src/ingestion/worker.py",
    }
)
_PHASE4_LITE_TASK11G_SUCCESSOR_PATHS = frozenset(
    {
        "src/ingestion/repository.py",
        "src/server.py",
    }
)
_PHASE4_LITE_REVIEWED_STRUCTURAL_AST_SHA256 = {
    "src/db/access_contract.py": (
        "2d930679f8ab1ca121b0485e101ea0dab72e6a04e18665045ad87956cd277ff2"
    ),
    "src/db/roles.py": (
        "1f37d4f16ae6f4b5422d7b71e0955ea3740017933fdb257c6276224dcfffc94a"
    ),
    "src/ingestion/legacy_adapter.py": (
        # Reviewed after propagating a validated Exchange Retry-After hint.
        "523a6fc1028940a6b97dd66086fde8492648fb92621ab5f149569c6c42fbad17"
    ),
    "src/ingestion/processing.py": (
        "48a2531595f37502be2b07b592f211e53fff2df50f4e785b7c82c1d7a90bda77"
    ),
    "src/ingestion/repository.py": (
        "b8e2544ab54bc0c87a72a8148567966b9cfa8df56382c5329d5bdc130a913830"
    ),
    "src/ingestion/runtime.py": (
        "879573e600d3eb8a1dd96be7f81ae4001e2434159ca697ac17c6b7be6ab944a1"
    ),
    "src/ingestion/worker.py": (
        "75ed0dfeda6d6010d4b91741ee2679f2658b82afec8b1a8d303a8eecf0b066f0"
    ),
    "src/init_app.py": (
        "43af96b3596113d0745e1b6bb6c13fa7acbd0aca0f089d4bec7eec13d3bfce0a"
    ),
    "src/server.py": (
        "d838a61f841ca8af7b19117bc60214706d13310d7a7b401f58dc711af4241f1d"
    ),
}
_TRUSTED_DYNAMIC_SQL_FILE_STRUCTURAL_AST_SHA256 = {
    "scripts/reprocess_email.py": (
        "5214cf76cd516974cd453f7b203b05d4a962023b202c28ea9d3f5ceef5a6e24b"
    ),
    "src/db/auditor.py": (
        "3db5f148a405402f21ca61fa3e465f7453cda8f99b7ed36986d2e2fb8903fde9"
    ),
    "src/db/bootstrap.py": _TASK10G_REVIEWED_STRUCTURAL_AST_SHA256[
        "src/db/bootstrap.py"
    ],
    "src/db/roles.py": _PHASE4_LITE_REVIEWED_STRUCTURAL_AST_SHA256[
        "src/db/roles.py"
    ],
    "src/db/provision.py": (
        "04f72590b1098d1b60356b34e52221ba94cb39113e490821134cdc8bd3ac8483"
    ),
    "src/ingestion/cold_start.py": _TASK7_REVIEWED_STRUCTURAL_AST_SHA256[
        "src/ingestion/cold_start.py"
    ],
    "src/ingestion/command_receipts.py": _TASK7_REVIEWED_STRUCTURAL_AST_SHA256[
        "src/ingestion/command_receipts.py"
    ],
}
_NON_SQL_EXCEPTION_FILE_STRUCTURAL_AST_SHA256 = {
    # Tier 3 is now reachable only through the post-Tier-2 fallback seam.
    "src/router/engine.py": (
        "9a2336ce5b9eeace9c1c2b3a110422b5864e7e282100b4262325b9f0e2cf46e2"
    ),
    "src/server.py": _PHASE4_LITE_REVIEWED_STRUCTURAL_AST_SHA256["src/server.py"],
    # Reviewed after splitting current-message embeddings from bounded quoted
    # history and removing raw provider alternative bodies; the approved
    # email.copy call remains the only non-SQL exception.
    "src/utils/email_processor.py": (
        "6bca4ef1e07cbed5ab8188d12de451ca29ba01cccf2eff28c5cf7d17998905b7"
    ),
}
_EMAIL_MUTATION = re.compile(
    r"""
    (?:
        \b(?:
            insert\s+into
            |
            update\s+(?:only\s+)?
            |
            delete\s+from\s+(?:only\s+)?
            |
            merge\s+into\s+(?:only\s+)?
            |
            truncate\s+(?:table\s+)?(?:only\s+)?
        )
        \s*
        (?:(?:"[^"]+"|[a-z_][a-z0-9_$]*)\s*\.\s*)?
        (?:"emails"|emails\b)
        |
        \bcopy\s+
        (?:(?:"[^"]+"|[a-z_][a-z0-9_$]*)\s*\.\s*)?
        (?:"emails"|emails\b)
        (?:\s*\([^)]*\))?\s+from\b
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _call_name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _expression_path(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        value = _expression_path(node.value)
        return f"{value}.{node.attr}" if value else None
    return None


def _execution_policy_matches(
    filename: str,
    owner: str,
    call_path: str | None,
    policies: frozenset[tuple[str, str, str]],
) -> bool:
    normalized = _project_relative_path(filename)
    return call_path is not None and any(
        normalized == path and owner == allowed_owner and call_path == allowed_call
        for path, allowed_owner, allowed_call in policies
    )


def _project_relative_path(filename: str) -> str | None:
    candidate = Path(filename)
    if not candidate.is_absolute():
        return None
    project_root = Path(__file__).resolve().parents[2]
    try:
        return candidate.resolve().relative_to(project_root).as_posix()
    except ValueError:
        return None


_AST_DUMP_SUPPORTS_SHOW_EMPTY = "show_empty" in inspect.signature(ast.dump).parameters


def _normalized_ast_dump(node: ast.AST) -> str:
    if _AST_DUMP_SUPPORTS_SHOW_EMPTY:
        return ast.dump(node, include_attributes=False, **{"show_empty": True})
    return ast.dump(node, include_attributes=False)


def _normalized_file_ast_sha256(path: Path) -> str:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    normalized = _normalized_ast_dump(tree)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _trusted_dynamic_execution_matches(
    filename: str,
    owner: str,
    call_path: str | None,
    query: ast.expr | None,
    bindings: dict[str, list[ast.expr]],
    occurrences: dict[tuple[str, str, str, str], int],
) -> bool:
    normalized = _project_relative_path(filename)
    if normalized is None or call_path is None or query is None:
        return False
    policy_key = (normalized, owner, call_path)
    shape = hashlib.sha256(_normalized_ast_dump(query).encode("utf-8")).hexdigest()
    maximum = _TRUSTED_DYNAMIC_SQL_EXECUTION_SHAPES.get(policy_key, {}).get(shape)
    if maximum is None:
        return False
    if isinstance(query, ast.Name):
        query_bindings = bindings.get(query.id, [])
        if not query_bindings or any(
            not isinstance(value, ast.Name) or value.id != _DYNAMIC_IDENTIFIER
            for value in query_bindings
        ):
            return False
    occurrence_key = (*policy_key, shape)
    used = occurrences.get(occurrence_key, 0)
    if used >= maximum:
        return False
    occurrences[occurrence_key] = used + 1
    return True


def _trusted_dynamic_non_sql_call_matches(
    filename: str,
    owner: str,
    call: ast.Call,
    occurrences: dict[tuple[str, str, str], int],
) -> bool:
    normalized = _project_relative_path(filename)
    if normalized is None:
        return False
    policy_key = (normalized, owner)
    shape = hashlib.sha256(_normalized_ast_dump(call).encode("utf-8")).hexdigest()
    maximum = _TRUSTED_DYNAMIC_NON_SQL_CALL_SHAPES.get(policy_key, {}).get(shape)
    if maximum is None:
        return False
    occurrence_key = (*policy_key, shape)
    used = occurrences.get(occurrence_key, 0)
    if used >= maximum:
        return False
    occurrences[occurrence_key] = used + 1
    return True


def _scope_nodes(root: ast.AST):
    pending = list(ast.iter_child_nodes(root))
    while pending:
        node = pending.pop()
        if isinstance(
            node,
            (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef, ast.Lambda),
        ):
            continue
        yield node
        pending.extend(ast.iter_child_nodes(node))


def _scope_bindings(root: ast.AST) -> dict[str, list[ast.expr]]:
    bindings: dict[str, list[ast.expr]] = {}

    def mark_dynamic(target: ast.expr) -> None:
        if isinstance(target, (ast.List, ast.Tuple)):
            for element in target.elts:
                mark_dynamic(element)
            return
        current = target
        while isinstance(current, (ast.Attribute, ast.Subscript)):
            current = current.value
        if isinstance(current, ast.Name):
            bindings.setdefault(current.id, []).append(
                ast.Name(id=_DYNAMIC_IDENTIFIER, ctx=ast.Load())
            )

    def bind_assignment(target: ast.expr, value: ast.expr) -> None:
        if isinstance(target, ast.Name):
            bindings.setdefault(target.id, []).append(value)
            return
        if isinstance(target, (ast.List, ast.Tuple)):
            for index, element in enumerate(target.elts):
                extracted = ast.Subscript(
                    value=value,
                    slice=ast.Constant(value=index),
                    ctx=ast.Load(),
                )
                bind_assignment(element, extracted)
            return
        if isinstance(target, (ast.Starred, ast.Subscript)):
            mark_dynamic(target.value if isinstance(target, ast.Starred) else target)

    for node in _scope_nodes(root):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                bind_assignment(target, node.value)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.value is not None:
                bindings.setdefault(node.target.id, []).append(node.value)
            elif not isinstance(node.target, ast.Name):
                mark_dynamic(node.target)
        elif isinstance(node, ast.AugAssign):
            mark_dynamic(node.target)
        elif isinstance(node, (ast.AsyncFor, ast.For)):
            mark_dynamic(node.target)
        elif isinstance(node, (ast.AsyncWith, ast.With)):
            for item in node.items:
                if item.optional_vars is not None:
                    mark_dynamic(item.optional_vars)
        elif isinstance(node, ast.Delete):
            for target in node.targets:
                mark_dynamic(target)
        elif isinstance(node, ast.ExceptHandler) and node.name is not None:
            bindings.setdefault(node.name, []).append(
                ast.Name(id=_DYNAMIC_IDENTIFIER, ctx=ast.Load())
            )
        elif isinstance(node, (ast.MatchAs, ast.MatchStar)) and node.name is not None:
            bindings.setdefault(node.name, []).append(
                ast.Name(id=_DYNAMIC_IDENTIFIER, ctx=ast.Load())
            )
        elif isinstance(node, ast.MatchMapping) and node.rest is not None:
            bindings.setdefault(node.rest, []).append(
                ast.Name(id=_DYNAMIC_IDENTIFIER, ctx=ast.Load())
            )
        elif isinstance(node, ast.NamedExpr) and isinstance(node.target, ast.Name):
            bindings.setdefault(node.target.id, []).append(node.value)
    return bindings


def _scope_import_binding_names(root: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in _scope_nodes(root):
        if isinstance(node, ast.Import):
            names.update(
                alias.asname or alias.name.partition(".")[0] for alias in node.names
            )
        elif isinstance(node, ast.ImportFrom):
            names.update(
                alias.asname or alias.name for alias in node.names if alias.name != "*"
            )
    return names


def _comprehension_bound_names(root: ast.AST, call: ast.Call) -> frozenset[str]:
    names: set[str] = set()
    for expression in _scope_nodes(root):
        if not isinstance(
            expression,
            (ast.DictComp, ast.GeneratorExp, ast.ListComp, ast.SetComp),
        ) or not any(node is call for node in ast.walk(expression)):
            continue
        for generator in expression.generators:
            names.update(
                node.id
                for node in ast.walk(generator.target)
                if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store)
            )
    return frozenset(names)


def _nested_lambdas(root: ast.AST):
    pending = list(ast.iter_child_nodes(root))
    while pending:
        node = pending.pop()
        if isinstance(node, ast.Lambda):
            yield node
            continue
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef)):
            continue
        pending.extend(ast.iter_child_nodes(node))


def _sequence_expressions(
    node: ast.expr,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str],
) -> list[list[ast.expr]]:
    if isinstance(node, (ast.List, ast.Tuple)):
        return [list(node.elts)]
    if not isinstance(node, ast.Name) or node.id in resolving:
        return []
    return [
        sequence
        for value in bindings.get(node.id, [])
        for sequence in _sequence_expressions(
            value,
            bindings,
            resolving=resolving | {node.id},
        )
    ]


def _mapping_expressions(
    node: ast.expr,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str],
) -> list[dict[str, ast.expr]]:
    if isinstance(node, ast.Dict):
        mapping: dict[str, ast.expr] = {}
        for key, value in zip(node.keys, node.values, strict=True):
            if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
                return []
            mapping[key.value] = value
        return [mapping]
    if not isinstance(node, ast.Name) or node.id in resolving:
        return []
    values = bindings.get(node.id, [])
    if any(
        isinstance(value, ast.Name) and value.id == _DYNAMIC_IDENTIFIER
        for value in values
    ):
        return []
    return [
        mapping
        for value in values
        for mapping in _mapping_expressions(
            value,
            bindings,
            resolving=resolving | {node.id},
        )
    ]


def _cartesian_identifier(
    arguments: list[ast.expr],
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str],
) -> list[str]:
    rendered = [""]
    for argument in arguments:
        values = _render_sql(
            argument,
            bindings,
            resolving=resolving,
        ) or [_DYNAMIC_IDENTIFIER]
        rendered = [
            value if not prefix else f"{prefix}.{value}"
            for prefix in rendered
            for value in values
        ]
    return rendered or [_DYNAMIC_IDENTIFIER]


def _render_sql(
    node: ast.expr,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str] = frozenset(),
) -> list[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.JoinedStr):
        rendered = [""]
        for part in node.values:
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                values = [part.value]
            elif isinstance(part, ast.FormattedValue):
                values = _render_sql(
                    part.value,
                    bindings,
                    resolving=resolving,
                ) or [_DYNAMIC_IDENTIFIER]
            else:
                values = [_DYNAMIC_IDENTIFIER]
            rendered = [prefix + value for prefix in rendered for value in values]
        return rendered
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _render_sql(node.left, bindings, resolving=resolving)
        right = _render_sql(node.right, bindings, resolving=resolving)
        return [left_part + right_part for left_part in left for right_part in right]
    if isinstance(node, (ast.List, ast.Tuple)):
        rendered = [""]
        for element in node.elts:
            values = _render_sql(
                element,
                bindings,
                resolving=resolving,
            ) or [_DYNAMIC_IDENTIFIER]
            rendered = [prefix + value for prefix in rendered for value in values]
        return rendered
    if isinstance(node, ast.Name):
        if node.id in resolving:
            return [_DYNAMIC_IDENTIFIER]
        values = bindings.get(node.id, [])
        if not values:
            return [_DYNAMIC_IDENTIFIER]
        return [
            rendered
            for value in values
            for rendered in (
                _render_sql(
                    value,
                    bindings,
                    resolving=resolving | {node.id},
                )
                or [_DYNAMIC_IDENTIFIER]
            )
        ]
    if isinstance(node, ast.IfExp):
        return [
            rendered
            for branch in (node.body, node.orelse)
            for rendered in _render_sql(
                branch,
                bindings,
                resolving=resolving,
            )
        ]
    if not isinstance(node, ast.Call):
        return []
    call_name = _call_name(node.func)
    if call_name in _SQL_TEXT_CONSTRUCTORS and node.args:
        return _render_sql(node.args[0], bindings, resolving=resolving)
    if call_name in _SQL_IDENTIFIER_CONSTRUCTORS:
        return _cartesian_identifier(
            list(node.args),
            bindings,
            resolving=resolving,
        )
    if call_name in _TRUSTED_TABLE_CONSTRUCTORS:
        trusted_prefix = getattr(
            node,
            _TRUSTED_TABLE_MARKER_ATTRIBUTE,
            _TRUSTED_TABLE_PREFIX,
        )
        values = (
            _render_sql(node.args[0], bindings, resolving=resolving)
            if len(node.args) == 1
            else [_DYNAMIC_IDENTIFIER]
        )
        return [
            (
                f"{trusted_prefix}.{value}"
                if value != _DYNAMIC_IDENTIFIER
                and "." not in value
                and value.replace("$", "_").isidentifier()
                else _DYNAMIC_IDENTIFIER
            )
            for value in values
        ]
    if call_name == "Composed" and node.args:
        return _render_sql(node.args[0], bindings, resolving=resolving)
    if isinstance(node.func, ast.Attribute) and node.func.attr == "join" and node.args:
        separators = _render_sql(node.func.value, bindings, resolving=resolving)
        value_groups: list[list[list[str]]] = []
        sequence = node.args[0]
        if isinstance(sequence, ast.GeneratorExp):
            if (
                len(sequence.generators) == 1
                and isinstance(sequence.generators[0].target, ast.Name)
                and not sequence.generators[0].ifs
                and not sequence.generators[0].is_async
            ):
                generator = sequence.generators[0]
                for elements in _sequence_expressions(
                    generator.iter,
                    bindings,
                    resolving=resolving,
                ):
                    rendered_elements: list[list[str]] = []
                    for element in elements:
                        generator_bindings = {
                            name: list(values) for name, values in bindings.items()
                        }
                        generator_bindings[generator.target.id] = [element]
                        rendered_elements.append(
                            _render_sql(
                                sequence.elt,
                                generator_bindings,
                                resolving=resolving,
                            )
                            or [_DYNAMIC_IDENTIFIER]
                        )
                    value_groups.append(rendered_elements)
        else:
            for elements in _sequence_expressions(
                sequence,
                bindings,
                resolving=resolving,
            ):
                value_groups.append(
                    [
                        _render_sql(element, bindings, resolving=resolving)
                        or [_DYNAMIC_IDENTIFIER]
                        for element in elements
                    ]
                )
        rendered: list[str] = []
        for element_values in value_groups:
            for separator in separators:
                statements = [""]
                for index, values in enumerate(element_values):
                    prefix = "" if index == 0 else separator
                    statements = [
                        statement + prefix + value
                        for statement in statements
                        for value in values
                    ]
                rendered.extend(statements)
        return rendered
    if isinstance(node.func, ast.Attribute) and node.func.attr == "format":
        templates = _render_sql(node.func.value, bindings, resolving=resolving)
        positional = list(templates)
        for index, argument in enumerate(node.args):
            values = _render_sql(
                argument,
                bindings,
                resolving=resolving,
            ) or [_DYNAMIC_IDENTIFIER]
            expanded: list[str] = []
            for statement in positional:
                marker = "{}" if "{}" in statement else "{" + str(index) + "}"
                expanded.extend(statement.replace(marker, value, 1) for value in values)
            positional = expanded

        environments: list[dict[str, str]] = [{}]
        for keyword in node.keywords:
            if keyword.arg is not None:
                mappings = [{keyword.arg: keyword.value}]
            else:
                mappings = _mapping_expressions(
                    keyword.value,
                    bindings,
                    resolving=resolving,
                )
            if not mappings:
                environments = [
                    {**environment, "__unresolved__": _DYNAMIC_IDENTIFIER}
                    for environment in environments
                ]
                continue
            expanded_environments: list[dict[str, str]] = []
            for environment in environments:
                for mapping in mappings:
                    candidates = [dict(environment)]
                    for key, value_node in mapping.items():
                        values = _render_sql(
                            value_node,
                            bindings,
                            resolving=resolving,
                        ) or [_DYNAMIC_IDENTIFIER]
                        candidates = [
                            {**candidate, key: value}
                            for candidate in candidates
                            for value in values
                        ]
                    expanded_environments.extend(candidates)
            environments = expanded_environments

        rendered: list[str] = []
        for statement in positional:
            for environment in environments:
                formatted = statement
                for key, value in environment.items():
                    if key != "__unresolved__":
                        formatted = formatted.replace("{" + key + "}", value)
                if "__unresolved__" in environment:
                    formatted = re.sub(r"\{[^{}]+\}", _DYNAMIC_IDENTIFIER, formatted)
                rendered.append(formatted)
        return rendered
    return []


def _is_sql_execution(
    call: ast.Call,
    bindings: dict[str, list[ast.expr]] | None = None,
) -> bool:
    return (
        isinstance(call.func, ast.Attribute)
        and call.func.attr in _SQL_EXECUTION_METHODS
    )


def _container_expressions(
    node: ast.expr,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str],
) -> list[ast.expr]:
    if isinstance(node, ast.Name):
        if node.id in resolving:
            return []
        return [
            candidate
            for value in bindings.get(node.id, [])
            for candidate in _container_expressions(
                value,
                bindings,
                resolving=resolving | {node.id},
            )
        ]
    if isinstance(node, ast.Subscript):
        return _subscript_expressions(
            node,
            bindings,
            resolving=resolving,
        )
    return [node]


def _subscript_keys(
    node: ast.expr,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str],
) -> set[str | int] | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, (str, int)):
        return {node.value}
    candidates = _string_candidates(node, bindings, resolving=resolving)
    if _DYNAMIC_IDENTIFIER in candidates:
        return None
    return set(candidates)


def _subscript_expressions(
    node: ast.Subscript,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str],
) -> list[ast.expr]:
    keys = _subscript_keys(node.slice, bindings, resolving=resolving)
    values: list[ast.expr] = []
    for container in _container_expressions(
        node.value,
        bindings,
        resolving=resolving,
    ):
        if isinstance(container, ast.Dict):
            for key, value in zip(container.keys, container.values, strict=True):
                if key is None:
                    continue
                if keys is None or (
                    isinstance(key, ast.Constant) and key.value in keys
                ):
                    values.append(value)
        elif isinstance(container, (ast.List, ast.Tuple)):
            if keys is None:
                values.extend(container.elts)
                continue
            for key in keys:
                if isinstance(key, int) and -len(container.elts) <= key < len(
                    container.elts
                ):
                    values.append(container.elts[key])
    return values


def _string_candidates(
    node: ast.expr,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str] = frozenset(),
) -> set[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return {node.value}
    if isinstance(node, ast.Name):
        if node.id in resolving:
            return {_DYNAMIC_IDENTIFIER}
        values = bindings.get(node.id, [])
        if not values:
            return {_DYNAMIC_IDENTIFIER}
        return {
            candidate
            for value in values
            for candidate in _string_candidates(
                value,
                bindings,
                resolving=resolving | {node.id},
            )
        }
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _string_candidates(node.left, bindings, resolving=resolving)
        right = _string_candidates(node.right, bindings, resolving=resolving)
        if _DYNAMIC_IDENTIFIER in left | right:
            return {_DYNAMIC_IDENTIFIER}
        return {left_part + right_part for left_part in left for right_part in right}
    if isinstance(node, ast.IfExp):
        return _string_candidates(
            node.body, bindings, resolving=resolving
        ) | _string_candidates(node.orelse, bindings, resolving=resolving)
    if isinstance(node, ast.BoolOp):
        return {
            candidate
            for value in node.values
            for candidate in _string_candidates(value, bindings, resolving=resolving)
        }
    if isinstance(node, ast.NamedExpr):
        return _string_candidates(node.value, bindings, resolving=resolving)
    if isinstance(node, ast.Subscript):
        values = _subscript_expressions(
            node,
            bindings,
            resolving=resolving,
        )
        if not values:
            return {_DYNAMIC_IDENTIFIER}
        return {
            candidate
            for value in values
            for candidate in _string_candidates(
                value,
                bindings,
                resolving=resolving,
            )
        }
    return {_DYNAMIC_IDENTIFIER}


def _has_dangerous_reflective_name(
    node: ast.expr,
    bindings: dict[str, list[ast.expr]],
    *,
    reject_dynamic: bool,
) -> bool:
    candidates = {candidate.lower() for candidate in _string_candidates(node, bindings)}
    static_candidates = candidates - {_DYNAMIC_IDENTIFIER}
    return (
        (reject_dynamic and _DYNAMIC_IDENTIFIER in candidates)
        or not all(candidate.isidentifier() for candidate in static_candidates)
        or bool(static_candidates & (_SQL_EXECUTION_METHODS | _DYNAMIC_CODE_NAME_CALLS))
    )


def _is_reflection_primitive(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return node.id in _REFLECTION_NAME_PRIMITIVES
    if not isinstance(node, ast.Attribute):
        return False
    path = _expression_path(node)
    return path in _REFLECTION_ATTRIBUTE_PRIMITIVE_PATHS or node.attr in {
        "__getattr__",
        "__getattribute__",
    }


def _is_dangerous_reflective_lookup(
    node: ast.AST,
    bindings: dict[str, list[ast.expr]],
    *,
    reject_dynamic: bool,
) -> bool:
    if isinstance(node, ast.Call):
        call_name = _call_name(node.func)
        call_path = _expression_path(node.func)
        if call_name == "getattr":
            return len(node.args) < 2 or _has_dangerous_reflective_name(
                node.args[1], bindings, reject_dynamic=reject_dynamic
            )
        if call_name in {"attrgetter", "methodcaller"}:
            return not node.args or _has_dangerous_reflective_name(
                node.args[0], bindings, reject_dynamic=reject_dynamic
            )
        if call_name in {"__getattr__", "__getattribute__"}:
            method_index = 1 if call_path == "object.__getattribute__" else 0
            return len(node.args) <= method_index or _has_dangerous_reflective_name(
                node.args[method_index],
                bindings,
                reject_dynamic=reject_dynamic,
            )
        if call_path == "dict.__getitem__":
            return len(node.args) < 2 or _has_dangerous_reflective_name(
                node.args[1], bindings, reject_dynamic=reject_dynamic
            )
    return (
        isinstance(node, ast.Subscript)
        and (
            isinstance(node.value, ast.Name)
            and node.value.id == "__builtins__"
            or isinstance(node.value, ast.Call)
            and _call_name(node.value.func) in {"globals", "locals", "vars"}
        )
        and _has_dangerous_reflective_name(
            node.slice,
            bindings,
            reject_dynamic=reject_dynamic,
        )
    )


def _resolves_to_reflective_callable(
    node: ast.expr,
    bindings: dict[str, list[ast.expr]],
    *,
    resolving: frozenset[str] = frozenset(),
) -> bool:
    if isinstance(node, (ast.Call, ast.Subscript)):
        return _is_dangerous_reflective_lookup(
            node,
            bindings,
            reject_dynamic=True,
        )
    if isinstance(node, ast.Name):
        if node.id in resolving:
            return True
        return any(
            _resolves_to_reflective_callable(
                value,
                bindings,
                resolving=resolving | {node.id},
            )
            for value in bindings.get(node.id, [])
        )
    if isinstance(node, ast.NamedExpr):
        return _resolves_to_reflective_callable(
            node.value,
            bindings,
            resolving=resolving,
        )
    if isinstance(node, ast.IfExp):
        return _resolves_to_reflective_callable(
            node.body,
            bindings,
            resolving=resolving,
        ) or _resolves_to_reflective_callable(
            node.orelse,
            bindings,
            resolving=resolving,
        )
    return False


def _reflective_executor_argument(call: ast.Call) -> ast.expr | None:
    call_name = _call_name(call.func)
    argument_index = 1 if call_name == "run_in_executor" else 0
    if call_name not in {"partial", "run_in_executor", "submit", "to_thread"}:
        return None
    return call.args[argument_index] if len(call.args) > argument_index else None


def _execution_query(
    call: ast.Call,
    bindings: dict[str, list[ast.expr]] | None = None,
) -> ast.expr | None:
    if not _is_sql_execution(call, bindings):
        return None
    if call.args:
        return call.args[0]
    for keyword in call.keywords:
        if keyword.arg in {"operation", "query", "sql", "statement"}:
            return keyword.value
    query_expressions: list[ast.expr] = []
    unresolved_mapping = False
    for keyword in call.keywords:
        if keyword.arg is not None:
            continue
        mappings = _mapping_expressions(
            keyword.value,
            bindings or {},
            resolving=frozenset(),
        )
        if not mappings:
            unresolved_mapping = True
            continue
        for mapping in mappings:
            query_expressions.extend(
                value
                for name, value in mapping.items()
                if name in {"operation", "query", "sql", "statement"}
            )
    if not unresolved_mapping and len(query_expressions) == 1:
        return query_expressions[0]
    return None


def _find_email_mutations(source: str, *, filename: str) -> list[int]:
    tree = ast.parse(source, filename=filename)
    violations: list[int] = []
    trusted_dynamic_occurrences: dict[tuple[str, str, str, str], int] = {}
    trusted_dynamic_non_sql_occurrences: dict[tuple[str, str, str], int] = {}

    def inspect_scope(
        root: ast.AST,
        inherited_bindings: dict[str, list[ast.expr]],
        owner: str,
    ) -> None:
        bindings = {name: list(values) for name, values in inherited_bindings.items()}
        parameter_names: set[str] = set()
        if isinstance(root, (ast.AsyncFunctionDef, ast.FunctionDef, ast.Lambda)):
            arguments = root.args
            parameter_names.update(
                argument.arg
                for argument in (
                    *arguments.posonlyargs,
                    *arguments.args,
                    *arguments.kwonlyargs,
                )
            )
            if arguments.vararg is not None:
                parameter_names.add(arguments.vararg.arg)
            if arguments.kwarg is not None:
                parameter_names.add(arguments.kwarg.arg)
            for name in parameter_names:
                bindings[name] = [ast.Name(id=_DYNAMIC_IDENTIFIER, ctx=ast.Load())]
        for name, values in _scope_bindings(root).items():
            if name in parameter_names:
                bindings[name].extend(values)
            else:
                bindings[name] = values
        for name in _scope_import_binding_names(root):
            bindings.setdefault(name, []).append(
                ast.Name(id=_DYNAMIC_IDENTIFIER, ctx=ast.Load())
            )
        scope_nodes = list(_scope_nodes(root))
        calls = sorted(
            (node for node in scope_nodes if isinstance(node, ast.Call)),
            key=lambda node: (node.lineno, node.col_offset),
        )
        direct_call_functions = {id(node.func) for node in calls}
        for candidate in scope_nodes:
            if (
                isinstance(candidate, ast.Attribute)
                and candidate.attr in _SQL_EXECUTION_METHODS
                and id(candidate) not in direct_call_functions
            ) or (
                _is_reflection_primitive(candidate)
                and id(candidate) not in direct_call_functions
            ):
                violations.append(candidate.lineno)
                continue
            if not isinstance(candidate, ast.Call) and _is_dangerous_reflective_lookup(
                candidate,
                bindings,
                reject_dynamic=False,
            ):
                violations.append(candidate.lineno)
        for node in calls:
            call_bindings = {name: list(values) for name, values in bindings.items()}
            for name in _comprehension_bound_names(root, node):
                call_bindings[name] = [ast.Name(id=_DYNAMIC_IDENTIFIER, ctx=ast.Load())]
            call_path = _expression_path(node.func)
            if _is_dangerous_reflective_lookup(
                node,
                call_bindings,
                reject_dynamic=False,
            ):
                violations.append(node.lineno)
            executor_argument = _reflective_executor_argument(node)
            if _resolves_to_reflective_callable(
                node.func,
                call_bindings,
            ) or (
                executor_argument is not None
                and _resolves_to_reflective_callable(
                    executor_argument,
                    call_bindings,
                )
            ):
                violations.append(node.lineno)
            if _execution_policy_matches(
                filename,
                owner,
                call_path,
                _NON_SQL_EXECUTION_CALLS,
            ):
                continue
            if (
                isinstance(node.func, ast.Name)
                and node.func.id in _DYNAMIC_CODE_NAME_CALLS
            ):
                violations.append(node.lineno)
                continue
            if not isinstance(node.func, (ast.Attribute, ast.Name)):
                if not _trusted_dynamic_non_sql_call_matches(
                    filename,
                    owner,
                    node,
                    trusted_dynamic_non_sql_occurrences,
                ):
                    violations.append(node.lineno)
                continue
            query = _execution_query(node, call_bindings)
            if query is None:
                if _is_sql_execution(
                    node, call_bindings
                ) and not _trusted_dynamic_execution_matches(
                    filename,
                    owner,
                    call_path,
                    query,
                    call_bindings,
                    trusted_dynamic_occurrences,
                ):
                    violations.append(node.lineno)
                continue
            statements = _render_sql(query, call_bindings)
            if any(_EMAIL_MUTATION.search(statement) for statement in statements):
                violations.append(node.lineno)
                continue
            unresolved = not statements or any(
                _DYNAMIC_IDENTIFIER in statement for statement in statements
            )
            if unresolved and not _trusted_dynamic_execution_matches(
                filename,
                owner,
                call_path,
                query,
                call_bindings,
                trusted_dynamic_occurrences,
            ):
                violations.append(node.lineno)
        for child in ast.iter_child_nodes(root):
            if isinstance(child, (ast.AsyncFunctionDef, ast.FunctionDef, ast.ClassDef)):
                child_owner = f"{owner}.{child.name}" if owner else child.name
                inspect_scope(child, bindings, child_owner)
        for child in _nested_lambdas(root):
            child_owner = f"{owner}.<lambda>" if owner else "<lambda>"
            inspect_scope(child, bindings, child_owner)

    inspect_scope(tree, {}, "")
    return sorted(set(violations))


def _render_bound_expression(source: str, name: str) -> list[str]:
    tree = ast.parse(source, filename="<renderer-contract>")
    bindings = _scope_bindings(tree)
    return _render_sql(bindings[name][-1], bindings)


def test_renderer_preserves_identifier_candidates_and_unresolved_parts() -> None:
    source = """
schema = "tenant_a"
schema = "tenant_b"
table = "emails"
table = unresolved_table
identifier = sql.Identifier(schema, table)
"""

    assert set(_render_bound_expression(source, "identifier")) == {
        "tenant_a.dynamic_identifier",
        "tenant_a.emails",
        "tenant_b.dynamic_identifier",
        "tenant_b.emails",
    }


def test_renderer_expands_format_cartesian_and_static_kwargs() -> None:
    source = """
verb = "UPDATE"
verb = "INSERT INTO"
target = sql.Identifier("emails")
target = sql.Identifier("audit_events")
parts = {"verb": verb, "target": target}
positional = sql.SQL("{} {}").format(verb, target)
keyword = sql.SQL("{verb} {target}").format(**parts)
"""
    expected = {
        "INSERT INTO audit_events",
        "INSERT INTO emails",
        "UPDATE audit_events",
        "UPDATE emails",
    }

    assert set(_render_bound_expression(source, "positional")) == expected
    assert set(_render_bound_expression(source, "keyword")) == expected


def test_renderer_marks_only_table_helper_as_trusted_in_compositions() -> None:
    source = """
trusted = _table("emails")
plain = sql.Identifier("emails")
composed = sql.Composed([sql.SQL("UPDATE "), trusted, sql.SQL(" SET status = %s")])
joined = sql.SQL(" ").join([sql.SQL("UPDATE"), trusted, sql.SQL("SET status = %s")])
"""

    assert _render_bound_expression(source, "trusted") == [
        "__task5_trusted_schema__.emails"
    ]
    assert _render_bound_expression(source, "plain") == ["emails"]
    assert _render_bound_expression(source, "composed") == [
        "UPDATE __task5_trusted_schema__.emails SET status = %s"
    ]
    assert _render_bound_expression(source, "joined") == [
        "UPDATE __task5_trusted_schema__.emails SET status = %s"
    ]


def test_renderer_expands_generator_join_over_static_tuple() -> None:
    source = """
columns = ("id", "status")
returning = sql.SQL(", ").join(
    sql.SQL("e.{}").format(sql.Identifier(column)) for column in columns
)
"""

    assert _render_bound_expression(source, "returning") == ["e.id, e.status"]


def test_renderer_expands_static_conditional_composition_branches() -> None:
    source = """
predicate = sql.SQL(" AND state = %s") if enabled else sql.SQL("")
query = sql.SQL("SELECT id FROM {} WHERE account_id = %s{}").format(
    _table("pipeline_ownership"),
    predicate,
)
"""

    assert set(_render_bound_expression(source, "query")) == {
        "SELECT id FROM __task5_trusted_schema__.pipeline_ownership "
        "WHERE account_id = %s",
        "SELECT id FROM __task5_trusted_schema__.pipeline_ownership "
        "WHERE account_id = %s AND state = %s",
    }


def test_detector_handles_composed_fstring_and_keyword_sql_only_at_execution() -> None:
    source = """
async def mutate(cursor, sql, _table, schema):
    composed = sql.SQL("InSeRt Into {} (id) VALUES (%s)").format(_table("emails"))
    await cursor.execute(composed)
    await cursor.execute(statement=f"UPDATE {schema}.emails SET status = 'sent'")
    await cursor.execute(query='DELETE FROM ONLY "runtime"."emails" WHERE id = %s')
    logger.info("TRUNCATE emails is forbidden explanatory text")
"""

    assert _find_email_mutations(source, filename="<composition-contract>") == [
        4,
        5,
        6,
    ]


def test_detector_resolves_bound_identifier_inside_fstring() -> None:
    source = """
async def mutate(cursor):
    table = "emails"
    await cursor.execute(f"UPDATE {table} SET status = 'processing'")
"""

    assert _find_email_mutations(source, filename="<bound-fstring-contract>") == [4]


def test_detector_handles_psycopg_composed_and_joined_sql() -> None:
    source = """
async def mutate(cursor, sql):
    composed = sql.Composed([
        sql.SQL("UPDATE "),
        sql.Identifier("emails"),
        sql.SQL(" SET status = %s"),
    ])
    await cursor.execute(composed)
    joined = sql.SQL(" ").join((
        sql.SQL("UPDATE"),
        sql.Identifier("emails"),
        sql.SQL("SET status = %s"),
    ))
    await cursor.execute(joined)
"""

    assert _find_email_mutations(source, filename="<psycopg-composed-contract>") == [
        8,
        14,
    ]


def test_detector_covers_every_mutation_form_and_ignores_select_and_copy_to() -> None:
    source = """
async def mutate(cursor, sql, _table):
    await cursor.execute('INSERT INTO emails (id) VALUES (%s)')
    await cursor.executemany('UPDATE ONLY "emails" SET status = %s', rows)
    await cursor.exec_driver_sql('DELETE FROM runtime.emails')
    await cursor.execute(sql.SQL('MERGE INTO {} USING source ON false').format(sql.Identifier('runtime', 'emails')))
    await cursor.execute(sql.SQL('TRUNCATE TABLE {}').format(_table('emails')))
    async with cursor.copy('COPY "runtime"."emails" (id) FROM STDIN') as writer:
        await writer.write_row((1,))
    await cursor.copy_expert(sql='COPY emails FROM STDIN', file=stream)
    await cursor.execute('SELECT * FROM emails')
    await cursor.execute('COPY emails TO STDOUT')
    await cursor.copy_expert('COPY emails TO STDOUT', stream)
"""

    assert _find_email_mutations(source, filename="<mutation-contract>") == [
        3,
        4,
        5,
        6,
        7,
        8,
        10,
    ]


def test_detector_fail_closes_shadowed_and_unrenderable_execution_queries() -> None:
    source = """
QUERY = "SELECT 1"

async def shadowed(cursor, QUERY):
    await cursor.execute(QUERY)

def query():
    return "UPDATE emails SET status = 'sent'"

async def returned(cursor):
    await cursor.execute(query())

async def partial(cursor, table):
    await cursor.execute(f"UPDATE {table} SET status = 1")
"""
    execution_lines = sorted(
        node.lineno
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "execute"
    )

    assert _find_email_mutations(source, filename="<dynamic-query-contract>") == (
        execution_lines
    )


def test_detector_rejects_module_query_import_rebinding() -> None:
    source = """
QUERY = "SELECT 1"
from evil import QUERY

async def mutate(cursor):
    await cursor.execute(QUERY)
"""

    assert _find_email_mutations(source, filename="<module-query-rebinding>") == [6]


def test_detector_follows_indirect_sql_execution_callables() -> None:
    cases = {
        "name-alias": """
async def mutate(cursor):
    run = cursor.execute
    await run("UPDATE emails SET status = 'sent'")
""",
        "alias-chain": """
async def mutate(cursor):
    first = cursor.execute
    second = first
    await second("UPDATE emails SET status = 'sent'")
""",
        "mapping-subscript": """
async def mutate(cursor):
    calls = {"run": cursor.execute}
    await calls["run"]("UPDATE emails SET status = 'sent'")
""",
        "tuple-unpack": """
async def mutate(cursor):
    run, harmless = (cursor.execute, logger.info)
    await run("UPDATE emails SET status = 'sent'")
""",
        "attribute-alias": """
async def mutate(cursor, holder):
    holder.run = cursor.execute
    await holder.run("UPDATE emails SET status = 'sent'")
""",
        "conditional": """
async def mutate(cursor, use_many):
    await (cursor.execute if use_many else cursor.executemany)(
        "UPDATE emails SET status = 'sent'",
        rows,
    )
""",
        "getattr": """
async def mutate(cursor):
    run = getattr(cursor, "execute")
    await run("UPDATE emails SET status = 'sent'")
""",
        "named-expression": """
async def mutate(cursor):
    await (run := cursor.execute)("UPDATE emails SET status = 'sent'")
""",
        "dunder-getattribute": """
async def mutate(cursor):
    run = cursor.__getattribute__("execute")
    await run("UPDATE emails SET status = 'sent'")
""",
        "attrgetter": """
import operator

async def mutate(cursor):
    run = operator.attrgetter("execute")(cursor)
    await run("UPDATE emails SET status = 'sent'")
""",
        "methodcaller": """
import operator

async def mutate(cursor):
    run = operator.methodcaller("execute", "UPDATE emails SET status = 'sent'")
    await run(cursor)
""",
        "vars-subscript": """
async def mutate(cursor):
    run = vars(cursor)["execute"]
    await run("UPDATE emails SET status = 'sent'")
""",
        "partial": """
import functools

async def mutate(cursor):
    await functools.partial(cursor.execute)(
        "UPDATE emails SET status = 'sent'"
    )
""",
        "to-thread": """
import asyncio

async def mutate(cursor):
    await asyncio.to_thread(
        cursor.execute,
        "UPDATE emails SET status = 'sent'",
    )
""",
    }
    missed: list[str] = []
    for label, source in cases.items():
        actual = _find_email_mutations(
            source,
            filename=f"<indirect-sql-callable-{label}>",
        )
        if not actual:
            missed.append(label)

    assert missed == []


def test_detector_fail_closes_dynamic_code_name_calls() -> None:
    source = """
def mutate():
    exec(payload)
    eval(payload)
    compile(payload, "<dynamic>", "exec")
"""

    assert _find_email_mutations(source, filename="<dynamic-code-calls>") == [3, 4, 5]


def test_detector_rejects_aliased_reflection_primitives() -> None:
    cases = {
        "getattr-alias": """
def mutate(cursor):
    lookup = getattr
    run = lookup(cursor, "execute")
    run("UPDATE emails SET status = 'sent'")
""",
        "method-variable": """
def mutate(cursor):
    method = "execute"
    run = getattr(cursor, method)
    run("UPDATE emails SET status = 'sent'")
""",
        "dynamic-method": """
def mutate(cursor, method):
    run = getattr(cursor, method)
    run("UPDATE emails SET status = 'sent'")
""",
        "dunder-alias": """
def mutate(cursor):
    lookup = cursor.__getattribute__
    run = lookup("execute")
    run("UPDATE emails SET status = 'sent'")
""",
        "dunder-getattr": """
def mutate(cursor):
    run = cursor.__getattr__("execute")
    run("UPDATE emails SET status = 'sent'")
""",
        "attrgetter-alias": """
import operator

def mutate(cursor):
    lookup = operator.attrgetter
    factory = lookup("execute")
    run = factory(cursor)
    run("UPDATE emails SET status = 'sent'")
""",
        "methodcaller-alias": """
import operator

def mutate(cursor):
    lookup = operator.methodcaller
    run = lookup("execute", "UPDATE emails SET status = 'sent'")
    run(cursor)
""",
        "dict-getitem": """
def mutate(cursor):
    run = dict.__getitem__(vars(cursor), "execute")
    run("UPDATE emails SET status = 'sent'")
""",
        "object-getattribute": """
def mutate(cursor):
    run = object.__getattribute__(cursor, "execute")
    run("UPDATE emails SET status = 'sent'")
""",
        "constant-folded-method": """
def mutate(cursor):
    run = getattr(cursor, "exe" + "cute")
    run("UPDATE emails SET status = 'sent'")
""",
        "dynamic-partial": """
import functools

def mutate(cursor, method):
    run = functools.partial(getattr(cursor, method))
    run("UPDATE emails SET status = 'sent'")
""",
        "dynamic-to-thread": """
import asyncio

async def mutate(cursor, method):
    await asyncio.to_thread(
        getattr(cursor, method),
        "UPDATE emails SET status = 'sent'",
    )
""",
    }
    missed = [
        label
        for label, source in cases.items()
        if not _find_email_mutations(
            source,
            filename=f"<aliased-reflection-{label}>",
        )
    ]

    assert missed == []


def test_detector_allows_dynamic_reflection_that_stays_data_only() -> None:
    source = """
def inspect(settings, snapshot, field, name, default, logger, level):
    value = getattr(settings, name, None)
    enabled = getattr(snapshot, field.name) is True
    fallback = getattr(settings, name, default)
    logger.setLevel(getattr(logger, level.upper(), 20))
    return value, enabled, fallback
"""

    assert _find_email_mutations(source, filename="<dynamic-data-reflection>") == []


def test_detector_resolves_reflection_names_from_static_tuple_registries() -> None:
    safe_source = """
REGISTRY = {
    "codex": ("providers.codex", "CodexChatModel"),
    "gemini": ("providers.gemini", "GeminiChatModel"),
}

def build(module, provider):
    module_path, class_name = REGISTRY[provider]
    cls = getattr(module, class_name)
    return cls()
"""
    dangerous_source = safe_source.replace("GeminiChatModel", "execute")

    assert _find_email_mutations(safe_source, filename="<static-registry-safe>") == []
    assert _find_email_mutations(
        dangerous_source,
        filename="<static-registry-dangerous>",
    )


def test_detector_rejects_escaped_dynamic_code_primitives() -> None:
    cases = {
        "exec-alias": "runner = exec",
        "eval-alias": "runner = eval",
        "compile-alias": "runner = compile",
        "globals": 'runner = globals()["exec"]',
        "locals": 'runner = locals()["eval"]',
        "builtins-subscript": 'runner = __builtins__["compile"]',
        "builtins-getattr": 'runner = getattr(__builtins__, "exec")',
    }
    missed: list[str] = []
    for label, binding in cases.items():
        source = f"""
def mutate(payload):
    {binding}
    runner(payload)
"""
        if not _find_email_mutations(
            source,
            filename=f"<escaped-dynamic-code-{label}>",
        ):
            missed.append(label)

    assert missed == []


def test_dynamic_execution_allowlist_requires_exact_project_relative_path() -> None:
    source = """
async def _apply_checkpoint_migrations(cur, migration):
    await cur.execute(migration)
"""

    assert _find_email_mutations(
        source,
        filename="/tmp/rogue/src/db/bootstrap.py",
    ) == [3]


def test_dynamic_execution_allowlist_freezes_query_shapes_and_counts() -> None:
    source = """
async def _ensure_checkpoint_index(cur, spec, migration, evil):
    await cur.execute(spec.drop_sql)
    await cur.execute(migration)
    await cur.execute(migration)
    await cur.execute(evil)
"""
    project_root = Path(__file__).resolve().parents[2]

    assert _find_email_mutations(
        source,
        filename=str(project_root / "src" / "db" / "bootstrap.py"),
    ) == [5, 6]


def test_dynamic_execution_allowlist_rejects_rebound_query_bindings() -> None:
    source = """
async def _ensure_checkpoint_index(cur, migration, evil):
    migration = evil
    await cur.execute(migration)
"""
    project_root = Path(__file__).resolve().parents[2]

    assert _find_email_mutations(
        source,
        filename=str(project_root / "src" / "db" / "bootstrap.py"),
    ) == [4]


def test_bootstrap_routine_acl_exceptions_do_not_admit_email_dml() -> None:
    source = """
async def _revoke_routine_access(cursor):
    await cursor.execute("UPDATE emails SET status = 'sent'")

async def _grant_routine_access(cursor):
    await cursor.execute("DELETE FROM emails")
"""
    project_root = Path(__file__).resolve().parents[2]

    assert _find_email_mutations(
        source,
        filename=str(project_root / "src" / "db" / "bootstrap.py"),
    ) == [3, 6]


def test_dynamic_non_sql_allowlist_freezes_server_constructor_shape_and_count() -> None:
    source = """
async def inject_test_email():
    type("MockState", (), {})()
    type("MockState", (), {})()
    factory()()
"""
    project_root = Path(__file__).resolve().parents[2]

    assert _find_email_mutations(
        source,
        filename=str(project_root / "src" / "server.py"),
    ) == [4, 5]


def test_detector_applies_comprehension_bindings_only_inside_the_expression() -> None:
    source = """
QUERY = "SELECT 1"

async def mutate(cursor, queries):
    return [await cursor.execute(QUERY) for QUERY in queries]
"""

    assert _find_email_mutations(source, filename="<comprehension-query>") == [5]


def test_detector_inspects_sql_executions_inside_lambda_bodies() -> None:
    source = """
def mutate(cursor):
    runner = lambda: cursor.execute("UPDATE emails SET status = 'sent'")
    runner()
"""

    assert _find_email_mutations(source, filename="<lambda-query>") == [3]


def test_email_mutations_are_owned_only_by_the_ingestion_repository() -> None:
    project_root = Path(__file__).resolve().parents[2]
    allowed = project_root / "src" / "ingestion" / "repository.py"
    candidates = list((project_root / "src").rglob("*.py"))
    scripts = project_root / "scripts"
    if scripts.is_dir():
        candidates.extend(scripts.rglob("*.py"))

    violations: list[str] = []
    for path in sorted(candidates):
        if path == allowed:
            continue
        source = path.read_text(encoding="utf-8")
        violations.extend(
            f"{path.relative_to(project_root)}:{line}"
            for line in _find_email_mutations(source, filename=str(path))
        )

    assert violations == [], (
        f"emails mutations are reserved for src/ingestion/repository.py: {violations}"
    )


def test_normalized_ast_dump_keeps_empty_fields_across_python_versions() -> None:
    normalized = _normalized_ast_dump(
        ast.parse("def reviewed():\n    return boundary()\n")
    )

    assert "type_ignores=[]" in normalized
    assert "args=[]" in normalized
    assert "keywords=[]" in normalized
    assert "decorator_list=[]" in normalized
    assert "type_params=[]" in normalized


def test_task5_repository_structural_ast_requires_explicit_review() -> None:
    project_root = Path(__file__).resolve().parents[2]
    assert _TASK7_TASK5_SUCCESSOR_PATHS == {
        "src/ingestion/models.py",
        "src/ingestion/repository.py",
    }
    assert _TASK7_TASK5_SUCCESSOR_PATHS <= set(_TASK7_REVIEWED_STRUCTURAL_AST_SHA256)
    historical_paths = (
        set(_TASK5_REPOSITORY_STRUCTURAL_AST_SHA256) - _TASK7_TASK5_SUCCESSOR_PATHS
    )
    actual = {
        relative: _normalized_file_ast_sha256(project_root / relative)
        for relative in historical_paths
    }
    expected = {
        relative: _TASK5_REPOSITORY_STRUCTURAL_AST_SHA256[relative]
        for relative in historical_paths
    }

    assert actual == expected, (
        "Task-5 repository structural review required before changing a path that "
        "has no explicit reviewed successor: "
        f"expected {expected}, got {actual}"
    )


def test_task7_reviewed_structural_ast_requires_explicit_review() -> None:
    project_root = Path(__file__).resolve().parents[2]
    assert _TASK8_TASK7_SUCCESSOR_PATHS == {"src/ingestion/repository.py"}
    assert _TASK8_TASK7_SUCCESSOR_PATHS <= set(_TASK7_REVIEWED_STRUCTURAL_AST_SHA256)
    assert _TASK8_TASK7_SUCCESSOR_PATHS <= set(_TASK8_REVIEWED_STRUCTURAL_AST_SHA256)
    assert _TASK10G_TASK7_SUCCESSOR_PATHS <= set(_TASK7_REVIEWED_STRUCTURAL_AST_SHA256)
    assert _TASK10G_TASK7_SUCCESSOR_PATHS <= set(
        _TASK10G_REVIEWED_STRUCTURAL_AST_SHA256
    )
    historical_paths = set(_TASK7_REVIEWED_STRUCTURAL_AST_SHA256) - (
        _TASK8_TASK7_SUCCESSOR_PATHS | _TASK10G_TASK7_SUCCESSOR_PATHS
    )
    actual = {
        relative: _normalized_file_ast_sha256(project_root / relative)
        for relative in historical_paths
    }
    expected = {
        relative: _TASK7_REVIEWED_STRUCTURAL_AST_SHA256[relative]
        for relative in historical_paths
    }

    assert actual == expected, (
        "Task-7 structural review required before changing a path that has no "
        f"explicit Task-8 reviewed successor: expected {expected}, got {actual}"
    )


def test_task8_reviewed_structural_ast_requires_explicit_review() -> None:
    project_root = Path(__file__).resolve().parents[2]
    assert _TASK9G_TASK8_SUCCESSOR_PATHS == {"src/ingestion/repository.py"}
    assert _TASK9G_TASK8_SUCCESSOR_PATHS <= set(_TASK8_REVIEWED_STRUCTURAL_AST_SHA256)
    assert _TASK10G_TASK8_SUCCESSOR_PATHS <= set(_TASK8_REVIEWED_STRUCTURAL_AST_SHA256)
    assert _TASK10G_TASK8_SUCCESSOR_PATHS <= set(
        _TASK10G_REVIEWED_STRUCTURAL_AST_SHA256
    )
    assert _PHASE4_LITE_TASK8_SUCCESSOR_PATHS <= set(
        _TASK8_REVIEWED_STRUCTURAL_AST_SHA256
    )
    assert _PHASE4_LITE_TASK8_SUCCESSOR_PATHS <= set(
        _PHASE4_LITE_REVIEWED_STRUCTURAL_AST_SHA256
    )
    historical_paths = set(_TASK8_REVIEWED_STRUCTURAL_AST_SHA256) - (
        _TASK9G_TASK8_SUCCESSOR_PATHS
        | _TASK10G_TASK8_SUCCESSOR_PATHS
        | _PHASE4_LITE_TASK8_SUCCESSOR_PATHS
    )
    actual = {
        relative: _normalized_file_ast_sha256(project_root / relative)
        for relative in historical_paths
    }
    expected = {
        relative: _TASK8_REVIEWED_STRUCTURAL_AST_SHA256[relative]
        for relative in historical_paths
    }

    assert actual == expected, (
        "Task-8 structural review required before updating its normalized AST "
        f"SHA-256 ratchet: expected {expected}, got {actual}"
    )


def test_task9g_reviewed_structural_ast_requires_explicit_review() -> None:
    project_root = Path(__file__).resolve().parents[2]
    assert _TASK9G_TASK8_SUCCESSOR_PATHS <= set(_TASK9G_REVIEWED_STRUCTURAL_AST_SHA256)
    assert _TASK9G_NON_SQL_SUCCESSOR_PATHS <= set(
        _TASK9G_REVIEWED_STRUCTURAL_AST_SHA256
    )
    assert _TASK10G_TASK9G_SUCCESSOR_PATHS <= set(
        _TASK9G_REVIEWED_STRUCTURAL_AST_SHA256
    )
    assert _TASK10G_TASK9G_SUCCESSOR_PATHS <= set(
        _TASK10G_REVIEWED_STRUCTURAL_AST_SHA256
    )
    historical_paths = (
        set(_TASK9G_REVIEWED_STRUCTURAL_AST_SHA256)
        - _TASK10G_TASK9G_SUCCESSOR_PATHS
        - _TASK11G_PREDECESSOR_PATHS
    )
    actual = {
        relative: _normalized_file_ast_sha256(project_root / relative)
        for relative in historical_paths
    }
    expected = {
        relative: _TASK9G_REVIEWED_STRUCTURAL_AST_SHA256[relative]
        for relative in historical_paths
    }

    assert actual == expected, (
        "Task-9G structural review required before changing a path that has no "
        f"explicit Task-10G reviewed successor: expected {expected}, got {actual}"
    )


def test_task10g_reviewed_structural_ast_requires_explicit_review() -> None:
    project_root = Path(__file__).resolve().parents[2]
    predecessor_paths = (
        _TASK10G_TASK7_SUCCESSOR_PATHS
        | _TASK10G_TASK8_SUCCESSOR_PATHS
        | _TASK10G_TASK9G_SUCCESSOR_PATHS
    )
    assert predecessor_paths == set(_TASK10G_REVIEWED_STRUCTURAL_AST_SHA256)
    assert _PHASE4_LITE_TASK10G_SUCCESSOR_PATHS <= set(
        _TASK10G_REVIEWED_STRUCTURAL_AST_SHA256
    )
    assert _PHASE4_LITE_TASK10G_SUCCESSOR_PATHS <= set(
        _PHASE4_LITE_REVIEWED_STRUCTURAL_AST_SHA256
    )
    historical_paths = set(_TASK10G_REVIEWED_STRUCTURAL_AST_SHA256) - (
        _TASK11G_PREDECESSOR_PATHS | _PHASE4_LITE_TASK10G_SUCCESSOR_PATHS
    )
    actual = {
        relative: _normalized_file_ast_sha256(project_root / relative)
        for relative in historical_paths
    }
    expected = {
        relative: _TASK10G_REVIEWED_STRUCTURAL_AST_SHA256[relative]
        for relative in historical_paths
    }

    assert actual == expected, (
        "Task-10G structural review required before updating its normalized AST "
        f"SHA-256 ratchet: expected {expected}, got {actual}"
    )


def test_task11g_reviewed_structural_ast_requires_explicit_review() -> None:
    project_root = Path(__file__).resolve().parents[2]
    predecessor_paths = set(_TASK9G_REVIEWED_STRUCTURAL_AST_SHA256) | set(
        _TASK10G_REVIEWED_STRUCTURAL_AST_SHA256
    )
    assert _TASK11G_PREDECESSOR_PATHS <= predecessor_paths
    assert _TASK11G_PREDECESSOR_PATHS == set(_TASK11G_REVIEWED_STRUCTURAL_AST_SHA256)
    assert _PHASE4_LITE_TASK11G_SUCCESSOR_PATHS <= set(
        _TASK11G_REVIEWED_STRUCTURAL_AST_SHA256
    )
    assert _PHASE4_LITE_TASK11G_SUCCESSOR_PATHS <= set(
        _PHASE4_LITE_REVIEWED_STRUCTURAL_AST_SHA256
    )
    historical_paths = (
        set(_TASK11G_REVIEWED_STRUCTURAL_AST_SHA256)
        - _PHASE4_LITE_TASK11G_SUCCESSOR_PATHS
    )
    actual = {
        relative: _normalized_file_ast_sha256(project_root / relative)
        for relative in historical_paths
    }
    expected = {
        relative: _TASK11G_REVIEWED_STRUCTURAL_AST_SHA256[relative]
        for relative in historical_paths
    }

    assert actual == expected, (
        "Task-11G structural review required before updating its normalized AST "
        f"SHA-256 ratchet: expected {expected}, got {actual}"
    )


def test_phase4_lite_reviewed_structural_ast_requires_explicit_review() -> None:
    project_root = Path(__file__).resolve().parents[2]
    predecessor_successors = (
        _PHASE4_LITE_TASK8_SUCCESSOR_PATHS
        | _PHASE4_LITE_TASK10G_SUCCESSOR_PATHS
        | _PHASE4_LITE_TASK11G_SUCCESSOR_PATHS
    )
    assert predecessor_successors <= set(
        _PHASE4_LITE_REVIEWED_STRUCTURAL_AST_SHA256
    )
    actual = {
        relative: _normalized_file_ast_sha256(project_root / relative)
        for relative in _PHASE4_LITE_REVIEWED_STRUCTURAL_AST_SHA256
    }

    assert actual == _PHASE4_LITE_REVIEWED_STRUCTURAL_AST_SHA256, (
        "Phase-4-Lite structural review required before updating its normalized "
        f"AST SHA-256 ratchet: expected "
        f"{_PHASE4_LITE_REVIEWED_STRUCTURAL_AST_SHA256}, got {actual}"
    )


def test_trusted_dynamic_sql_files_require_explicit_structural_review() -> None:
    project_root = Path(__file__).resolve().parents[2]
    actual = {
        relative: _normalized_file_ast_sha256(project_root / relative)
        for relative in _TRUSTED_DYNAMIC_SQL_FILE_STRUCTURAL_AST_SHA256
    }

    assert actual == _TRUSTED_DYNAMIC_SQL_FILE_STRUCTURAL_AST_SHA256, (
        "Trusted dynamic SQL structural review required before updating the "
        f"approved normalized AST SHA-256: expected "
        f"{_TRUSTED_DYNAMIC_SQL_FILE_STRUCTURAL_AST_SHA256}, got {actual}"
    )


def test_policy_exception_paths_equal_structural_ratchet_paths() -> None:
    dynamic_policy_paths = {
        path for path, _, _ in _TRUSTED_DYNAMIC_SQL_EXECUTION_SHAPES
    }
    assert dynamic_policy_paths == set(_TRUSTED_DYNAMIC_SQL_FILE_STRUCTURAL_AST_SHA256)

    non_sql_policy_paths = {path for path, _, _ in _NON_SQL_EXECUTION_CALLS} | {
        path for path, _ in _TRUSTED_DYNAMIC_NON_SQL_CALL_SHAPES
    }
    assert non_sql_policy_paths == set(_NON_SQL_EXCEPTION_FILE_STRUCTURAL_AST_SHA256)


def test_non_sql_exception_files_require_explicit_structural_review() -> None:
    project_root = Path(__file__).resolve().parents[2]
    assert _TASK9G_NON_SQL_SUCCESSOR_PATHS == {"src/server.py"}
    assert _TASK9G_NON_SQL_SUCCESSOR_PATHS <= set(
        _NON_SQL_EXCEPTION_FILE_STRUCTURAL_AST_SHA256
    )
    historical_paths = (
        set(_NON_SQL_EXCEPTION_FILE_STRUCTURAL_AST_SHA256)
        - _TASK9G_NON_SQL_SUCCESSOR_PATHS
    )
    actual = {
        relative: _normalized_file_ast_sha256(project_root / relative)
        for relative in historical_paths
    }
    expected = {
        relative: _NON_SQL_EXCEPTION_FILE_STRUCTURAL_AST_SHA256[relative]
        for relative in historical_paths
    }

    assert actual == expected, (
        "Non-SQL exception structural review required before updating the "
        f"approved normalized AST SHA-256: expected {expected}, got {actual}"
    )
