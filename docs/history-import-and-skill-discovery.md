# 历史邮件导入 & Skill 自动发现

本文档包含两个独立工具的使用说明：

1. **`import_pst.py`** — 将历史邮件（PST / Mbox / EML）或 Exchange 服务器当前邮件导入到 Qdrant 向量数据库
2. **`discover_skills.py`** — 分析历史邮件，自动发现处理模式并生成 Skill

---

## 环境要求

### 推荐方式：使用 uv（零配置）

安装 [uv](https://docs.astral.sh/uv/)（如尚未安装）：

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

之后所有命令用 `uv run` 替代 `python`，**不需要手动创建虚拟环境、不需要 pip install、不需要关心 Python 版本**：

```bash
# uv 会自动：
#   1. 找到或下载合适的 Python (>=3.11)
#   2. 创建隔离环境并安装脚本声明的依赖
#   3. 运行脚本
uv run scripts/import_pst.py archive.pst --dry-run
```

> **注意**：PST 格式的解析依赖 `libpff-python`，该包需要 C 编译器。
> 如果 `uv run` 报编译错误，安装一下系统开发包：
>
> ```bash
> # Ubuntu / Debian
> sudo apt install python3-dev build-essential
>
> # macOS
> xcode-select --install
> ```
>
> Mbox 和 EML 格式不需要任何额外依赖，开箱即用。

### 备选方式：使用项目虚拟环境

如果你已经搭建了项目的开发环境（`.venv/`），也可以直接用：

```bash
source .venv/bin/activate

# 确保安装了 PST 解析库
pip install libpff-python

python scripts/import_pst.py archive.pst --dry-run
```

---

## 工具一：历史邮件导入 (`import_pst.py`)

### 功能

将本地的历史邮件文件批量解析，写入 Qdrant 向量数据库，供 RAG 检索和 Skill 发现使用。

### 支持的格式

| 格式 | 输入 | 依赖 | 说明 |
|------|------|------|------|
| **PST** | `archive.pst` | `libpff-python`（pip 自动安装） | Outlook 数据文件，直接用 Python 读取 |
| **Mbox** | `mail.mbox` | 无（Python 标准库） | Linux/Thunderbird 常见格式 |
| **EML** | `email.eml` | 无（Python 标准库） | 单封邮件文件 |
| **EML 目录** | `./emails/` | 无（Python 标准库） | 递归扫描所有 `.eml` 文件 |
| **Exchange** | `--source exchange` | 无（使用项目已有 API 客户端） | 从 Exchange 服务器直接拉取当前邮件 |

### 基本用法

#### 第一步：预览（推荐先执行）

预览模式不写入任何数据，只显示解析结果：

```bash
uv run scripts/import_pst.py archive.pst --dry-run
```

输出示例：

```
📧 邮件导入工具
   来源: PST 文件: archive.pst
   模式: 预览 (DRY RUN)

  📥 [Inbox] Q4 发票审批 - 请尽快处理              (finance@corp.com)
  📥 [Inbox] 季度汇报 - 请准备材料                (boss@corp.com)
  📤 [Sent Items] Re: Q4 发票审批                 (me@corp.com)
  ...

导入结果汇总:
  扫描邮件数:  1,234
  成功导入:    1,230
  跳过 (空):   4
```

- 📥 = 收到的邮件（Inbox 等收件文件夹）
- 📤 = 已发送的邮件（Sent Items 等发件文件夹）

脚本会根据文件夹名称自动识别邮件类型。

#### 第二步：正式导入

确认预览无误后，去掉 `--dry-run` 执行正式导入：

```bash
uv run scripts/import_pst.py archive.pst
```

> **前提**：`.env` 中需要配置 Qdrant 和 Embedding 服务地址。
> 正式导入会调用项目的 `EmailProcessor`，因此需要在项目目录下执行，
> 且项目依赖需要可用（使用项目 `.venv/` 或通过 `uv run` 补充 `--with`）。

#### 大文件优化

PST 文件可能包含数万封邮件。可以调大批次减少 Qdrant 写入次数：

```bash
uv run scripts/import_pst.py archive.pst --batch-size 100
```

### 完整参数

```text
用法: import_pst.py [-h] [--source {file,exchange}] [--folder FOLDER]
                     [--limit N] [--all-mail] [--batch-size N] [--dry-run]
                     [SOURCE]

位置参数:
  SOURCE              PST/Mbox/EML 文件路径，或 EML 目录路径 (--source file 时必填)

可选参数:
  --source {file,exchange}  数据来源: file=本地文件 (默认), exchange=Exchange 服务器
  --folder FOLDER     Exchange 文件夹: ALL=全部邮件文件夹 (默认), 或指定如 inbox/sent/drafts
  --limit N           从 Exchange 拉取的最大邮件数 (默认: 0=全部)
  --all-mail          拉取全部邮件（含已读），默认只拉未读
  --batch-size N      每批次处理的邮件数 (默认: 50)
  --dry-run           仅预览，不写入 Qdrant
```

### 导入的数据结构

每封邮件在 Qdrant 中存储以下字段：

| 字段 | 说明 |
|------|------|
| `id` | 唯一 ID（本地文件为 `pst_` + 哈希；Exchange 为 `exc_` + Base64） |
| `subject` | 邮件主题 |
| `sender` | 发件人 |
| `to` / `cc` | 收件人 / 抄送人 |
| `body` | 邮件正文（优先 HTML，回退纯文本） |
| `received_at` | 收信时间（ISO 格式） |
| `source_folder` | 来源文件夹名（如 Inbox、Sent Items） |
| `type` | 邮件类型：`received` / `sent` / `draft` |
| `in_reply_to` | 回复的原始邮件 ID（用于线程追踪） |
| `thread_id` | 会话 ID（从 In-Reply-To / References 推断） |
| `_import_source` | 用于区分数据来源：`pst_import`（本地文件）或 `exchange_import`（服务器） |

### PST 解析策略

脚本内置两种解析器，运行时自动选择：

| 优先级 | 解析器 | 安装方式 | 说明 |
|--------|--------|----------|------|
| **1** | `pypff` | `pip install libpff-python` | 纯 Python，直接读取 PST 内部结构 |
| **2** | `readpst` | Ubuntu: `sudo apt install pst-utils`<br>macOS: `brew install libpst` | 系统命令行工具，仅在 pypff 不可用时使用 |

使用 `uv run` 时会自动安装 `libpff-python`，无需手动操作。

---

## 工具二：Skill 自动发现 (`discover_skills.py`)

### 功能

分析历史邮件，从中挖掘出可自动化的处理模式（谁发的、发给谁、内容是什么、是否回复过），以可视化链路图的形式展示给用户确认，然后自动生成 Skill 文件。

### 数据来源

| 模式 | 参数 | 说明 |
|------|------|------|
| **Qdrant** | `--source qdrant`（默认） | 从已导入 Qdrant 的邮件中分析 |
| **EML 目录** | `--source eml --pst-path DIR` | 直接分析 EML 文件夹，不需要先导入 |
| **PST 文件** | `--source pst --pst-path FILE` | 直接分析 PST 文件，不需要先导入 |

### 快速开始

最简单的用法——直接分析一个 EML 目录，不需要 LLM：

```bash
uv run scripts/discover_skills.py \
  --source eml \
  --pst-path /path/to/emails/ \
  --no-llm
```

### 完整工作流（推荐）

```bash
# 1. 先导入历史邮件到 Qdrant
uv run scripts/import_pst.py archive.pst

# 2. 基于 Qdrant 数据进行分析（使用 LLM 深度挖掘）
uv run scripts/discover_skills.py

# 3. 按提示选择模式编号 → 自动生成 Skill

# 4. 重启服务，新 Skill 立即生效
python -m src.main
```

### 交互流程详解

运行后脚本会依次经过以下阶段：

**阶段 1：数据收集**

```
⏳ 正在收集邮件数据...
   收集完成: 200 封收件, 85 封已发送
```

**阶段 2：模式分析**

脚本统计发件人频率、回复率、主题关键词等维度，然后：
- 有 LLM → 调用 LLM 深度分析，发现更细粒度的组合模式
- `--no-llm` → 使用启发式算法（纯统计，不需要 API）

**阶段 3：链路图展示**

每个发现的模式用 ASCII 链路图展示：

```
━━━ Pattern #1: 财务邮件自动处理 ━━━━━━━━━━━━━━━━

  触发链路:
    ┌─────────────────────────────────────┐
    │ [发件人] 属于: finance@corp.com      │
    └──────────────┬──────────────────────┘
                   ↓
    ┌─────────────────────────────────────┐
    │ [主题含] 正则: 发票|报销|费用        │
    └──────────────┬──────────────────────┘
                   ↓
    ┌─────────────────────────────────────┐
    │ ✅ 回复率: 95% (19 封)              │
    │ 🔴 优先级: P1                       │
    │ 📝 需要回复: 是                      │
    │ 💼 语气: 专业正式                    │
    └─────────────────────────────────────┘
```

**阶段 4：多选确认**

```
  [1] 财务邮件处理     ✅ 回复率=95% (19封)  置信度=★★★★☆
  [2] VIP 领导邮件     ✅ 回复率=100% (12封) 置信度=★★★★★
  [3] 系统通知         ❌ 回复率=5% (30封)   置信度=★★★☆☆

  > 请输入选择: 1,2
```

- 输入编号，逗号分隔（如 `1,2,5`）
- 输入 `all` 全选
- 输入 `q` 退出

**阶段 5：生成 Skill**

```
  ✅ 财务邮件处理 → skills_registry/skill_auto_finance
  ✅ VIP 领导邮件 → skills_registry/skill_auto_boss

  🎉 成功生成 2 个 Skill!
     位置: skills_registry/
     重启服务后自动加载。
```

### 生成的 Skill 结构

每个 Skill 是一个目录，包含两个文件：

```
skills_registry/skill_auto_finance/
├── manifest.yaml    # 触发规则（发件人、主题正则等）
└── handler.py       # 处理逻辑（修改优先级、设置回复标记等）
```

生成的文件格式与现有手写 Skill 完全一致，兼容 `SkillManager` 自动加载。

### 完整参数

```
用法: discover_skills.py [-h]
                         [--source {qdrant,pst,eml}]
                         [--pst-path PATH]
                         [--no-llm]
                         [--limit N]
                         [--auto-confirm]

可选参数:
  --source {qdrant,pst,eml}   数据来源 (默认: qdrant)
  --pst-path PATH             PST 文件或 EML 目录路径
  --no-llm                    不使用 LLM，纯启发式分析
  --limit N                   最大分析邮件数 (默认: 5000)
  --auto-confirm              跳过交互选择，自动确认全部模式
```

### 分析模式对比

| 模式 | 需要 LLM | 需要 Qdrant | 分析质量 | 速度 |
|------|----------|-------------|----------|------|
| `--source qdrant` | 可选 | ✅ | 最佳（基于全量数据） | 中等 |
| `--source eml --no-llm` | ❌ | ❌ | 良好（纯统计） | 快 |
| `--source eml` | ✅ | ❌ | 较好（LLM + 统计） | 较慢 |
| `--source pst --no-llm` | ❌ | ❌ | 良好（纯统计） | 快 |

---

## 常见场景

### 场景一：「我只想看看 PST 里有什么」

```bash
uv run scripts/import_pst.py archive.pst --dry-run
```

零配置，零依赖安装（uv 自动处理），立即看到所有邮件列表。

### 场景二：「我想快速发现一些可以自动化的规则」

```bash
# 直接分析 PST，不需要 Qdrant 也不需要 LLM
uv run scripts/discover_skills.py \
  --source pst \
  --pst-path archive.pst \
  --no-llm
```

### 场景三：「我要完整走一遍流程」

```bash
# 1. 导入到 Qdrant（需要 Qdrant + Embedding 服务）
source .venv/bin/activate
python scripts/import_pst.py archive.pst

# 2. 用 LLM 深度分析（需要 LLM API）
python scripts/discover_skills.py

# 3. 选择模式 → 生成 Skill → 重启服务
python -m src.main
```

### 场景四：「我的邮件不是 PST 格式」

**Outlook 导出为 EML：**
Outlook → 文件 → 另存为 → 选择文件夹 → 保存为 .eml

**Thunderbird 导出为 Mbox：**
Thunderbird 的邮件已经以 mbox 格式存储在本地，通常在：
- Linux: `~/.thunderbird/<profile>/Mail/`
- macOS: `~/Library/Thunderbird/Profiles/<profile>/Mail/`
- Windows: `%APPDATA%\Thunderbird\Profiles\<profile>\Mail\`

直接指向对应目录或 mbox 文件即可：

```bash
uv run scripts/import_pst.py ~/.thunderbird/xxx/Mail/Local\ Folders/ --dry-run
```

### 场景五：「我想导入 Exchange 服务器上的当前邮件」

```bash
# 预览服务器上所有文件夹的未读邮件
uv run scripts/import_pst.py --source exchange --dry-run

# 拉取全部文件夹的全部邮件（含已读），自动分页获取
uv run scripts/import_pst.py --source exchange --all-mail

# 只拉取指定文件夹的邮件（如收件箱，限制获取前 50 封做测试）
uv run scripts/import_pst.py --source exchange --folder inbox --limit 50 --dry-run

# 只拉取已发送邮件
uv run scripts/import_pst.py --source exchange --folder sent --all-mail --dry-run
```

> **前提**：`.env` 中需要配置好 `EXCHANGE_API_URL`、`EXCHANGE_API_KEY`、`EXCHANGE_ACCOUNT_ID`。
> Exchange 导入模式会自动通过 API 获取服务器全部有邮件的文件夹，过滤掉日历、联系人等系统内部目录，并采用分页防超时机制直接同步大批量邮件。

---

## FAQ

**Q: `libpff-python` 安装失败怎么办？**

这个包需要 C 编译器。安装系统开发工具链后重试：

```bash
# Ubuntu/Debian
sudo apt install python3-dev build-essential

# macOS
xcode-select --install

# 另外，如果 C 依赖报错，macOS 用户还可以直接安装 libpst，提供 readpst 作为纯命令行后备方案：
# brew install libpst

# 如果以上都不行，可以先把 PST 在 Outlook 里导出为 EML/Mbox 格式，
# 这两种格式不需要任何额外依赖。
```

**Q: `--dry-run` 和正式导入有什么区别？**

`--dry-run` 只做解析和预览，不连接 Qdrant，不需要 `.env` 配置。
正式导入需要 Qdrant 和 Embedding 服务可用。

**Q: 导入后数据在哪？**

在 Qdrant 的 `emails` 集合中。可以通过 Qdrant Dashboard (http://localhost:6333/dashboard) 查看。

**Q: 生成的 Skill 怎么修改？**

直接编辑 `skills_registry/skill_auto_xxx/manifest.yaml` 和 `handler.py`。
格式参考现有的手写 Skill（如 `skill_vip_handling`）。

**Q: 可以重新运行发现吗？**

可以。重复运行会覆盖同名 Skill 目录。建议先删除不需要的自动生成目录后再重新发现。
