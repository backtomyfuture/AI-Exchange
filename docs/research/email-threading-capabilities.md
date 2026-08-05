# Exchange、Gmail、IMAP/163 邮件线程与正文增量能力

> 研究日期：2026-07-30
> 范围：只核对 Microsoft/EWS、exchangelib、Google/Gmail、IETF RFC、网易官方帮助及网易 IMAP 服务器的只读协议响应。

## 结论

需要对当前 Exchange 上游做一个很小但有价值的改动：把 exchangelib 已经能够取得、但当前 Gateway 响应没有透传的会话和邮件头字段序列化出来。建议至少返回：

- `conversation_id`
- `conversation_index`
- `internet_message_id`
- `in_reply_to`
- `references`
- `unique_body`
- 原始完整 `body`

其中最重要的新发现是：EWS 原生提供 `UniqueBody`，Microsoft 将它定义为“该会话中本项独有的 HTML 片段或纯文本”；exchangelib 已将其暴露为只读的 `Item.unique_body`。因此，在 Exchange 链路中，`unique_body` 应优先作为“本轮新增正文”，本地正文分界解析作为缺失、异常和跨邮箱兜底。[Microsoft `UniqueBody`](https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/uniquebody)，[exchangelib `Item.unique_body`](https://github.com/ecederstrand/exchangelib/blob/ffffa98bff028cf93a2beeb64d89db1cd005c4ab/exchangelib/items/item.py#L112-L115)

但不能只依赖服务端线程字段：

1. 线程字段回答“这些邮件属于哪个会话”。
2. `UniqueBody` 或正文解析回答“这一封相较于历史新增了什么”。
3. 转发邮件可能是一个全新的 RFC 邮件，原邮件只是嵌在正文中；RFC 5322 明确区分这种“把被转发邮件放进新邮件正文”的转发方式。因此，即使有线程字段，正文分界解析仍然必要。[RFC 5322 §3.6.6](https://www.rfc-editor.org/rfc/rfc5322.html#section-3.6.6)

推荐最终采用三层策略：

1. **服务商线程标识**：Exchange `ConversationId`；Gmail `threadId` / IMAP `X-GM-THRID`。
2. **跨服务商标准关系**：RFC `Message-ID`、`In-Reply-To`、`References`。
3. **正文增量**：Exchange `UniqueBody` 优先；否则解析 HTML/纯文本引用边界。

## 1. Exchange / exchangelib

### 1.1 可直接取得的字段

| 统一字段 | exchangelib 属性 | EWS 字段 | EWS / Python 值类型 | 语义 |
|---|---|---|---|---|
| `conversation_id` | `Item.conversation_id` | `item:ConversationId` | EWS `ItemIdType`；exchangelib `ConversationId` 对象，实际标识在 `.id: str` | Exchange 计算的会话标识，Exchange 2010+ |
| `conversation_index` | `Message.conversation_index` | `message:ConversationIndex` | EWS `base64Binary`；exchangelib 解码为 `bytes` | 邮件在会话中的位置/层级信息，不应替代 `conversation_id` |
| `internet_message_id` | `Message.message_id` | `message:InternetMessageId` | `str` | RFC `Message-ID` |
| `in_reply_to` | `Item.in_reply_to` | `item:InReplyTo` | `str` | 当前项回复的父邮件标识 |
| `references` | `Message.references` | `message:References` | `str`，通常包含一个或多个 `msg-id` | 回复链祖先标识 |
| `unique_body` | `Item.unique_body` | `item:UniqueBody` | `Body` / `HTMLBody`，可按字符串序列化 | 本项相对于会话的独有正文 |

exchangelib 的字段声明可在官方源码中直接核对：

- `conversation_id`、`in_reply_to`、`unique_body`：[exchangelib `item.py`](https://github.com/ecederstrand/exchangelib/blob/ffffa98bff028cf93a2beeb64d89db1cd005c4ab/exchangelib/items/item.py#L70-L115)
- `conversation_index`、`message_id`、`references`：[exchangelib `message.py`](https://github.com/ecederstrand/exchangelib/blob/ffffa98bff028cf93a2beeb64d89db1cd005c4ab/exchangelib/items/message.py#L55-L65)
- `conversation_index` 使用的 `Base64Field` 在 Python 中以 `bytes` 表示：[exchangelib `Base64Field`](https://github.com/ecederstrand/exchangelib/blob/ffffa98bff028cf93a2beeb64d89db1cd005c4ab/exchangelib/fields.py#L900-L908)

Microsoft 的 EWS 契约也明确提供这些元素：

- `ConversationId` 是带 `Id`、可选 `ChangeKey` 的 `ItemIdType`：[Microsoft `ConversationId`](https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/conversationid)
- `ConversationIndex` 是 Base64 二进制值：[Microsoft `ConversationIndex`](https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/conversationindex)
- `InternetMessageId` 是字符串：[Microsoft `InternetMessageId`](https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/internetmessageid)
- `InReplyTo` 是字符串：[Microsoft `InReplyTo`](https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/inreplyto)
- `References` 是用于关联回复和原邮件的字符串头：[Microsoft `References`](https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/references)

Microsoft 说明 Exchange 根据线程首封邮件的 `Message-ID` 定义 conversation，相关回复通过 `References` 和 `In-Reply-To` 引用原邮件；`ConversationIndex` 表示邮件在会话中的位置。[Microsoft：使用 EWS 处理会话](https://learn.microsoft.com/en-us/exchange/client-developer/exchange-web-services/how-to-work-with-conversations-by-using-ews-in-exchange)

### 1.2 上游具体需要改什么

exchangelib 的 `sync_items()` 已支持通过 `only_fields` 指定同步返回字段；如果不传 `only_fields`，它会请求该文件夹允许的全部项目字段。也就是说，能力已经在库里，当前缺口通常只是上游的字段白名单或 JSON 序列化器没有透传。[exchangelib `sync_items()`](https://github.com/ecederstrand/exchangelib/blob/ffffa98bff028cf93a2beeb64d89db1cd005c4ab/exchangelib/folders/collections.py#L293-L325)

建议上游：

1. 在 `sync_items` / `GetItem` 的字段集合中显式加入上表字段。
2. JSON 序列化时：
   - `conversation_id = item.conversation_id.id`，不要直接序列化对象的展示字符串；
   - `conversation_index` 重新编码成 Base64 字符串，避免把 Python `bytes` 直接塞进 JSON；
   - `message_id` 映射为统一字段 `internet_message_id`；
   - `unique_body` 和完整 `body` 同时保留。
3. 保留缺失值为 `null`，不要用主题拼接伪造会话 ID。

不需要为了拿到这些字段改成 `FindConversation` 或 `GetConversationItems`：继续使用现有 `sync_items(sync_state=...)` 即可，只需让同步结果携带并序列化这些字段。

## 2. Gmail

Gmail API 有原生服务端线程：

- 每个 `Message` 都有字符串 `threadId`；
- `Thread` 资源包含该会话的 `messages[]`；
- `threads.get` 可以一次取得会话中的全部邮件。[Gmail `Message` 资源](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages)，[Gmail `Thread` 资源](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.threads)

Google 明确规定，应用在发送或导入邮件并要求加入指定线程时，需要同时满足：

1. 指定目标 `threadId`；
2. RFC 合规地设置 `References` 和 `In-Reply-To`；
3. `Subject` 匹配。[Gmail：管理线程](https://developers.google.com/workspace/gmail/api/guides/threads)

Gmail API 的 `payload.headers[]` 可以读取顶层 RFC 邮件头；使用 `format=RAW` 时也可取得完整 RFC 2822 邮件。因此应同时保存 Gmail `threadId` 和 `Message-ID` / `In-Reply-To` / `References`，不要把 Gmail 专有 ID 当作跨服务商 ID。[Gmail `MessagePart.headers`](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages#MessagePart)

如果未来通过 IMAP 而不是 Gmail REST API 接入，Gmail 还公开了专有扩展：

- `X-GM-MSGID`：64 位无符号消息 ID；
- `X-GM-THRID`：64 位无符号线程 ID，与 Gmail API / Web 界面的十六进制 ID 对应；
- 通过 `FETCH` 取得，通过 `SEARCH` / `UID SEARCH` 查询。[Gmail IMAP 扩展](https://developers.google.com/workspace/gmail/imap/imap-extensions)

Gmail API 没有与 EWS `UniqueBody` 等价的、公开承诺的“只返回本轮新增正文”字段。因此 Gmail 仍需保留完整 MIME 正文并做本地引用边界解析。

## 3. 标准 IMAP 与 RFC 邮件头

### 3.1 标准可移植的关系字段

RFC 5322 规定：

- 每封邮件应该有唯一的 `Message-ID`；
- 回复邮件应该适当地带 `In-Reply-To` 和 `References`；
- `In-Reply-To` 指向父邮件；
- `References` 可以表达整条 conversation thread；
- 三者的值由一个或多个 `<id-left@id-right>` 形式的 `msg-id` 组成。[RFC 5322 §3.6.4](https://www.rfc-editor.org/rfc/rfc5322.html#section-3.6.4)

标准 IMAP 可以通过 `BODY.PEEK[HEADER.FIELDS (...)]` 只读取得指定 RFC 头而不把邮件标为已读，例如：

```text
UID FETCH <uid> (
  BODY.PEEK[HEADER.FIELDS (MESSAGE-ID IN-REPLY-TO REFERENCES SUBJECT)]
)
```

`HEADER.FIELDS` 的标准行为见 [RFC 9051 §6.4.5.1](https://www.rfc-editor.org/rfc/rfc9051.html#section-6.4.5.1)。

### 3.2 可选的服务端线程扩展

标准 IMAP 基础协议不保证服务端线程 ID，但有两个可选扩展：

1. RFC 5256 `THREAD`：支持服务器会在 `CAPABILITY` 中公布 `THREAD=<算法>`。`THREAD=REFERENCES` 按 `References`、`In-Reply-To` 和主题建立父子树；`THREAD=ORDEREDSUBJECT` 主要按标准化主题分组，精度较低。[RFC 5256](https://www.rfc-editor.org/rfc/rfc5256.html)
2. RFC 8474 `OBJECTID`：支持服务器会公布 `OBJECTID`，并允许 `FETCH THREADID`；但 `THREADID` 本身仍是可选的，服务器无法计算时可以返回 `NIL`。[RFC 8474](https://www.rfc-editor.org/rfc/rfc8474.html)

所以通用 IMAP 适配器应在认证后检查 `CAPABILITY`，按能力使用 `THREAD` / `THREADID`，不能假设所有邮箱都有。

## 4. 网易 163 邮箱

网易官方共享帮助中心确认 163/126/yeah 邮箱支持通过 IMAP 客户端接入。[网易官方：安卓客户端添加网易邮箱](https://help.mail.yeah.net/faqDetail.do?code=d7a5dc8471cd0c0e8b4b8f4f8e49998b374173cfe9171305fa1ce630d7f67ac2f7273b721cc829cb)

2026-07-30 对 `imap.163.com:993` 做了未登录、只读的 `CAPABILITY` 探测：

```text
* CAPABILITY IMAP4rev1 XLIST SPECIAL-USE ID LITERAL+ STARTTLS
  APPENDLIMIT=71680000 XAPPLEPUSHSERVICE UIDPLUS X-CM-EXT-1
  SASL-IR AUTH=XOAUTH2
```

当前未认证响应中没有：

- `THREAD=...`
- `OBJECTID`
- 类似 Gmail `X-GM-THRID` 的公开线程字段

按照 RFC 5256 和 RFC 8474 的能力声明规则，这意味着目前不能把 163 的公开 IMAP 接口视为提供标准服务端线程能力。[RFC 5256 的 `THREAD=` 能力声明](https://www.rfc-editor.org/rfc/rfc5256.html#section-1)，[RFC 8474 的 `OBJECTID` 能力声明](https://www.rfc-editor.org/rfc/rfc8474.html#section-3)

限制：IMAP 服务器可以在认证后改变能力列表，所以正式接入 163 时还应使用真实测试账号在认证后再探测一次。即使认证后仍无 `THREAD` / `OBJECTID`，163 的 IMAP4rev1 仍可读取标准 `Message-ID`、`In-Reply-To`、`References`，足够由应用自行构建关系图；缺头的转发邮件再由正文解析补足。

`X-CM-EXT-1` 是 Coremail/网易专有能力，但没有找到网易面向开发者公开的线程语义契约，不应把它当作稳定线程 API。

## 5. 正文分界解析是否可靠

它是必要且实用的兜底，但不是跨服务商标准。

RFC 5322 明确说邮件 body 对该标准而言只是“不解释的一系列文本行”，并没有规定回复客户端必须使用哪一种引用分隔格式。[RFC 5322 §3.5](https://www.rfc-editor.org/rfc/rfc5322.html#section-3.5)

因此，“本次回复内容之后出现一组发件人、发送时间、收件人、抄送、主题”确实是很强的天然分界线，但应按**字段簇**识别，而不是看到单个“发件人”或 `From:` 就截断。建议：

1. 优先采用结构化边界：
   - EWS `UniqueBody`；
   - HTML 引用容器或明确的转发/原始邮件分隔符。
2. 对纯文本或被清洗过的 HTML，识别一个短窗口内至少三个头字段：
   - 中文：`发件人`、`发送时间`、`收件人`、`抄送`、`主题`；
   - 英文：`From`、`Sent` / `Date`、`To`、`Cc`、`Subject`。
3. 边界前为 `current_text`，边界后为 `quoted_history`；两者都保存，分类和规则路由只使用 `current_text`，历史仅作为上下文。
4. 完整原文仍应保留在内容存储中，便于回溯和改进解析器。

这条策略可以覆盖：

- Exchange：`UniqueBody` 优先，正文解析兜底；
- Gmail：`threadId` 负责会话归属，正文解析负责本轮增量；
- 163/其他 IMAP：RFC 头负责可用时的关系图，正文解析覆盖转发和缺失邮件头。

## 6. 推荐的统一接入契约

建议各上游最终输出同一套 provider-neutral 字段：

```json
{
  "provider": "exchange | gmail | imap",
  "provider_message_id": "provider-scoped opaque id",
  "provider_thread_id": "provider-scoped opaque thread id or null",
  "provider_thread_index": "provider-specific position or null",
  "internet_message_id": "<globally-unique@example.com>",
  "in_reply_to": "<parent@example.com>",
  "references": ["<root@example.com>", "<parent@example.com>"],
  "body": "完整原始正文",
  "unique_body": "服务端提供的本轮正文或 null"
}
```

消费端的优先级：

1. `current_text = unique_body`，若无则运行正文投影解析器；
2. provider 内优先用 `provider_thread_id` 聚合；
3. 跨 provider 或缺线程 ID 时，用 `References` / `In-Reply-To` / `Message-ID` 构图；
4. 仅在上述标识全部缺失时，才把标准化主题、参与人和时间邻近度作为低置信度候选，不直接生成确定线程关系。
