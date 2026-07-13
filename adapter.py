"""
微织 Weave 平台适配器 — Hermes Agent 插件

通过反向 WebSocket 连接 weaveai.chat，实现：
- 流式消息（content_delta 逐字输出）
- 工具调用生命周期事件转发
- 会话管理（Hermes 为 session_id 事实来源）
- 斜杠命令（/new、/title、/model 等）
- 附件支持（base64 内嵌）
- 断线自动重连（指数退避）
- TUI 还原事件（tool.call.started/completed 等）

安装：
  hermes plugins install <path-to-weave-platform>
  hermes plugins enable weave-platform

配置（环境变量或 config.yaml）：
  WEAVE_WS_URL  — Weave WebSocket 地址
  WEAVE_WS_ID   — Agent 连接标识符
  WEAVE_API_KEY — API 密钥（可选）
"""

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Hermes 核心导入（延迟加载，插件发现阶段不报错） ──────────────────

from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
)
from gateway.config import Platform, PlatformConfig
from gateway.session import SessionSource

# ── 依赖检查 ──────────────────────────────────────────────────────

try:
    import websockets
    from websockets.exceptions import ConnectionClosed, WebSocketException
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False
    websockets = None  # type: ignore


def check_requirements() -> bool:
    """检查 websockets 库是否可用"""
    return HAS_WEBSOCKETS


# ── 环境变量启用钩子 ──────────────────────────────────────────────

def _env_enablement() -> Optional[dict]:
    """从环境变量读取配置，在适配器构造前填充 PlatformConfig.extra"""
    ws_url = os.getenv("WEAVE_WS_URL")
    ws_id = os.getenv("WEAVE_WS_ID") or os.getenv("WEAVE_AGENT_ID")
    if not ws_url or not ws_id:
        return None

    extra: Dict[str, Any] = {
        "ws_url": ws_url,
        "ws_id": ws_id,
    }

    api_key = os.getenv("WEAVE_API_KEY")
    if api_key:
        extra["api_key"] = api_key

    home_channel = os.getenv("WEAVE_HOME_CHANNEL") or ws_id

    # Weave 平台始终允许所有用户 — 跳过配对流程
    # Weave 后端自己做认证（WS 连接时验证 ws_id + api_key），不需要 Hermes 网关再配对
    os.environ.setdefault("WEAVE_ALLOW_ALL_USERS", "true")

    return {"extra": extra, "home_channel": home_channel}


def _apply_yaml_config(yaml_cfg: dict, platform_cfg: PlatformConfig) -> Optional[dict]:
    """从 config.yaml 的 platforms.weave 节读取配置"""
    if not yaml_cfg:
        return None
    extra: Dict[str, Any] = {}
    for key in ("ws_url", "ws_id", "api_key"):
        val = yaml_cfg.get(key)
        if val:
            extra[key] = val
            os.environ.setdefault(f"WEAVE_{key.upper()}", str(val))
    return {"extra": extra} if extra else None


def validate_config(config: PlatformConfig) -> bool:
    """验证配置是否完整（宽松模式 — .env 在运行时加载）"""
    return True  # 实际验证由 adapter.connect() 在启动时执行


def is_connected(config: PlatformConfig) -> bool:
    """快速检查（仅检查配置是否存在，不探测连接）"""
    extra = getattr(config, "extra", {}) or {}
    return bool(
        (os.getenv("WEAVE_WS_URL") or extra.get("ws_url"))
        and (os.getenv("WEAVE_WS_ID") or extra.get("ws_id"))
    )


# ── 适配器主类 ────────────────────────────────────────────────────


class WeaveAdapter(BasePlatformAdapter):
    """微织 Weave 平台适配器

    通过反向 WebSocket 连接到 Weave 后端：
      ws://{WEAVE_WS_URL}/api/v1/ai-contacts/ws/agent/{WEAVE_WS_ID}?api_key={WEAVE_API_KEY}

    Hermes Agent 主动发起连接，Weave 后端接受连接后双向通信。
    """

    # Weave 前端支持 Markdown 渲染
    supports_code_blocks = True

    # 流式协议标记 — 确保 message_done 触发持久化
    REQUIRES_EDIT_FINALIZE = True

    # WebSocket 消息大小上限（50MB，支持 base64 附件）
    _WS_MAX_SIZE = 52_428_800

    # 重连参数
    _RECONNECT_BASE_DELAY = 3.0
    _RECONNECT_MAX_DELAY = 60.0
    _RECONNECT_MAX_ATTEMPTS = 0  # 0 = 无限重连

    # 网关运行时引用（由 GatewayRunner 在注册适配器时注入）
    # 用于模型切换时设置 _session_model_overrides + 驱逐缓存的 agent
    gateway_runner = None

    def __init__(self, config, **kwargs):
        platform = Platform("weave")
        super().__init__(config=config, platform=platform)

        # 延迟加载 — connect() 中读取 os.environ，此时 .env 已就绪
        self.ws_url = ""
        self.ws_id = ""
        self.api_key = ""
        self._full_ws_url = ""

        # 运行状态
        self._config = config
        self._ws = None
        self._listen_task: Optional[asyncio.Task] = None
        self._reconnect_attempt = 0
        self._should_reconnect = True
        self._session_ids: Dict[str, str] = {}
        self._pending_session_requests: Dict[str, str] = {}
        self._message_buffer: List[dict] = []
        # 已通过 edit_message(finalize=True) 完成的消息
        # 防止 send(reply_to=...) 重复发送
        self._finalized_sessions: set = set()

        logger.info("[Weave] 适配器初始化 (延迟加载)")
    def _build_ws_url(self) -> str:
        """构造完整的 WebSocket 连接 URL"""
        base = self.ws_url.rstrip("/")
        path = f"/api/v1/ai-contacts/ws/agent/{self.ws_id}"
        url = f"{base}{path}"
        if self.api_key:
            url += f"?api_key={self.api_key}"
        return url

    @property
    def name(self) -> str:
        return "weave"

    @property
    def chat_id(self) -> str:
        """当前适配器的 chat_id（等于 ws_id）"""
        return self.ws_id

    # ── 生命周期 ─────────────────────────────────────────────────

    async def connect(self, **kwargs) -> bool:
        """连接到 Weave 后端"""
        # 兼容不同版本 Hermes：忽略 is_reconnect 等额外参数
        if not HAS_WEBSOCKETS:
            logger.error("[Weave] websockets 库未安装，请运行: pip install websockets")
            return False

        # 延迟加载环境变量（此时 .env 已由 Hermes 加载）
        extra = getattr(self._config, "extra", {}) or {}
        self.ws_url = os.getenv("WEAVE_WS_URL") or extra.get("ws_url", "")
        self.ws_id = os.getenv("WEAVE_WS_ID") or os.getenv("WEAVE_AGENT_ID") or extra.get("ws_id", "")
        self.api_key = os.getenv("WEAVE_API_KEY") or extra.get("api_key", "")
        self._full_ws_url = self._build_ws_url()
        logger.info("[Weave] 适配器启动: ws_id=%s, ws_url=%s", self.ws_id, self.ws_url)

        if not self.ws_url or not self.ws_id:
            logger.error("[Weave] WEAVE_WS_URL 或 WEAVE_WS_ID 未设置")
            return False

        self._should_reconnect = True
        self._reconnect_attempt = 0

        # 启动监听循环（含自动重连）
        self._listen_task = asyncio.create_task(self._connect_and_listen())
        try:
            self._background_tasks.add(self._listen_task)
        except TypeError:
            pass

        # 等待首次连接（最多 15 秒）
        for _ in range(30):
            if self._ws is not None:
                self._mark_connected()
                logger.info("[Weave] 连接成功: %s", self._full_ws_url)
                return True
            await asyncio.sleep(0.5)

        logger.warning("[Weave] 连接超时，后台将继续重连")
        return True  # 返回 True 让网关继续启动，后台重连

    async def disconnect(self) -> None:
        """断开连接"""
        self._should_reconnect = False
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await asyncio.wait_for(self._listen_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        self._mark_disconnected()
        logger.info("[Weave] 已断开连接")

    def _mark_connected(self):
        """标记为已连接（用于状态显示）"""
        self._running = True

    def _mark_disconnected(self):
        """标记为已断开"""
        self._running = False

    # ── WebSocket 连接与监听 ─────────────────────────────────────

    async def _connect_and_listen(self):
        """WebSocket 连接 + 监听循环（含自动重连）"""
        while self._should_reconnect:
            try:
                logger.info("[Weave] 连接中: %s", self._full_ws_url)
                self._ws = await websockets.connect(
                    self._full_ws_url,
                    max_size=self._WS_MAX_SIZE,
                    ping_interval=30,
                    ping_timeout=10,
                )

                # 发送握手消息
                await self._ws.send(json.dumps({
                    "type": "hello",
                    "agent_name": os.getenv("HERMES_AGENT_NAME", "Hermes Agent"),
                    "ws_id": self.ws_id,
                }))

                self._reconnect_attempt = 0
                self._mark_connected()
                logger.info("[Weave] WebSocket 已连接")

                # 刷新缓冲消息
                if self._message_buffer:
                    for msg in self._message_buffer:
                        await self._send_raw(msg)
                    self._message_buffer.clear()
                    logger.info("[Weave] 已刷新缓冲消息")

                # 消息接收循环
                async for raw_data in self._ws:
                    try:
                        data = json.loads(raw_data)
                        await self._handle_incoming_message(data)
                    except json.JSONDecodeError:
                        logger.warning("[Weave] JSON 解析失败: %s", raw_data[:200])
                    except Exception as e:
                        logger.exception("[Weave] 消息处理异常: %s", e)

            except ConnectionClosed:
                logger.info("[Weave] WebSocket 连接关闭")
            except WebSocketException as e:
                logger.warning("[Weave] WebSocket 异常: %s", e)
            except Exception as e:
                logger.exception("[Weave] 连接异常: %s", e)
            finally:
                self._ws = None
                self._mark_disconnected()

            # 自动重连
            if not self._should_reconnect:
                break

            self._reconnect_attempt += 1
            delay = min(
                self._RECONNECT_BASE_DELAY * (2 ** (self._reconnect_attempt - 1)),
                self._RECONNECT_MAX_DELAY,
            )
            logger.info("[Weave] %ds 后重连 (第 %d 次)", delay, self._reconnect_attempt)
            await asyncio.sleep(delay)

    # ── 入站消息处理 ─────────────────────────────────────────────

    async def _handle_incoming_message(self, data: dict):
        """处理来自 Weave 的消息"""
        msg_type = data.get("type")

        if msg_type == "pong":
            return

        if msg_type == "message":
            await self._handle_user_message(data)
        elif msg_type == "slash_command":
            await self._handle_slash_command(data)
        elif msg_type == "create_session":
            await self._handle_create_session(data)
        elif msg_type == "stop":
            await self._handle_stop(data)
        elif msg_type == "approval.respond":
            await self._handle_approval_respond(data)
        elif msg_type == "clarify.respond":
            await self._handle_clarify_respond(data)
        elif msg_type == "confirm.respond":
            await self._handle_confirm_respond(data)
        elif msg_type == "ping":
            await self._send_raw({"type": "pong"})
        else:
            logger.debug("[Weave] 未知消息类型: %s", msg_type)

    async def _handle_user_message(self, data: dict):
        """处理用户消息 -> 交给 Hermes Agent"""
        content = data.get("content", "")
        session_id = data.get("session_id", "")
        user_id = data.get("user_id", "weave_user")
        attachments = data.get("attachments", [])

        if not content and not attachments:
            return

        # 保存 session_id 映射
        if session_id:
            self._session_ids[self.chat_id] = session_id

        # 构造 MessageEvent
        source = self.build_source(
            chat_id=self.chat_id,
            chat_name="Weave Chat",
            chat_type="dm",
            user_id=user_id,
        )
        source.thread_id = session_id  # 用 thread_id 存 session_id

        # 处理附件：base64 data URL -> 缓存文件 -> media_urls
        media_urls = []
        media_types = []
        attachment_notes = []  # 附加到消息文本中的附件说明

        for att in attachments:
            att_name = att.get("name", "attachment")
            att_type = att.get("type", "file")
            att_data = att.get("data", "")

            if not att_data:
                continue

            try:
                # 解析 data URL: "data:image/png;base64,xxxx"
                import base64
                if "," in att_data:
                    header, b64_content = att_data.split(",", 1)
                    # 从 header 提取 MIME 类型
                    mime = header.split(":")[1].split(";")[0] if ":" in header else ""
                else:
                    b64_content = att_data
                    mime = att_type if "/" in att_type else ""

                raw_bytes = base64.b64decode(b64_content)

                # 根据类型选择缓存函数
                if att_type == "image" or mime.startswith("image/"):
                    from gateway.platforms.base import cache_image_from_bytes
                    ext = "." + (mime.split("/")[1] if "/" in mime else "png")
                    if ext == ".jpeg": ext = ".jpg"
                    cached_path = cache_image_from_bytes(raw_bytes, ext=ext)
                    media_urls.append(cached_path)
                    media_types.append(mime or f"image/{ext.lstrip('.')}")
                    logger.info("[Weave] 图片附件已缓存: %s -> %s (%d bytes)", att_name, cached_path, len(raw_bytes))
                else:
                    from gateway.platforms.base import cache_document_from_bytes
                    cached_path = cache_document_from_bytes(raw_bytes, att_name)
                    media_urls.append(cached_path)
                    media_types.append(mime or "application/octet-stream")
                    logger.info("[Weave] 文件附件已缓存: %s -> %s (%d bytes)", att_name, cached_path, len(raw_bytes))

                # 对小文本文件，将内容注入消息文本让 Agent 直接看到
                if att_type != "image" and not mime.startswith("image/") and len(raw_bytes) < 8000:
                    try:
                        text_content = raw_bytes.decode("utf-8", errors="replace")
                        attachment_notes.append(f"\n\n--- {att_name} ---\n{text_content}\n--- end of {att_name} ---")
                    except Exception:
                        pass
            except Exception as e:
                logger.warning("[Weave] 附件处理失败: %s - %s", att_name, e)
                attachment_notes.append(f"\n\n[附件 {att_name} 处理失败: {e}]")

        # 将文本附件内容追加到消息文本
        full_text = content + "".join(attachment_notes)

        # 根据附件类型设置消息类型
        msg_type = MessageType.TEXT
        if media_urls and all(t.startswith("image/") for t in media_types):
            msg_type = MessageType.PHOTO

        event = MessageEvent(
            text=full_text,
            message_type=msg_type,
            source=source,
            raw_message=data,
            media_urls=media_urls,
            media_types=media_types,
        )


        await self.handle_message(event)

    async def _handle_slash_command(self, data: dict):
        """处理斜杠命令"""
        command = data.get("command", "")
        args = data.get("args", "")
        session_id = data.get("session_id", "")
        request_id = data.get("request_id", "")
        user_id = data.get("user_id", "weave_user")

        # workspace 操作
        workspace_action = data.get("workspace_action", "")
        workspace_path = data.get("workspace_path", "")
        workspace_content = data.get("workspace_content", "")

        if session_id:
            self._session_ids[self.chat_id] = session_id

        # 命令名（去掉 / 前缀）
        cmd_name = command.lstrip("/").lower()

        # /stop, /abort, /cancel — 中断当前会话
        if cmd_name in ("stop", "abort", "cancel"):
            session_key = self._build_session_key(self.chat_id)
            self._interrupt_session(session_key)
            await self._send_raw({
                "type": "command_result",
                "command": command,
                "request_id": request_id,
                "session_id": session_id,
                "success": True,
                "content": "已停止生成",
                "side_effects": [{"type": "generation_stopped"}],
            })
            logger.info("[Weave] /stop 命令已执行")
            return

        # /new, /reset — 创建新会话
        if cmd_name in ("new", "reset"):
            await self._handle_create_session(data)
            await self._send_raw({
                "type": "command_result",
                "command": command,
                "request_id": request_id,
                "session_id": session_id,
                "success": True,
                "content": "已创建新会话",
                "side_effects": [{"type": "session_reset"}],
            })
            logger.info("[Weave] /new 命令已执行")
            return

        # /model (无参数) — 查询完整模型列表
        if cmd_name == "model" and not args.strip():
            try:
                from hermes_cli.inventory import build_models_payload, load_picker_context
                ctx = load_picker_context()
                payload = build_models_payload(ctx, include_unconfigured=True, canonical_order=True)

                model = payload.get("model", "unknown")
                provider = payload.get("provider", "")
                lines = [f"Current: `{model}` on {provider}", ""]
                for p in payload["providers"]:
                    if p["models"]:
                        lines.append(f"**{p['name']}** `--{p['slug']}`:")
                        for m in p["models"]:
                            lines.append(f"  `{m}`")
                        lines.append("")

                content = "\n".join(lines)
                await self._send_raw({
                    "type": "command_result",
                    "command": command,
                    "request_id": request_id,
                    "session_id": session_id,
                    "success": True,
                    "content": content,
                })
                logger.info("[Weave] /model 查询: %s, providers=%d", model, len(payload["providers"]))
                return
            except Exception as e:
                logger.warning("[Weave] /model 查询失败: %s", e)
                # 失败时也返回 command_result，不 fall through 到 AI 处理
                # 因为网关内置 /model 命令走 message_start 但不发 message_done，
                # 会导致前端 streaming 状态卡死
                await self._send_raw({
                    "type": "command_result",
                    "command": command,
                    "request_id": request_id,
                    "session_id": session_id,
                    "success": False,
                    "content": f"模型查询失败: {e}",
                })
                return

        # /model xxx --provider yyy - 切换模型
        if cmd_name == "model" and args.strip():
            try:
                from hermes_cli.config import load_config, save_config

                arg_line = args.strip()
                model_name = arg_line.split()[0]
                provider_slug = ""
                if "--provider" in arg_line:
                    provider_idx = arg_line.index("--provider") + len("--provider")
                    rest = arg_line[provider_idx:].strip()
                    provider_slug = rest.split()[0] if rest else ""

                cfg = load_config()
                if "model" not in cfg:
                    cfg["model"] = {}
                cfg["model"]["default"] = model_name
                if provider_slug:
                    cfg["model"]["provider"] = provider_slug
                save_config(cfg)

                # 通过 gateway_runner 驱逐缓存的 agent
                # 这样下次消息会从更新后的 config 创建新 agent
                if self.gateway_runner is not None:
                    try:
                        source = self.build_source(
                            chat_id=self.chat_id,
                            chat_name="Weave Chat",
                            chat_type="dm",
                            user_id=user_id,
                        )
                        session_key = self.gateway_runner._session_key_for_source(source)
                        self.gateway_runner._evict_cached_agent(session_key)
                        # 清除 session override，让网关从 config 读取新模型
                        self.gateway_runner._session_model_overrides.pop(session_key, None)
                        logger.info("[Weave] 已驱逐缓存 agent + 清除 session override: %s", session_key)
                    except Exception as e:
                        logger.warning("[Weave] gateway_runner 操作失败: %s", e)

                await self._send_raw({
                    "type": "command_result",
                    "command": command,
                    "request_id": request_id,
                    "session_id": session_id,
                    "success": True,
                    "content": f"模型已切换: {model_name}",
                    "side_effects": [{"type": "model_changed", "model": model_name}],
                })
                logger.info("[Weave] 模型切换成功: %s (provider=%s)", model_name, provider_slug)
                return
            except Exception as e:
                logger.warning("[Weave] 模型切换异常: %s", e)
                # 失败时也返回 command_result，不 fall through 到 AI 处理
                await self._send_raw({
                    "type": "command_result",
                    "command": command,
                    "request_id": request_id,
                    "session_id": session_id,
                    "success": False,
                    "content": f"模型切换失败: {e}",
                })
                return

        # /profile - 修改 AI 联系人在 Weave 上的头像/昵称/简介
        if cmd_name == "profile":
            await self._handle_profile_command(command, args, request_id, session_id)
            return

        # /reasoning, /yolo, /personality, /persona — Hermes 网关内置命令
        # 这些命令不能透传给 AI（网关处理，响应走 message_start 而非 command_result），
        # 适配器必须拦截并返回 command_result，否则前端 sendSilentCommand 超时。
        if cmd_name in ("reasoning", "yolo", "personality", "persona"):
            try:
                # 透传给 AI 处理（改变状态）
                full_text = f"{command} {args}".strip() if args else command
                source = self.build_source(
                    chat_id=self.chat_id,
                    chat_name="Weave Chat",
                    chat_type="dm",
                    user_id=user_id,
                )
                source.thread_id = session_id

                event = MessageEvent(
                    text=full_text,
                    message_type=MessageType.TEXT,
                    source=source,
                    raw_message=data,
                )

                if request_id:
                    self._pending_session_requests[request_id] = self.chat_id

                await self.handle_message(event)

                # 同时返回 command_result，让前端 sendSilentCommand 不超时
                # 注意：内置命令的响应不走 command_result，所以这里手动发一个
                await self._send_raw({
                    "type": "command_result",
                    "command": command,
                    "request_id": request_id,
                    "session_id": session_id,
                    "success": True,
                    "content": f"命令已发送: {command} {args}".strip(),
                })
            except Exception as e:
                logger.warning("[Weave] %s 命令异常: %s", cmd_name, e)
                await self._send_raw({
                    "type": "command_result",
                    "command": command,
                    "request_id": request_id,
                    "session_id": session_id,
                    "success": False,
                    "content": f"命令执行失败: {e}",
                })
            return

        # 其他命令 - 作为普通消息交给 Agent 处理
        full_text = f"{command} {args}".strip() if args else command
        source = self.build_source(
            chat_id=self.chat_id,
            chat_name="Weave Chat",
            chat_type="dm",
            user_id=user_id,
        )
        source.thread_id = session_id

        event = MessageEvent(
            text=full_text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=data,
        )

        # 存储 request_id 供 command_result 回传
        if request_id:
            self._pending_session_requests[request_id] = self.chat_id

        await self.handle_message(event)

    async def _handle_create_session(self, data: dict):
        """处理创建会话请求 — Hermes 是 session_id 的事实来源"""
        request_id = data.get("request_id", "")
        user_id = data.get("user_id", "weave_user")

        # 生成新的 session_id
        new_session_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S_") + uuid.uuid4().hex[:8]
        chat_id = uuid.uuid4().hex

        # 保存映射
        self._session_ids[self.chat_id] = new_session_id

        # 返回 session_created 给 Weave
        await self._send_raw({
            "type": "session_created",
            "request_id": request_id,
            "session_id": new_session_id,
            "chat_id": chat_id,
            "title": f"对话 {datetime.utcnow().strftime('%m-%d %H:%M')}",
        })

        logger.info("[Weave] 创建会话: session_id=%s, chat_id=%s", new_session_id, chat_id)

    async def _handle_stop(self, data: dict):
        """处理停止信号"""
        session_id = data.get("session_id", "")
        if session_id:
            # 中断当前会话的 Agent 运行
            session_key = self._build_session_key(self.chat_id)
            self._interrupt_session(session_key)

    async def _handle_profile_command(self, command, args, request_id, session_id):
        """处理 /profile 命令 - 修改 AI 联系人在 Weave 上的头像/昵称/简介

        用法：
          /profile                    - 显示用法
          /profile name 新昵称         - 修改昵称
          /profile desc 新简介         - 修改简介
          /profile avatar /path/to/img - 修改头像（本地文件）
        """
        args = args.strip()
        if not args:
            usage = (
                "**修改 AI 联系人简介**\n\n"
                "用法：\n"
                "- `/profile name 新昵称` - 修改昵称\n"
                "- `/profile desc 新简介` - 修改简介\n"
                "- `/profile avatar /path/to/img.png` - 修改头像\n\n"
                "示例：\n"
                "- `/profile name 我的工作助手`\n"
                "- `/profile desc 专注于编程和架构设计`\n"
                "- `/profile avatar ~/avatar.png`"
            )
            await self._send_raw({
                "type": "command_result",
                "command": command,
                "request_id": request_id,
                "session_id": session_id,
                "success": True,
                "content": usage,
            })
            return

        parts = args.split(None, 1)
        field = parts[0].lower()
        value = parts[1] if len(parts) > 1 else ""

        profile_data = {}
        hint = ""

        if field in ("name", "昵称"):
            if not value:
                hint = "请提供昵称，例如：/profile name 我的工作助手"
            else:
                profile_data["name"] = value[:50]
                hint = f"昵称已更新: {value[:50]}"

        elif field in ("desc", "description", "简介"):
            if not value:
                hint = "请提供简介，例如：/profile desc 专注于编程和架构设计"
            else:
                profile_data["description"] = value[:200]
                hint = f"简介已更新: {value[:200]}"

        elif field in ("avatar", "头像"):
            if not value:
                hint = "请提供头像文件路径，例如：/profile avatar ~/avatar.png"
            else:
                # 读取本地文件并转 Base64
                import base64
                expanded = os.path.expanduser(value)
                if not os.path.isfile(expanded):
                    hint = f"文件不存在: {expanded}"
                else:
                    try:
                        with open(expanded, "rb") as f:
                            raw = f.read()
                        if len(raw) > 512 * 1024:
                            hint = "头像文件不能超过 512KB"
                        else:
                            ext = expanded.rsplit(".", 1)[-1].lower()
                            mime_map = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg", "gif": "image/gif"}
                            mime = mime_map.get(ext, "image/png")
                            b64 = base64.b64encode(raw).decode()
                            profile_data["avatar"] = f"data:{mime};base64,{b64}"
                            hint = f"头像已更新: {os.path.basename(expanded)}"
                    except Exception as e:
                        hint = f"读取文件失败: {e}"

        else:
            hint = f"未知字段: {field}\n支持的字段: name(昵称), desc(简介), avatar(头像)"

        if not profile_data:
            await self._send_raw({
                "type": "command_result",
                "command": command,
                "request_id": request_id,
                "session_id": session_id,
                "success": False,
                "content": hint,
            })
            return

        # 发送 update_profile 到 Weave 后端
        await self._send_raw({
            "type": "update_profile",
            "data": profile_data,
        })

        # 返回命令结果给前端
        await self._send_raw({
            "type": "command_result",
            "command": command,
            "request_id": request_id,
            "session_id": session_id,
            "success": True,
            "content": hint,
        })
        logger.info("[Weave] /profile 命令: %s", profile_data)

    async def _handle_approval_respond(self, data: dict):
        """处理审批响应"""
        # 转发给 Agent 的审批系统
        session_key = self._build_session_key(self.chat_id)
        # Hermes 的审批系统通过 session_key + choice 处理
        logger.info("[Weave] 收到审批响应: %s", data.get("choice"))

    async def _handle_clarify_respond(self, data: dict):
        """处理澄清响应"""
        logger.info("[Weave] 收到澄清响应")

    async def _handle_confirm_respond(self, data: dict):
        """处理确认响应"""
        logger.info("[Weave] 收到确认响应")

    def _build_session_key(self, chat_id: str) -> str:
        """构造 session_key"""
        return f"weave:{chat_id}"

    def _interrupt_session(self, session_key: str):
        """中断会话"""
        guard = self._active_sessions.get(session_key)
        if guard:
            guard.set()

    # ── 出站消息（Hermes → Weave） ──────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """发送消息到 Weave

        发 message_start 创建占位符，后续由 edit_message() 和
        edit_message(finalize=True) 做流式更新和完成。

        当 reply_to 有值时，表示这是 _send_with_retry 发出的最终回复。
        流式消费者已经通过 edit_message(finalize=True) 完成了消息，
        此时跳过发送，避免重复。
        """
        session_id = self._session_ids.get(chat_id, "")

        # 最终回复（reply_to 有值）且已通过流式完成 -> 跳过，避免重复
        if reply_to is not None and session_id in self._finalized_sessions:
            return SendResult(success=True, message_id="weave_streaming")

        message = {
            "type": "message_start",
            "session_id": session_id,
            "content": content,
            "role": "assistant",
            "message_id": metadata.get("message_id") if metadata else None,
        }
        await self._send_raw(message)
        return SendResult(success=True, message_id="weave_streaming")

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """流式编辑消息

        当 finalize=True 时发送 message_done，否则发送 content_delta。
        """
        session_id = self._session_ids.get(chat_id, "")
        if finalize:
            msg_type = "message_done"
            self._finalized_sessions.add(session_id)
        else:
            msg_type = "content_delta"
        message = {
            "type": msg_type,
            "session_id": session_id,
            "content": content,
            "message_id": message_id,
        }
        await self._send_raw(message)
        return SendResult(success=True, message_id=message_id)
        pass

    async def send_image(
        self, chat_id: str, image_url: str, caption: Optional[str] = None
    ) -> SendResult:
        """发送图片 — 转为 Markdown 图片格式"""
        content = f"![image]({image_url})"
        if caption:
            content = f"{caption}\n{content}"
        return await self.send(chat_id, content)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """返回聊天信息"""
        return {
            "name": "Weave Chat",
            "type": "dm",
            "chat_id": chat_id,
        }

    # ── 工具调用事件转发 ─────────────────────────────────────────

    async def forward_tool_event(self, event_type: str, data: dict):
        """转发工具调用生命周期事件到 Weave 前端

        事件类型：
        - tool.call.started → {type: "tool.call.started", tool_name, tool_call_id, ...}
        - tool.call.completed → {type: "tool.call.completed", tool_name, ...}
        - session.usage → {type: "session.usage", ...}
        - session.complete → {type: "session.complete", ...}
        - assistant.thinking.delta → {type: "assistant.thinking.delta", ...}
        - assistant.text.delta → {type: "assistant.text.delta", ...}
        - tool.approval.required → {type: "tool.approval.required", ...}
        - tool.clarify.required → {type: "tool.clarify.required", ...}
        - tool.confirm.required → {type: "tool.confirm.required", ...}
        """
        session_id = self._session_ids.get(self.chat_id, "")
        data["session_id"] = session_id
        await self._send_raw(data)

    # ── 底层发送 ─────────────────────────────────────────────────

    async def _send_raw(self, message: dict):
        """发送原始 JSON 消息到 Weave"""
        if self._ws is None:
            # 连接断开，缓冲消息
            self._message_buffer.append(message)
            logger.debug("[Weave] 连接断开，消息已缓冲")
            return

        try:
            await self._ws.send(json.dumps(message, ensure_ascii=False, default=str))
        except ConnectionClosed:
            self._message_buffer.append(message)
            logger.warning("[Weave] 发送失败（连接关闭），消息已缓冲")
        except Exception as e:
            logger.error("[Weave] 发送失败: %s", e)
            self._message_buffer.append(message)


# ── 独立发送（Cron 投递支持） ────────────────────────────────────


async def _standalone_send(
    ws_url: str,
    ws_id: str,
    api_key: str,
    chat_id: str,
    content: str,
) -> dict:
    """独立发送函数 — 用于 Cron 任务（不依赖网关进程）"""
    if not HAS_WEBSOCKETS:
        return {"error": "websockets not installed"}

    base = ws_url.rstrip("/")
    url = f"{base}/api/v1/ai-contacts/ws/agent/{ws_id}"
    if api_key:
        url += f"?api_key={api_key}"

    try:
        async with websockets.connect(url, max_size=52_428_800) as ws:
            await ws.send(json.dumps({
                "type": "message",
                "session_id": "",
                "content": content,
                "role": "assistant",
            }))
            return {"success": True}
    except Exception as e:
        return {"error": str(e)}


# ── 插件注册入口 ─────────────────────────────────────────────────


def register(ctx):
    """插件注册入口 — 由 Hermes 插件系统调用"""
    ctx.register_platform(
        name="weave",
        label="Weave",
        adapter_factory=lambda cfg: WeaveAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["WEAVE_WS_URL", "WEAVE_WS_ID"],
        install_hint="运行 pip install websockets 安装依赖",
        env_enablement_fn=_env_enablement,
        apply_yaml_config_fn=_apply_yaml_config,
        cron_deliver_env_var="WEAVE_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allow_all_env="WEAVE_ALLOW_ALL_USERS",
        allowed_users_env="WEAVE_ALLOWED_USERS",
    )
    logger.info("[Weave] 插件已注册")
