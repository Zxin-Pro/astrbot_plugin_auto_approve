"""AstrBot 插件：入群自动审核 + 防刷屏禁言。

核心逻辑抽取自 zcj-ui/astrbot_plugin_group_guardian（MIT），
精简为无 WebUI / 无 SQLite 的独立轻量插件。
"""
import asyncio
import json
import re
import time
from collections import deque

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register

# ---------------------------------------------------------------- 常量
_REPEAT_IGNORED_RE = re.compile(
    r"^(?:\[(?:图片|语音|视频|表情|商城表情|文件|合并转发消息|空消息)\]\s*)+$"
)
_ANSWER_RE = re.compile(r"(?ms)(?:^|\r?\n)[ \t]*答案[:：][ \t]*(.*)\Z")
_QUESTION_HEAD_RE = re.compile(r"^[ \t]*问题[:：]")
_QUESTION_RE = re.compile(r"(?s)^[ \t]*问题[:：][ \t]*(.*?)(?:\r?\n)[ \t]*答案[:：]")
_ADMIN_CACHE_TTL = 600
_DEDUP_TTL = 2 * 3600
_QUEUE_MAX = 200

@register(
    "astrbot_plugin_auto_approve",
    "Zxin-Pro",
    "入群自动审核（通过词/拒绝词/LLM 判定）+ 防刷屏禁言（三档速率/夜间阈值/重复消息）",
    "2.0.0",
)
class GroupGuardLitePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._client = None
        # 防刷屏状态
        self._flood: dict = {}            # {gid: {uid: deque[(ts, msg_id, text)]}}
        self._flood_cleanup_at = 0.0
        self._penalty_until: dict = {}    # {gid: {uid: ts}}
        self._seen_ids: dict = {}         # {gid: {uid: {msg_id: monotonic}}}
        self._admin_cache: dict = {}      # {(gid,uid): (is_admin, expire)}
        self._processing_flags: set = set()

    async def initialize(self):
        logger.info(
            f"[auto_approve v2] 已加载 | 入群审核={self._cfg('join_audit_enabled', False)} "
            f"防刷屏={self._cfg('anti_flood_enabled', True)} "
            f"群白名单={self._cfg('group_whitelist', []) or '全部群'}"
        )

    # ================================================================ 配置
    def _cfg(self, key, default=None):
        v = self.config.get(key, default)
        return default if v is None else v

    def _cfg_int(self, key, default=0):
        try:
            return int(self._cfg(key, default))
        except (TypeError, ValueError):
            return default

    def _cfg_list(self, key):
        v = self._cfg(key, [])
        if isinstance(v, str):
            return [x.strip() for x in v.split(",") if x.strip()]
        return [str(x) for x in (v or []) if str(x).strip()]

    def _group_allowed(self, group_id: str) -> bool:
        wl = self._cfg_list("group_whitelist")
        return not wl or str(group_id) in wl

    # ================================================================ OneBot 客户端
    def _normalize_client(self, c):
        if c and (hasattr(c, "call_action") or hasattr(c, "api")):
            return c
        return None

    async def _get_client(self, event: AstrMessageEvent = None):
        if event is not None:
            c = self._normalize_client(getattr(event, "bot", None))
            if c:
                self._client = c
                return c
        c = self._normalize_client(self._client)
        if c:
            return c
        try:
            pm = self.context.platform_manager
            platforms = (pm.get_insts() if hasattr(pm, "get_insts") else None) or []
            for p in platforms:
                for attr in ("client", "bot", "api"):
                    c = self._normalize_client(getattr(p, attr, None))
                    if c:
                        self._client = c
                        return c
        except Exception as e:
            logger.debug(f"[auto_approve] platform_manager 取 client 失败: {e}")
        return None

    async def _call_api(self, client, action: str, **kwargs):
        """调用 OneBot API，返回 (ok, result_or_error)。"""
        try:
            api = getattr(client, "api", client)
            result = await api.call_action(action, **kwargs)
            return True, result
        except Exception as e:
            return False, str(e)

    async def _send_group_msg(self, group_id: str, text: str):
        client = await self._get_client()
        if not client:
            return
        ok, err = await self._call_api(
            client, "send_group_msg", group_id=int(group_id), message=text
        )
        if not ok:
            logger.debug(f"[auto_approve] 群消息发送失败: {err}")

    # ================================================================ 管理员判定（带缓存）
    async def _is_group_admin(self, group_id: str, user_id: str) -> bool:
        if not group_id or not user_id:
            return False
        key = (str(group_id), str(user_id))
        now = time.time()
        cached = self._admin_cache.get(key)
        if cached and cached[1] > now:
            return cached[0]
        is_admin = False
        client = await self._get_client()
        if client:
            ok, info = await self._call_api(
                client, "get_group_member_info",
                group_id=int(group_id), user_id=int(user_id), no_cache=False,
            )
            if ok and isinstance(info, dict):
                is_admin = info.get("role") in ("owner", "admin")
        self._admin_cache[key] = (is_admin, now + _ADMIN_CACHE_TTL)
        return is_admin

    # ================================================================ 事件监听
    @filter.event_message_type(filter.EventMessageType.ALL, priority=100)
    async def on_all_messages(self, event: AstrMessageEvent):
        try:
            raw = self._get_raw_event(event)
            if isinstance(raw, dict) and raw.get("post_type") == "request":
                await self._handle_join_request(event, raw)
                return
            if event.is_private_or_group_message() if hasattr(event, "is_private_or_group_message") else False:
                pass
            group_id = self._safe_group_id(event)
            if group_id:
                await self._handle_group_message(event, group_id)
        except Exception as e:
            logger.warning(f"[auto_approve] 事件处理出错: {e}")

    @staticmethod
    def _get_raw_event(event: AstrMessageEvent):
        raw = getattr(event, "raw_event", None)
        if isinstance(raw, dict):
            return raw
        msg_obj = getattr(event, "message_obj", None)
        raw = getattr(msg_obj, "raw_message", None) if msg_obj else None
        return raw if isinstance(raw, dict) else None

    @staticmethod
    def _safe_group_id(event) -> str:
        try:
            gid = event.get_group_id()
            return str(gid) if gid else ""
        except Exception:
            return ""

    # ================================================================ 入群审核
    async def _handle_join_request(self, event: AstrMessageEvent, raw: dict):
        if not self._cfg("join_audit_enabled", False):
            return
        if raw.get("request_type") != "group" or raw.get("sub_type") != "add":
            return  # 只审「申请加群」，群邀请不介入
        group_id = str(raw.get("group_id", ""))
        user_id = str(raw.get("user_id", ""))
        flag = str(raw.get("flag", ""))
        comment = str(raw.get("comment", "") or "")
        if not group_id or not flag or not self._group_allowed(group_id):
            return
        dedup_key = f"join:{flag}"
        if dedup_key in self._processing_flags:
            return
        self._processing_flags.add(dedup_key)
        try:
            await self._audit_join(event, group_id, user_id, flag, comment)
        finally:
            self._processing_flags.discard(dedup_key)

    @staticmethod
    def _extract_answer(comment: str) -> str:
        """剥离验证问题原文，只匹配用户填写的答案。"""
        if not comment or not _QUESTION_HEAD_RE.match(comment):
            return comment
        m = _ANSWER_RE.search(comment)
        return m.group(1).strip() if m else comment

    def _keyword_hit(self, answer_lower: str, keywords) -> str:
        for kw in keywords:
            kw = str(kw).strip()
            if kw and kw.lower() in answer_lower:
                return kw
        return ""

    def _ad_pattern_hit(self, text: str) -> bool:
        """简易广告特征：联系方式/引流话术。"""
        patterns = [
            r"(?i)(微信|vx|weixin|wx|qq|q群|扣群|扣扣)\s*[:：]?\s*[a-zA-Z0-9_-]{5,}",
            r"(?i)(加|\+v|\+v信|私聊|联系).{0,6}(我|客服|代理)",
            r"(?i)(刷单|兼职|返利|佣金|代理|招商|加盟|低价|折扣.{0,3}(充|币|点))",
            r"(?i)(邀请码|注册.{0,4}(链接|地址)|点击.{0,4}(链接|注册))",
            r"https?://",
        ]
        return any(re.search(p, text) for p in patterns)

    async def _audit_join(self, event, group_id, user_id, flag, comment):
        answer = self._extract_answer(comment)
        answer_lower = answer.lower()

        # 1. 拒绝词优先
        reject_kw = self._keyword_hit(answer_lower, self._cfg_list("join_reject_keywords"))
        if reject_kw:
            return await self._finish_join(
                event, flag, group_id, user_id, comment, False,
                self._cfg("join_reject_reason", "") or "申请信息命中拒绝规则",
                f"命中拒绝词: {reject_kw}",
            )

        # 2. 通过词（默认优先于广告特征）
        accept_kw = self._keyword_hit(answer_lower, self._cfg_list("join_accept_keywords"))
        accept_first = self._cfg("join_accept_overrides_ads", True)
        if accept_kw and accept_first:
            return await self._finish_join(
                event, flag, group_id, user_id, comment, True, "",
                f"命中通过词: {accept_kw}",
            )

        # 3. 广告特征
        ad_hit = bool(answer) and self._cfg("join_reject_ads", True) and self._ad_pattern_hit(answer)
        if ad_hit and not self._cfg("join_llm_enabled", False):
            return await self._finish_join(
                event, flag, group_id, user_id, comment, False,
                self._cfg("join_reject_reason", "") or "申请信息疑似广告引流",
                "命中广告特征",
            )

        # 4. 通过词兜底（广告未命中时）
        if accept_kw and not ad_hit:
            return await self._finish_join(
                event, flag, group_id, user_id, comment, True, "",
                f"命中通过词: {accept_kw}",
            )

        # 5. LLM 审核
        if self._cfg("join_llm_enabled", False) and answer:
            decision, llm_reason = await self._join_llm_judge(group_id, user_id, answer, ad_hit)
            if decision == "accept":
                return await self._finish_join(
                    event, flag, group_id, user_id, comment, True, "",
                    f"LLM判定通过: {llm_reason}",
                )
            if decision == "reject":
                return await self._finish_join(
                    event, flag, group_id, user_id, comment, False,
                    self._cfg("join_reject_reason", "") or "未通过智能入群审核",
                    f"LLM判定拒绝: {llm_reason}",
                )
            # manual / 失败 → 落到默认动作
            logger.info(f"[auto_approve] 入群LLM转人工/失败 group={group_id} user={user_id}: {llm_reason}")
            if ad_hit:
                return await self._finish_join(
                    event, flag, group_id, user_id, comment, False,
                    self._cfg("join_reject_reason", "") or "申请信息疑似广告引流",
                    "广告特征命中（LLM降级回退本地规则）",
                )

        # 6. 默认动作
        action = str(self._cfg("join_default_action", "manual")).strip().lower()
        if action == "accept":
            return await self._finish_join(
                event, flag, group_id, user_id, comment, True, "", "默认通过",
            )
        if action == "reject":
            return await self._finish_join(
                event, flag, group_id, user_id, comment, False,
                self._cfg("join_reject_reason", "") or "不符合入群条件", "默认拒绝",
            )
        # manual：不介入，交给人工处理

    async def _join_llm_judge(self, group_id, user_id, answer, ad_hit):
        """LLM 判断入群申请。返回 (accept/reject/manual, reason)。"""
        try:
            provider = None
            pid = str(self._cfg("join_llm_provider_id", "") or "").strip()
            try:
                if pid:
                    provider = self.context.provider_manager.get_provider_by_id(pid)
            except Exception:
                provider = None
            if provider is None:
                provider = self.context.get_using_provider()
            if provider is None:
                return "manual", "无可用 LLM Provider"

            extra = "（注意：申请文本命中了广告特征规则，请重点甄别）" if ad_hit else ""
            prompt = (
                f"以下是一份QQ群入群申请的用户填写的验证答案，请判断是否允许加入本群。\n"
                f"{extra}\n"
                f"判定标准：广告引流、推销、拉人头、发外链、博彩色情等一律拒绝；正常入群诉求通过；无法确定时返回 manual。\n"
                f"严格只输出 JSON：{{\"decision\": \"accept|reject|manual\", \"reason\": \"简短理由\"}}\n"
                f"验证答案：{answer[:500]}"
            )
            system = "你是入群申请审核员。只能输出 JSON，不要输出任何其他内容。"
            timeout = self._cfg_int("join_llm_timeout", 60)
            func = provider.text_chat
            import inspect
            if inspect.iscoroutinefunction(func):
                coro = func(prompt=prompt, system_prompt=system)
            else:
                return "manual", "Provider 不可调用"
            resp = await asyncio.wait_for(coro, timeout=timeout)
            text = getattr(resp, "completion_text", None) or (resp.get("completion_text") if isinstance(resp, dict) else "") or ""
            m = re.search(r"\{.*\}", text, re.S)
            if not m:
                return "manual", f"LLM返回非JSON: {text[:100]}"
            data = json.loads(m.group(0))
            decision = str(data.get("decision", "manual")).lower()
            if decision not in ("accept", "reject", "manual"):
                decision = "manual"
            return decision, str(data.get("reason", "") or "无理由")
        except asyncio.TimeoutError:
            return "manual", "LLM调用超时"
        except Exception as e:
            return "manual", f"LLM调用失败: {e}"

    async def _finish_join(self, event, flag, sub_group_id, user_id, comment,
                           approve: bool, reject_reason: str, audit_reason: str):
        client = await self._get_client(event)
        if not client:
            logger.warning("[auto_approve] 处理加群申请失败: 无法获取客户端")
            return
        payload = {"flag": flag, "sub_type": "add", "approve": bool(approve)}
        if not approve and reject_reason:
            payload["reason"] = reject_reason
        ok, err = await self._call_api(client, "set_group_add_request", **payload)
        if not ok:
            logger.warning(f"[auto_approve] 处理加群申请失败: {err}")
            return
        action = "通过" if approve else "拒绝"
        logger.info(f"[auto_approve] 入群{action} group={sub_group_id} user={user_id} 原因: {audit_reason}")
        if self._cfg("join_audit_notify", True):
            text = (
                f"[入群审核] 用户 {user_id} 申请加群已{action}\n"
                f"验证信息: {comment[:80] if comment else '无'}\n"
                f"原因: {audit_reason}"
            )
            await self._send_group_msg(sub_group_id, text)

    # ================================================================ 防刷屏
    @staticmethod
    def _normalize_text(text: str) -> str:
        return re.sub(r"\s+", " ", (text or "").strip().lower())

    async def _handle_group_message(self, event: AstrMessageEvent, group_id: str):
        if not self._cfg("anti_flood_enabled", True):
            return
        if not self._group_allowed(group_id):
            return
        try:
            user_id = str(event.get_sender_id() or "")
        except Exception:
            return
        if not user_id:
            return
        msg_id = ""
        try:
            msg_id = str(event.get_message_id() or "")
        except Exception:
            pass
        text = ""
        try:
            text = event.get_messages_str() or ""
        except Exception:
            pass

        # 事件去重（OneBot 可能重复推送）
        if msg_id:
            now_m = time.monotonic()
            seen = self._seen_ids.setdefault(group_id, {}).setdefault(user_id, {})
            for k, t0 in list(seen.items()):
                if t0 <= now_m - _DEDUP_TTL:
                    seen.pop(k, None)
            if msg_id in seen:
                return
            seen[msg_id] = now_m

        # 处罚冷却期：静默吸收积压消息
        until = self._penalty_until.get(group_id, {}).get(user_id, 0)
        if until and time.time() < until:
            return

        # 管理员豁免
        if self._cfg("anti_flood_admin_exempt", True) and await self._is_group_admin(group_id, user_id):
            return

        dq = self._flood.setdefault(group_id, {}).setdefault(user_id, deque(maxlen=_QUEUE_MAX))
        dq.append((time.time(), msg_id, self._normalize_text(text)))

        hit = self._check_flood(group_id, user_id)
        if hit:
            await self._punish_flood(group_id, user_id, hit)

    def _effective_limits(self, group_id: str, now: float) -> dict:
        limits = {
            "sec": self._cfg_int("anti_flood_rate_per_second", 5),
            "min": self._cfg_int("anti_flood_rate_per_minute", 20),
            "hour": self._cfg_int("anti_flood_rate_per_hour", 60),
            "night": False,
        }
        if self._cfg("anti_flood_night_enabled", False):
            start = max(0, min(self._cfg_int("anti_flood_night_start_hour", 0), 23))
            end = max(0, min(self._cfg_int("anti_flood_night_end_hour", 6), 23))
            hour = time.localtime(now).tm_hour
            in_night = (start == end) or (start < end and start <= hour < end) or \
                       (start > end and (hour >= start or hour < end))
            if in_night:
                limits.update({
                    "sec": self._cfg_int("anti_flood_night_rate_per_second", limits["sec"]),
                    "min": self._cfg_int("anti_flood_night_rate_per_minute", limits["min"]),
                    "hour": self._cfg_int("anti_flood_night_rate_per_hour", limits["hour"]),
                    "night": True,
                })
        return limits

    def _check_flood(self, group_id: str, user_id: str):
        """检查滑动窗口速率/重复消息，返回触发信息 dict 或 None。"""
        dq = self._flood.get(group_id, {}).get(user_id)
        if not dq:
            return None
        now = time.time()
        limits = self._effective_limits(group_id, now)
        sec_n = min_n = hour_n = 0
        sec_ids, min_ids, hour_ids = [], [], []

        repeat_on = self._cfg("repeat_detect_enabled", True)
        repeat_window = max(0, min(self._cfg_int("repeat_detect_window_seconds", 120), 3600))
        repeat_limit = self._cfg_int("repeat_detect_count", 3)
        long_text_limit = self._cfg_int("long_text_threshold", 0)
        cur_ts, cur_mid, cur_text = dq[-1]
        cur_key = "" if _REPEAT_IGNORED_RE.fullmatch(cur_text) else cur_text
        repeat_n = 0
        repeat_ids = []

        for ts, mid, txt in reversed(dq):
            dt = now - ts
            if dt >= 3600:
                break
            hour_n += 1
            hour_ids.append(mid)
            if dt < 60:
                min_n += 1
                min_ids.append(mid)
            if dt < 1:
                sec_n += 1
                sec_ids.append(mid)
            key = "" if _REPEAT_IGNORED_RE.fullmatch(txt) else txt
            if repeat_on and repeat_window > 0 and cur_key and dt < repeat_window and key == cur_key:
                repeat_n += 1
                repeat_ids.append(mid)

        label = "夜间" if limits["night"] else ""
        if limits["sec"] > 0 and sec_n > limits["sec"]:
            return {"rate": f"{label}每秒", "count": sec_n, "limit": limits["sec"], "msg_ids": sec_ids}
        if limits["min"] > 0 and min_n > limits["min"]:
            return {"rate": f"{label}每分钟", "count": min_n, "limit": limits["min"], "msg_ids": min_ids}
        if limits["hour"] > 0 and hour_n > limits["hour"]:
            return {"rate": f"{label}每小时", "count": hour_n, "limit": limits["hour"], "msg_ids": hour_ids}
        if long_text_limit > 0 and len(cur_text) > long_text_limit:
            return {"rate": "长文本", "count": len(cur_text), "limit": long_text_limit,
                    "msg_ids": [cur_mid] if cur_mid else []}
        if repeat_on and repeat_limit > 1 and repeat_n >= repeat_limit:
            return {"rate": "重复消息", "count": repeat_n, "limit": repeat_limit,
                    "msg_ids": repeat_ids[:repeat_limit]}
        return None

    async def _punish_flood(self, group_id: str, user_id: str, hit: dict):
        mute_seconds = self._cfg_int("anti_flood_mute_seconds", 600)
        client = await self._get_client()
        if not client:
            logger.warning("[auto_approve] 防刷屏处罚失败: 无客户端")
            return
        # 登记冷却 + 清空计数（先登记防并发再处罚）
        cooldown = max(60, mute_seconds)
        self._penalty_until.setdefault(group_id, {})[user_id] = time.time() + cooldown
        self._flood.get(group_id, {}).pop(user_id, None)

        muted = False
        if mute_seconds > 0:
            ok, err = await self._call_api(
                client, "set_group_ban",
                group_id=int(group_id), user_id=int(user_id), duration=mute_seconds,
            )
            muted = ok
            if not ok:
                logger.warning(f"[auto_approve] 刷屏禁言失败: {err}")
                self._penalty_until.get(group_id, {}).pop(user_id, None)

        # 可选撤回窗口内消息
        if muted and self._cfg("anti_flood_recall_messages", False):
            recall_n = self._cfg_int("anti_flood_recall_count", 10)
            for mid in list(reversed(hit.get("msg_ids", [])))[:recall_n]:
                if not mid or mid.startswith("seq:"):
                    continue
                try:
                    await self._call_api(client, "delete_msg", message_id=int(mid))
                except Exception:
                    pass

        if self._cfg("anti_flood_notify", True):
            mute_min = max(1, mute_seconds // 60)
            await self._send_group_msg(
                group_id,
                f"[防刷屏] 用户 {user_id} 触发{hit['rate']}刷屏"
                f"（{hit['count']}条 > 上限{hit['limit']}），已禁言 {mute_min} 分钟",
            )
        logger.info(
            f"[auto_approve] 防刷屏处罚 group={group_id} user={user_id} "
            f"类型={hit['rate']} 计数={hit['count']}/{hit['limit']} muted={muted}"
        )

    def _cleanup_flood(self):
        now = time.time()
        if now - self._flood_cleanup_at < 300:
            return
        self._flood_cleanup_at = now
        expired = now - 7200
        for gid in list(self._flood.keys()):
            users = self._flood[gid]
            for uid in list(users.keys()):
                dq = users[uid]
                while dq and dq[0][0] < expired:
                    dq.popleft()
                if not dq:
                    del users[uid]
            if not users:
                del self._flood[gid]
        for gid in list(self._penalty_until.keys()):
            users = self._penalty_until[gid]
            for uid in list(users.keys()):
                if now >= users[uid]:
                    del users[uid]
                    self._flood.get(gid, {}).pop(uid, None)
            if not users:
                del self._penalty_until[gid]
