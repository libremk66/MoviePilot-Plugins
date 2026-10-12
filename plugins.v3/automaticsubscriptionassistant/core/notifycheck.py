"""仅通知模式的资源检测：对暂停(S)的本插件订阅复刻宿主「搜索→规则→下载」决策链，
命中宿主真正保留的资源才通知（绝不下载、绝不改订阅状态）。

设计要点
--------
- 检测复用宿主订阅搜索链（``SearchChain.process``）逐参数同源：识别
  （``build_subscribe_meta`` + 订阅识别身份参数）→ 缺集检查（只读）→ 订阅规则组 /
  订阅过滤参数 / 洗版候选过滤 —— 被规则组或标题/季集匹配挡下的资源不再误报，
  同名不同片的假阳性由宿主 ``match_torrent`` 管线消除。
- 通知走宿主「订阅完成」模板渲染（正文：评分 / 来自用户 / 演员 / 简介），标题为
  原生 ``title_year`` + ``Sxx`` + 「检测到有效资源」；渲染失败退化为同源手拼格式。
- 宿主 API 不可用时整体跳过（fail-closed）：半程降级只会发出不对齐的通知。
"""
from __future__ import annotations

import datetime
from typing import Any, Callable, Dict, Optional, Tuple

# 同一个订阅在 TTL 天内只通知一次，避免刷屏
NOTIFY_TTL_DAYS = 3
# 每轮最多检测的订阅数（超出部分留到下一轮，避免一次搜爆站点配额）
MAX_PER_RUN = 50

# --------------------------------------------------------------------- #
# 通知样式开关位（后续如需接前端配置，只替换这几个常量的来源即可）
# --------------------------------------------------------------------- #
# 标题字样；置空则标题只保留 title_year(+Sxx)
TITLE_PHRASE = "检测到有效资源"
# 正文是否追加「有效资源数：N」（N = 宿主过滤匹配后保留的资源数）
SHOW_VALID_COUNT = True
# 正文是否追加「仅通知模式」脚注
SHOW_NOTIFY_ONLY_FOOTER = True
NOTIFY_ONLY_FOOTER = ("\n\n（仅通知模式：未自动下载；想下载可到「自动订阅助手」页"
                      "把该订阅恢复为「订阅中」）")

# 宿主 API 能力预检结果缓存（None=未检查）
_HOST_API_READY: Optional[bool] = None
# 宿主 logger 缓存（离线单测时退回标准 logging）
_LOGGER = None


def _logger():
    """宿主 logger；不在宿主环境时退回标准库 logging（保持模块可离线导入）。"""
    global _LOGGER
    if _LOGGER is None:
        try:
            from app.sdk.logging import logger
            _LOGGER = logger
        except Exception:  # noqa: BLE001
            import logging
            _LOGGER = logging.getLogger(__name__)
    return _LOGGER


def _load_host_api(log) -> bool:
    """一次性宿主 API 能力预检（fail-closed）。

    这组 API 同属一个宿主版本，要么全有要么全无；半程降级只会发出不对齐的通知，
    因此任一 import 失败即本轮整体跳过。
    """
    global _HOST_API_READY
    if _HOST_API_READY is None:
        try:
            from app.application.configuration import get_configured_system_config  # noqa: F401
            from app.application.messaging.message import MessageTemplateHelper  # noqa: F401
            from app.application.subscription.contract import build_subscribe_meta  # noqa: F401
            from app.chain.subscribe.identity import subscribe_recognize_kwargs  # noqa: F401
            from app.schemas.types import ContentType, MessageType, SystemConfigKey  # noqa: F401
            _HOST_API_READY = True
        except Exception as exc:  # noqa: BLE001
            _HOST_API_READY = False
            log.error(f"仅通知模式：宿主 API 不兼容（{exc}），资源检测整体跳过（不降级通知）")
    return bool(_HOST_API_READY)


def pause_active(oper, username: str, exempt=None) -> int:
    """把该插件仍处于「订阅中(R)」的订阅置为暂停(S)。返回处理条数。

    ``exempt`` 里的订阅 id（「仅通知豁免」名单）跳过不动：那是用户手动决定要追的。
    """
    exempt = set(exempt or [])
    n = 0
    for sub in oper.list_by_username(username, state="R") or []:
        if getattr(sub, "id", None) in exempt:
            continue
        try:
            oper.update(sub.id, {"state": "S"})
            n += 1
        except Exception:  # noqa: BLE001 - 单条失败不中断
            continue
    return n


def _is_tv(mtype) -> bool:
    try:
        from app.schemas.types import MediaType
        return mtype == MediaType.TV
    except Exception:  # noqa: BLE001
        return False


def _check_one(sub, searchchain, recognize, downloadchain, subscribechain, log):
    """单条订阅：复刻宿主订阅搜索链做只读检测。返回 ``(mediainfo, contexts)`` 或 None。

    与宿主 ``_process_paged_subscription``（app/chain/subscribe/search.py）逐参数同源，
    剥掉一切写操作（不下载、不改订阅状态、不发完成通知）。
    """
    # 0) 类型闸门：本插件只落电影/剧集（音乐等不在检测范围）
    from app.schemas.types import MediaType
    sub_type = getattr(sub, "type", None)
    if sub_type not in (MediaType.MOVIE.value, MediaType.TV.value):
        log.debug(f"仅通知模式：《{getattr(sub, 'name', '')}》类型 {sub_type} 不在检测范围，跳过")
        return None

    # 1) 识别（宿主 prepare_search_target 同款范式）
    from app.application.subscription.contract import build_subscribe_meta
    meta = build_subscribe_meta(sub)
    mediainfo = recognize(sub)
    if mediainfo is None:
        log.warning(f"仅通知模式：未识别到媒体信息，跳过《{getattr(sub, 'name', '')}》")
        return None

    # 2) 缺集检查（只读）。不用宿主 check_and_handle_existing_media——那会改订阅状态并
    #    发「订阅完成」通知，仅通知模式绝不能走。
    exist_flag, no_exists = downloadchain.get_no_exists_info(meta=meta, mediainfo=mediainfo)
    if exist_flag:
        log.debug(f"仅通知模式：《{getattr(sub, 'name', '')}》媒体库已存在，跳过")
        return None

    # 3) 规则组：订阅 filter_groups 优先，缺省回退系统配置（与宿主 rule_key 选择一致）
    from app.application.configuration import get_configured_system_config
    from app.schemas.types import SystemConfigKey
    rule_key = (
        SystemConfigKey.BestVersionFilterRuleGroups
        if getattr(sub, "best_version", None)
        else SystemConfigKey.SubscribeFilterRuleGroups
    )
    try:
        rule_groups = (
            getattr(sub, "filter_groups", None)
            or get_configured_system_config().get(rule_key)
            or []
        )
    except Exception as exc:  # noqa: BLE001
        # 读不到系统规则组时按空列表继续，由订阅自身过滤参数兜底；记 warning 便于排查。
        log.warning(f"仅通知模式：读取订阅规则组失败，按空规则继续：{exc}")
        rule_groups = []

    # 4) 搜索：宿主 SearchChain.process 内部按固定顺序执行
    #    站点搜索 → 订阅过滤参数 → 规则组 → 标题/年份/季集匹配 → 候选过滤 → 排序去重，
    #    返回即「宿主真会考虑下载的那批」；全程只读（唯一写是 Redis 进度条，瞬时无害）。
    from functools import partial
    candidate_filter = None
    _fsc = getattr(subscribechain, "_filter_search_contexts", None)
    if callable(_fsc):
        candidate_filter = partial(_fsc, sub)
    else:  # 宿主升级兼容：拿不到候选过滤就省略（普通订阅语义损失极小）
        log.debug("仅通知模式：宿主 _filter_search_contexts 不可用，省略候选过滤")
    contexts = searchchain.process(
        mediainfo=mediainfo,
        keyword=getattr(sub, "keyword", None),
        no_exists=no_exists,
        sites=subscribechain.get_sub_sites(sub),
        rule_groups=rule_groups,
        area=("imdbid"
              if getattr(sub, "search_imdbid", None) and getattr(mediainfo, "imdb_id", None)
              else "title"),
        custom_words=(sub.custom_words.split("\n")
                      if getattr(sub, "custom_words", None) else None),
        filter_params=subscribechain.get_params(sub),
        candidate_filter=candidate_filter,
    )
    return mediainfo, (contexts or [])


def _message_image(mediainfo):
    """取宿主消息海报地址；不可用返回 None（不阻断通知）。"""
    try:
        getter = getattr(mediainfo, "get_message_image", None)
        return getter() if callable(getter) else None
    except Exception:  # noqa: BLE001
        return None


def _render_template_text(mediainfo, username) -> str:
    """走宿主「订阅完成」模板渲染正文；失败/模板缺失返回空串（由调用方兜底）。

    与宿主 ``chain.post_message`` 的渲染条件一致：Message 的 title/text 留空 +
    指定 ctype + 提供上下文 kwargs，才会真正进入模板渲染。
    """
    try:
        from app.application.messaging.message import MessageTemplateHelper
        from app.schemas.message import Message
        from app.schemas.types import ContentType, MessageType
        msg = Message(
            mtype=MessageType.Subscribe,
            ctype=ContentType.SubscribeComplete,
            image=_message_image(mediainfo),
            username=username,
        )
        rendered = MessageTemplateHelper.render(
            msg, meta=None, mediainfo=mediainfo, msgstr="订阅", username=username,
            current_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        return (getattr(rendered, "text", None) or "").strip()
    except Exception as exc:  # noqa: BLE001 - 渲染失败改用兜底格式
        _logger().debug(f"仅通知模式：模板渲染失败，改用兜底格式：{exc}")
        return ""


def _fallback_text(mediainfo, username) -> str:
    """模板不可用时的兜底正文；字段与顺序、演员连接符与宿主模板完全同源。"""
    text = ""
    vote = getattr(mediainfo, "vote_average", None)
    if vote:
        text += f"评分：{vote}"
    if username:
        text += ("，" if text else "") + f"来自用户：{username}"
    names = []
    for actor in (getattr(mediainfo, "actors", None) or [])[:5]:
        name = actor.get("name") if isinstance(actor, dict) else getattr(actor, "name", None)
        if name:
            names.append(str(name))
    if names:
        text += "\n演员：" + "、 ".join(names)
    overview = getattr(mediainfo, "overview", None)
    if overview:
        text += f"\n简介：{overview}"
    return text


def _notify_title(mediainfo, sub) -> str:
    """标题对齐宿主原生格式：``title_year`` (+``Sxx``) + 检测字样。"""
    title_year = getattr(mediainfo, "title_year", None) or getattr(sub, "name", "") or ""
    parts = [title_year]
    if _is_tv(getattr(mediainfo, "type", None)):
        season = getattr(mediainfo, "season", None)
        if season is not None:
            parts.append(f"S{season:02d}")
    if TITLE_PHRASE:
        parts.append(TITLE_PHRASE)
    return " ".join(p for p in parts if p)


def _build_notification(sub, mediainfo, count: int) -> Dict[str, Any]:
    """构造 post_message 的 kwargs：原生模板标题 + 模板渲染正文 + 开关位附加行。"""
    username = getattr(sub, "username", None)
    text = _render_template_text(mediainfo, username)
    if not text:
        text = _fallback_text(mediainfo, username)
    if SHOW_VALID_COUNT:
        text += f"\n有效资源数：{count}"
    if SHOW_NOTIFY_ONLY_FOOTER:
        text += NOTIFY_ONLY_FOOTER
    return {
        "title": _notify_title(mediainfo, sub),
        "text": text,
        "image": _message_image(mediainfo),
    }


def check_available(oper, searchchain, post_message: Callable, notified: Dict[str, dict],
                    username: str, recognize: Optional[Callable] = None,
                    downloadchain=None, subscribechain=None,
                    ttl_days: int = NOTIFY_TTL_DAYS,
                    max_per_run: int = MAX_PER_RUN,
                    force: bool = False) -> Tuple[int, Dict[str, dict]]:
    """对暂停(S)的本插件订阅做资源检测，命中即通知。返回 (通知条数, 更新后的 notified)。

    ``recognize``：``sub -> mediainfo``（由插件注入宿主识别链）；``downloadchain`` /
    ``subscribechain``：宿主链实例（缺集检查 / 规则组与过滤参数）。
    ``force=True`` 忽略 TTL（供手动验证；仍会刷新 notified 时间戳）。
    """
    log = _logger()
    subs = oper.list_by_username(username, state="S") or []
    if not subs:
        return 0, notified
    if not callable(recognize) or downloadchain is None or subscribechain is None:
        log.error("仅通知模式：检测依赖缺失（recognize/downloadchain/subscribechain），本轮跳过")
        return 0, notified
    if not _load_host_api(log):
        return 0, notified
    now_ts = datetime.datetime.now().timestamp()
    ttl_seconds = 0 if force else ttl_days * 86400
    # 先按 TTL 过滤、再做上限截断：反过来的话，靠前的订阅每轮都占满名额，
    # 靠后的订阅永远排不进窗口（旧实现的饿死问题）。
    pending = [
        sub for sub in subs
        if now_ts - float((notified.get(str(sub.id)) or {}).get("ts", 0)) >= ttl_seconds
    ]
    if not pending:
        return 0, notified
    if len(pending) > max_per_run:
        pending = pending[:max_per_run]
    hit = 0
    for sub in pending:
        try:
            checked = _check_one(sub, searchchain, recognize, downloadchain, subscribechain, log)
        except Exception as exc:  # noqa: BLE001 - 单条失败不中断
            log.error(f"仅通知模式：订阅《{getattr(sub, 'name', '')}》检测异常（本轮跳过）：{exc}")
            continue
        if not checked:
            continue
        mediainfo, contexts = checked
        count = len(contexts)
        if count <= 0:
            continue
        try:
            post_message(**_build_notification(sub, mediainfo, count))
        except Exception as exc:  # noqa: BLE001 - 发送失败不记 TTL，下轮重试
            log.error(f"仅通知模式：发送通知失败：{exc}")
            continue
        notified[str(sub.id)] = {"ts": now_ts, "title": getattr(sub, "name", ""), "count": count}
        hit += 1
    return hit, notified
