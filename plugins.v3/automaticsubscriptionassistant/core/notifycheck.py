"""仅通知模式的资源检测：对暂停(S)的本插件订阅搜资源，命中就通知（绝不下载）。

与订阅落地解耦：只依赖注入的 ``oper``（SubscribeOper 鸭子类型）、``searchchain``、
``recognize``（可选）与 ``post_message``，便于离线单测。

通知样式对齐宿主的「订阅完成」模板：标题为「片名（年份）第N季 检测到有效资源」，
正文为「评分 / 来自用户 / 演员 / 简介」+ **有效资源数**；**不列出具体站点的资源与链接**。
"""
from __future__ import annotations

import datetime
from typing import Callable, Dict, Optional, Tuple

# 同一个订阅在 TTL 天内只通知一次，避免刷屏
NOTIFY_TTL_DAYS = 3
# 每轮最多检测的订阅数（超出部分留到下一轮，避免一次搜爆站点配额）
MAX_PER_RUN = 50


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


def _count_valid(contexts) -> int:
    """有效资源数：按资源标题去重计数（不关心站点与链接）。"""
    titles = {
        (getattr(getattr(ctx, "torrent_info", None), "title", "") or "").strip()
        for ctx in contexts
    }
    titles.discard("")
    return len(titles) or len(contexts)


def _build_notification(sub, mtype, count: int, info) -> Tuple[str, str]:
    """按「订阅完成」模板样式拼装 (title, text)：评分/来自用户/演员/简介 + 有效资源数。"""
    year = getattr(sub, "year", None)
    season = getattr(sub, "season", None)
    title = f"🎬 {getattr(sub, 'name', '')}"
    if year:
        title += f"（{year}）"
    if season and _is_tv(mtype):
        title += f"第{season}季"
    title += " 检测到有效资源"

    vote = getattr(sub, "vote", None)
    if not vote:
        vote = getattr(info, "vote_average", None)
    username = getattr(sub, "username", None)
    actors = getattr(info, "actors", None) if info is not None else None
    overview = getattr(sub, "description", None) or (
        getattr(info, "overview", None) if info is not None else None)

    text = ""
    if vote:
        text += f"评分：{vote}"
    if username:
        text += ("，" if text else "") + f"来自用户：{username}"
    if actors:
        text += f"\n演员：{actors}"
    if overview:
        text += f"\n简介：{overview}"
    text += f"\n有效资源数：{count}"
    text += "\n\n（仅通知模式：未自动下载；想下载可到「自动订阅助手」页把该订阅恢复为「订阅中」）"
    return title, text


def check_available(oper, searchchain, post_message: Callable, notified: Dict[str, dict],
                    username: str, recognize: Optional[Callable] = None,
                    ttl_days: int = NOTIFY_TTL_DAYS,
                    max_per_run: int = MAX_PER_RUN) -> Tuple[int, Dict[str, dict]]:
    """对暂停(S)的本插件订阅做资源检测，命中即通知。返回 (通知条数, 更新后的 notified)。

    ``recognize``（可选）：``sub -> mediainfo``，用于补「演员」等宿主媒体信息；失败不影响通知。
    """
    subs = oper.list_by_username(username, state="S") or []
    if not subs:
        return 0, notified
    if len(subs) > max_per_run:
        subs = subs[:max_per_run]
    now_ts = datetime.datetime.now().timestamp()
    hit = 0
    for sub in subs:
        try:
            mtype = None
            if getattr(sub, "type", None):
                from app.schemas.types import MediaType
                mtype = MediaType(sub.type)
        except Exception:  # noqa: BLE001
            mtype = None
        try:
            contexts = searchchain.search_by_title(sub.name, mtype=mtype) or []
        except Exception:  # noqa: BLE001 - 单条失败不中断
            continue
        if not contexts:
            continue
        rec = notified.get(str(sub.id)) or {}
        if now_ts - float(rec.get("ts", 0)) < ttl_days * 86400:
            continue
        info = None
        if callable(recognize):
            try:
                info = recognize(sub)
            except Exception:  # noqa: BLE001 - 识别失败只影响「演员」行
                info = None
        count = _count_valid(contexts)
        title, text = _build_notification(sub, mtype, count, info)
        post_message(title=title, text=text)
        notified[str(sub.id)] = {"ts": now_ts, "title": sub.name, "count": count}
        hit += 1
    return hit, notified
