"""仅通知模式的资源检测：对暂停(S)的本插件订阅搜资源，命中就通知（绝不下载）。

与订阅落地解耦：只依赖注入的 ``oper``（SubscribeOper 鸭子类型）、``searchchain``
与 ``post_message``，便于离线单测。
"""
from __future__ import annotations

import datetime
from typing import Callable, Dict, Tuple

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


def check_available(oper, searchchain, post_message: Callable, notified: Dict[str, dict],
                    username: str, ttl_days: int = NOTIFY_TTL_DAYS,
                    max_per_run: int = MAX_PER_RUN) -> Tuple[int, Dict[str, dict]]:
    """对暂停(S)的本插件订阅做资源检测，命中即通知。返回 (通知条数, 更新后的 notified)。"""
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
        ctx = contexts[0]
        ti = getattr(ctx, "torrent_info", None)
        tname = getattr(ti, "title", "") or sub.name
        site = getattr(ti, "site_name", "") or ""
        size = getattr(ti, "size", 0) or 0
        seeders = getattr(ti, "seeders", 0) or 0
        link = getattr(ti, "page_url", "") or getattr(ti, "enclosure", "") or ""
        text = (f"🎬 {sub.name}（{getattr(sub, 'year', None) or '—'}）检测到资源\n"
                f"资源：{tname}\n"
                f"站点：{site or '—'}　大小：{round(size / 1024 / 1024 / 1024, 2)} GB　做种：{seeders}\n")
        if link:
            text += f"链接：{link}\n"
        text += "\n（仅通知模式：未自动下载；想下载可到「自动订阅助手」页把该订阅恢复为「订阅中」）"
        post_message(title=f"🎬 自动订阅助手：{sub.name} 检测到资源", text=text)
        notified[str(sub.id)] = {"ts": now_ts, "title": sub.name, "torrent": tname}
        hit += 1
    return hit, notified
