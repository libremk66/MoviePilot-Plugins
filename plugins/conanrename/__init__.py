# coding=UTF-8
"""
柯南集数映射重命名（ConanRename）

按「银色子弹数据站」(sbsub) 的权威映射，把**拆分版集号**改名为 **TMDB 集号**，
解决"固定偏移（如 -60）在同一批次里也会变化"导致的错名问题。

典型流水线：
    订阅 → 1柯南做种 →【实时硬链接】→ 2柯南硬链 →【本插件】→ 3柯南重命名 →【目录实时监控】→ 入库刮削

识别词同步（v1.1.0 新增）：
    订阅搜索时 MP 用「自定义识别词」把种子名里的拆分版集号换算成 TMDB 集号。
    固定偏移（如 EP-60）只在某一段集号内正确 —— 本插件按当前映射的**每个偏移区间**
    各生成一条「区间限定」识别词，写回所选订阅的 custom_words，并可设定时同步。

    规则形如：Conan\\.S01E(?=12(?:4[6-9]|5[0-9]|6[01])(?:\\D|$)) <> \\.1996 >> EP-59
    （前定位词 + 区间断言 <> 后定位词 >> 集数偏移，只对该区间的集号生效）

安全约定：
    · 目标文件已存在时**跳过并告警**，绝不覆盖
    · 映射里查不到的集号 → 移到目标目录下的 `_待人工/`，不猜、不改
    · 支持「仅预演」，先看清单再实际执行
    · 每次实际改名都会写清单（插件数据目录 rename-YYYYMMDD.jsonl），可人工回滚
    · 识别词只写进插件自己维护的注释区块，订阅里其它识别词一律不动
"""
import json
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.types import EventType

# 处理的媒体文件后缀
MEDIA_EXT = {".mp4", ".mkv", ".ts", ".avi", ".rmvb", ".mov", ".flv", ".wmv", ".m2ts"}
# 集号识别：S01E1234 / E1234 / 第1234集
EP_PATTERNS = (re.compile(r"S\d{1,2}E(\d{1,4})", re.I),
               re.compile(r"[Ee][Pp](\d{1,4})"),
               re.compile(r"第\s*(\d{1,4})\s*[集话話]"))
# 文件名里的季集片段（用于替换）
EP_TOKEN = re.compile(r"S\d{1,2}E\d{1,4}(?:-part\d+)?")
# 识别词同步：插件维护区块的标记（MP 会跳过 # 开头的行）
SYNC_MARK_BEGIN = "# >>> ConanRename 自动同步（插件维护，请勿手改）"
SYNC_MARK_END = "# <<< ConanRename 自动同步区块结束"


class ConanRename(_PluginBase):
    # 插件名称
    plugin_name = "柯南集数映射重命名"
    # 插件描述
    plugin_desc = (
        "按银色子弹数据站(sbsub)的权威映射，把拆分版集号改名为 TMDB 集号"
        "（自动处理 -partN 多段与特辑），解决固定偏移会漂移的问题；"
        "并可按当前映射同步订阅的识别词、显示映射表。"
    )
    # 插件图标
    plugin_icon = "Linkace_C.png"
    # 插件版本
    plugin_version = "1.1.0"
    # 插件作者
    plugin_author = "libremk66"
    # 作者主页
    author_url = "https://github.com/libremk66"
    # 插件配置项ID前缀
    plugin_config_prefix = "conanrename_"
    # 加载顺序
    plugin_order = 5
    # 可使用的用户级别
    auth_level = 1

    # ─── 私有属性 ───
    _scheduler: Optional[BackgroundScheduler] = None
    _enabled: bool = False
    _notify: bool = False
    _onlyonce: bool = False
    _cron: Optional[str] = None
    _src_dir: str = ""
    _dst_dir: str = ""
    _mode: str = "move"
    _mapping_url: str = "https://cloud.sbsub.com/data/data.json"
    _extra_map: str = ""
    _dry_run: bool = False
    _cache_hours: int = 12
    # 识别词同步
    _sync_enabled: bool = False
    _sync_cron: Optional[str] = None
    _sync_sub_ids: List[int] = []
    _sync_mode: str = "range"
    _sync_front: str = r"Conan\.S01E"
    _sync_back: str = r"\.1996"
    _sync_keep_others: bool = True
    # 上次运行结果（供页面展示）
    _last_result: Dict[str, Any] = {}
    _last_sync: Dict[str, Any] = {}

    # ─── 生命周期 ───

    def init_plugin(self, config: dict = None):
        """读取配置；勾选"立即运行一次"时安排一次执行。"""
        self.stop_service()
        if config:
            self._enabled = bool(config.get("enabled"))
            self._notify = bool(config.get("notify"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._cron = config.get("cron")
            self._src_dir = str(config.get("src_dir") or "").strip()
            self._dst_dir = str(config.get("dst_dir") or "").strip()
            self._mode = config.get("mode") or "move"
            self._mapping_url = str(config.get("mapping_url") or "").strip() or \
                "https://cloud.sbsub.com/data/data.json"
            self._extra_map = config.get("extra_map") or ""
            self._dry_run = bool(config.get("dry_run"))
            try:
                self._cache_hours = int(config.get("cache_hours") or 12)
            except (TypeError, ValueError):
                self._cache_hours = 12
            # 识别词同步
            self._sync_enabled = bool(config.get("sync_enabled"))
            self._sync_cron = config.get("sync_cron")
            self._sync_sub_ids = self._normalize_sub_ids(config.get("sync_sub_ids"))
            self._sync_mode = config.get("sync_mode") or "range"
            self._sync_front = str(config.get("sync_front") or "").strip() or r"Conan\.S01E"
            self._sync_back = str(config.get("sync_back") or "").strip() or r"\.1996"
            self._sync_keep_others = bool(config.get("sync_keep_others", True))

        if self._enabled and self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
            self._scheduler.add_job(func=self.run_once, trigger="date",
                                    kwargs={"manual": False}, name="柯南映射重命名")
            if self._sync_enabled:
                self._scheduler.add_job(func=self.sync_words, trigger="date",
                                        kwargs={"manual": False}, name="柯南识别词同步")
            self._scheduler.start()
            self._onlyonce = False
            self.__update_config()

    def get_state(self) -> bool:
        return self._enabled

    def stop_service(self):
        """释放定时器。"""
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"柯南映射重命名：停止服务出错：{str(e)}")

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时服务（改名 + 识别词同步各一个）。"""
        services: List[Dict[str, Any]] = []
        if not self._enabled:
            return services
        if self._cron:
            services.append({
                "id": "ConanRename",
                "name": "柯南集数映射重命名",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.run_once,
                "kwargs": {"manual": False},
            })
        if self._sync_enabled and self._sync_cron:
            services.append({
                "id": "ConanRenameSync",
                "name": "柯南识别词同步",
                "trigger": CronTrigger.from_crontab(self._sync_cron),
                "func": self.sync_words,
                "kwargs": {"manual": False},
            })
        return services

    # ─── 配置与页面 ───

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled, "notify": self._notify, "onlyonce": self._onlyonce,
            "cron": self._cron, "src_dir": self._src_dir, "dst_dir": self._dst_dir,
            "mode": self._mode, "mapping_url": self._mapping_url,
            "extra_map": self._extra_map, "dry_run": self._dry_run,
            "cache_hours": self._cache_hours,
            "sync_enabled": self._sync_enabled, "sync_cron": self._sync_cron,
            "sync_sub_ids": self._sync_sub_ids, "sync_mode": self._sync_mode,
            "sync_front": self._sync_front, "sync_back": self._sync_back,
            "sync_keep_others": self._sync_keep_others,
        })

    @staticmethod
    def _normalize_sub_ids(value: Any) -> List[int]:
        """把表单里的订阅 ID（可能是字符串/列表）规整成 int 列表。"""
        if value is None:
            return []
        raw = value if isinstance(value, (list, tuple, set)) else [value]
        result: List[int] = []
        for item in raw:
            try:
                sid = int(str(item).strip())
            except (TypeError, ValueError):
                continue
            if sid not in result:
                result.append(sid)
        return result

    def _subscribe_items(self) -> List[dict]:
        """订阅下拉项（ID · 名称 · 类型）。"""
        try:
            from app.db.oper.subscribe import SubscribeOper
            items = []
            for sub in SubscribeOper().list():
                label = f"{sub.id} · {sub.name}"
                if getattr(sub, "year", None):
                    label += f"（{sub.year}）"
                if getattr(sub, "type", None):
                    label += f" · {sub.type}"
                items.append({"title": label, "value": int(sub.id)})
            return items
        except Exception as e:
            logger.warning(f"柯南识别词同步：读取订阅列表失败：{str(e)}")
            return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {"component": "VForm", "content": [
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "dry_run", "label": "仅预演（不改名、不写识别词）"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "src_dir", "label": "监控目录（递归扫描）",
                            "placeholder": "/你的盘/MOVIEPILOT/柯南/2柯南硬链"}}]},
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "dst_dir", "label": "目标目录",
                            "placeholder": "/你的盘/MOVIEPILOT/柯南/3柯南重命名"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VSelect", "props": {
                            "model": "mode", "label": "处理方式",
                            "items": [
                                {"title": "移动（源文件消失；源本身是硬链，做种不受影响）", "value": "move"},
                                {"title": "硬链接（源文件保留）", "value": "link"},
                            ]}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "cron", "label": "改名定时（cron，留空=只手动）",
                            "placeholder": "*/10 * * * *"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "mapping_url", "label": "映射数据地址（sbsub）"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "cache_hours", "label": "映射缓存小时数", "type": "number"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VTextarea", "props": {
                            "model": "extra_map", "label": "补充映射（每行：拆分版号=TMDB季集）",
                            "rows": 3, "placeholder": "1134=S0E32\n1262=S0E34"}}]},
                ]},
                {"component": "VDivider", "props": {"class": "my-4"}},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VAlert", "props": {
                            "type": "info", "variant": "tonal", "class": "mb-2",
                            "text": "识别词同步：按当前映射的每个「偏移区间」生成区间限定识别词，"
                                    "写回所选订阅的「自定义识别词」；只动插件自己维护的注释区块，"
                                    "订阅里其它识别词不受影响。集号区间变了（新特辑插入）会自动跟着更新。"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "sync_enabled", "label": "启用识别词同步"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "sync_keep_others", "label": "保留订阅里其它识别词"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "sync_cron", "label": "同步定时（cron，留空=只手动）",
                            "placeholder": "0 8 * * *"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VSelect", "props": {
                            "model": "sync_sub_ids", "label": "目标订阅（可多选）",
                            "multiple": True, "chips": True, "clearable": True,
                            "items": self._subscribe_items()}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VSelect", "props": {
                            "model": "sync_mode", "label": "识别词写法",
                            "items": [
                                {"title": "每个偏移区间一条（推荐，历史缺集也能正确识别）", "value": "range"},
                                {"title": "只写最新偏移一条（旧区间不管）", "value": "tail"},
                            ]}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "sync_front", "label": "前定位词（正则）",
                            "placeholder": "Conan\\.S01E"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "sync_back", "label": "后定位词（正则）",
                            "placeholder": "\\.1996"}}]},
                ]},
            ]}
        ], {
            "enabled": self._enabled, "notify": self._notify, "dry_run": self._dry_run,
            "src_dir": self._src_dir, "dst_dir": self._dst_dir, "mode": self._mode,
            "cron": self._cron, "mapping_url": self._mapping_url,
            "cache_hours": self._cache_hours, "extra_map": self._extra_map,
            "sync_enabled": self._sync_enabled, "sync_cron": self._sync_cron,
            "sync_sub_ids": self._sync_sub_ids, "sync_mode": self._sync_mode,
            "sync_front": self._sync_front, "sync_back": self._sync_back,
            "sync_keep_others": self._sync_keep_others,
        }

    def get_page(self) -> List[dict]:
        """页面：上次结果 + 识别词同步结果 + 当前映射表。"""
        content: List[dict] = []

        # 1) 上次改名
        result = self._last_result or {}
        if result:
            content.append({"component": "VAlert", "props": {
                "type": "warning" if (result.get("errors") or result.get("manual")) else "success",
                "variant": "tonal", "class": "mb-2",
                "text": f"上次改名 {result.get('time')}：扫描 {result.get('scanned')}｜"
                        f"改名 {result.get('renamed')}｜跳过 {result.get('skipped')}｜"
                        f"待人工 {result.get('manual')}｜错误 {len(result.get('errors') or [])}"}})
        else:
            content.append({"component": "VAlert", "props": {
                "type": "info", "variant": "tonal", "class": "mb-2", "text": "尚未运行过改名"}})

        # 2) 上次识别词同步
        sync = self._last_sync or {}
        if sync:
            detail = "｜".join(sync.get("detail") or []) or "无变化"
            content.append({"component": "VAlert", "props": {
                "type": "warning" if sync.get("errors") else "success",
                "variant": "tonal", "class": "mb-2",
                "text": f"上次识别词同步 {sync.get('time')}：规则 {sync.get('rules')} 条｜"
                        f"更新 {sync.get('changed')} 个订阅｜不变 {sync.get('unchanged')} 个"
                        + ("（预演）" if sync.get("dry_run") else "")
                        + f"｜{detail}"}})
        elif self._sync_enabled:
            content.append({"component": "VAlert", "props": {
                "type": "info", "variant": "tonal", "class": "mb-2", "text": "识别词同步已启用，尚未执行过"}})

        # 3) 当前映射
        try:
            data = self._load_mapping_data()
            mapping = self._build_mapping(data) if data else {}
        except Exception as e:
            logger.error(f"柯南映射重命名：页面读取映射失败：{str(e)}")
            mapping = {}
        if not mapping:
            content.append({"component": "VAlert", "props": {
                "type": "error", "variant": "tonal", "text": "映射数据不可用（请检查网络或映射数据地址）"}})
            return content

        ranges = self._compute_ranges(mapping)
        rules = self._build_rules(ranges, self._sync_mode)
        tail = ranges[-1]
        content.append({"component": "VAlert", "props": {
            "type": "info", "variant": "tonal", "class": "mb-2",
            "text": f"当前映射 {len(mapping)} 条（拆分版 {min(mapping)}–{max(mapping)}）｜"
                    f"偏移区间 {len(ranges)} 段｜最新偏移 {tail['offset']:+d}"
                    f"（拆分 {tail['start']}–{tail['end']} → TMDB {tail['tmdb_start']}–{tail['tmdb_end']}）｜"
                    f"识别词规则 {len(rules)} 条"}})

        # 区间表（倒序，最近的在前）
        headers = [
            {"title": "拆分版区间", "key": "split"},
            {"title": "TMDB 区间", "key": "tmdb"},
            {"title": "偏移", "key": "offset"},
            {"title": "集数", "key": "count"},
            {"title": "识别词规则", "key": "rule"},
        ]
        rows = []
        for item, rule in zip(ranges, rules):
            split = str(item["start"]) if item["start"] == item["end"] else f"{item['start']}–{item['end']}"
            tmdb = str(item["tmdb_start"]) if item["tmdb_start"] == item["tmdb_end"] else \
                f"{item['tmdb_start']}–{item['tmdb_end']}"
            rows.append({"split": split, "tmdb": tmdb, "offset": f"{item['offset']:+d}",
                         "count": item["count"], "rule": rule})
        rows.reverse()
        content.append({"component": "VDataTableVirtual", "props": {
            "class": "text-sm", "headers": headers, "items": rows,
            "height": "min(50vh, 28rem)", "density": "compact",
            "fixed-header": True, "hide-no-data": True, "hover": True,
            "items-per-page": -1}})
        return content

    def get_command(self) -> List[Dict[str, Any]]:
        return [{"cmd": "/conan_rename", "event": EventType.PluginAction,
                 "desc": "柯南映射重命名", "category": "管理",
                 "data": {"action": "conan_rename"}},
                {"cmd": "/conan_sync_words", "event": EventType.PluginAction,
                 "desc": "柯南识别词同步", "category": "管理",
                 "data": {"action": "conan_sync_words"}}]

    def get_api(self) -> List[Dict[str, Any]]:
        return [{"path": "/conan_rename", "endpoint": self.api_run, "methods": ["GET"],
                 "summary": "立即执行一次柯南映射重命名"},
                {"path": "/conan_sync_words", "endpoint": self.api_sync_words, "methods": ["GET"],
                 "summary": "立即同步一次识别词到订阅"},
                {"path": "/conan_words_preview", "endpoint": self.api_words_preview, "methods": ["GET"],
                 "summary": "预览将要写入的识别词（不写库）"}]

    @eventmanager.register(EventType.PluginAction)
    def remote_run(self, event: Event):
        if not event or not event.event_data:
            return
        action = event.event_data.get("action")
        if action == "conan_rename":
            result = self.run_once(manual=True)
            title = f"柯南映射重命名：改名 {result.get('renamed')} 个"
        elif action == "conan_sync_words":
            result = self.sync_words(manual=True)
            title = f"柯南识别词同步：规则 {result.get('rules')} 条，更新 {result.get('changed')} 个订阅"
        else:
            return
        if event.event_data.get("channel"):
            self.post_message(channel=event.event_data.get("channel"), title=title,
                              userid=event.event_data.get("user"))

    def api_run(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        return schemas.Response(success=True, data=self.run_once(manual=True))

    def api_sync_words(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        return schemas.Response(success=True, data=self.sync_words(manual=True))

    def api_words_preview(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        data = self._load_mapping_data()
        mapping = self._build_mapping(data) if data else {}
        if not mapping:
            return schemas.Response(success=False, message="映射数据不可用")
        ranges = self._compute_ranges(mapping)
        rules = self._build_rules(ranges, self._sync_mode)
        return schemas.Response(success=True, data={
            "ranges": len(ranges), "rules": rules, "text": self._compose_words("", rules),
            "front": self._sync_front, "back": self._sync_back, "mode": self._sync_mode,
        })

    # ─── 映射数据 ───

    def _cache_file(self) -> Path:
        return self.get_data_path() / "sbsub-data.json"

    def _load_mapping_data(self) -> Optional[dict]:
        """取 sbsub 数据：优先用未过期缓存，失败则退回旧缓存。"""
        cache = self._cache_file()
        if cache.exists() and self._cache_hours > 0:
            try:
                age = datetime.now() - datetime.fromtimestamp(cache.stat().st_mtime)
                if age < timedelta(hours=self._cache_hours):
                    return json.loads(cache.read_text(encoding="utf-8"))
            except Exception as e:
                logger.warning(f"读取映射缓存失败：{str(e)}")
        try:
            resp = requests.get(self._mapping_url, timeout=30,
                                headers={"User-Agent": "Mozilla/5.0 MoviePilot-Plugin/ConanRename"})
            resp.raise_for_status()
            data = resp.json()
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            logger.info("映射数据已更新")
            return data
        except Exception as e:
            logger.error(f"获取映射数据失败：{str(e)}")
            try:
                if cache.exists():
                    logger.warning("改用过期的映射缓存继续处理")
                    return json.loads(cache.read_text(encoding="utf-8"))
            except Exception:
                pass
            return None

    @staticmethod
    def _parse_split_nums(value: Any) -> List[int]:
        """'115' / '115-116' / '1202.5' → 整数集号列表。"""
        text = str(value or "").strip()
        if not text:
            return []
        if "-" in text:
            left, _, right = text.partition("-")
            try:
                return list(range(int(float(left)), int(float(right)) + 1))
            except (TypeError, ValueError):
                return []
        try:
            return [int(float(text))]
        except (TypeError, ValueError):
            return []

    def _build_mapping(self, data: dict) -> Dict[int, int]:
        """拆分版集号 → TMDB 集号（sbsub 列表的键即为 TMDB 集号）。"""
        mapping: Dict[int, int] = {}
        try:
            rows = data.get("res") or []
            episodes = rows[0][4] if rows and len(rows[0]) > 4 else {}
        except (IndexError, TypeError, KeyError):
            logger.error("映射数据结构不认识，请检查数据源")
            return mapping
        for key, item in episodes.items():
            if not isinstance(item, list) or not item:
                continue
            try:
                seq = float(key)
            except (TypeError, ValueError):
                continue
            if not seq.is_integer():
                continue  # 半集键（未被计入总集数的特辑）用补充映射处理
            for num in self._parse_split_nums(item[0]):
                mapping.setdefault(num, int(seq))
        return mapping

    # ─── 识别词同步 ───

    @staticmethod
    def _compute_ranges(mapping: Dict[int, int]) -> List[Dict[str, Any]]:
        """把映射切成「偏移相同且集号连续」的区间。"""
        ranges: List[Dict[str, Any]] = []
        current: Optional[Dict[str, Any]] = None
        for split, tmdb in sorted(mapping.items()):
            offset = tmdb - split
            if current and current["offset"] == offset and split == current["end"] + 1:
                current["end"] = split
                current["tmdb_end"] = tmdb
                current["count"] += 1
                continue
            if current:
                ranges.append(current)
            current = {"start": split, "end": split, "offset": offset,
                       "tmdb_start": tmdb, "tmdb_end": tmdb, "count": 1}
        if current:
            ranges.append(current)
        return ranges

    @staticmethod
    def _digits_class(digits: List[str]) -> str:
        """['6','7','8','9'] → '[6-9]'；['5'] → '5'。"""
        values = sorted(set(digits))
        if not values:
            return ""
        runs: List[Tuple[str, str]] = []
        start = prev = values[0]
        for ch in values[1:]:
            if ord(ch) == ord(prev) + 1:
                prev = ch
                continue
            runs.append((start, prev))
            start = prev = ch
        runs.append((start, prev))
        if len(runs) == 1 and runs[0][0] == runs[0][1]:
            return runs[0][0]
        body = "".join(a if a == b else f"{a}-{b}" for a, b in runs)
        return f"[{body}]"

    @classmethod
    def _fixed_width_regex(cls, low: int, high: int, width: int) -> str:
        """生成匹配定宽十进制区间 [low, high] 的紧凑正则片段。"""
        low_s, high_s = str(low).zfill(width), str(high).zfill(width)

        def build(pos: int, low_tight: bool, high_tight: bool) -> str:
            if pos >= width:
                return ""
            low_d = int(low_s[pos]) if low_tight else 0
            high_d = int(high_s[pos]) if high_tight else 9
            if low_d == high_d and low_tight and high_tight:
                return low_s[pos] + build(pos + 1, True, True)
            parts: List[str] = []
            if low_tight:
                parts.append(low_s[pos] + build(pos + 1, True, False))
                mid_low = low_d + 1
            else:
                mid_low = low_d
            mid_high = high_d - 1 if high_tight else high_d
            if mid_low <= mid_high:
                tail = "[0-9]" * (width - pos - 1)
                parts.append(cls._digits_class([str(d) for d in range(mid_low, mid_high + 1)]) + tail)
            if high_tight:
                parts.append(high_s[pos] + build(pos + 1, False, True))
            parts = [p for p in parts if p]
            if len(parts) == 1:
                return parts[0]
            return "(?:" + "|".join(parts) + ")"

        return build(0, True, True)

    @classmethod
    def _range_regex(cls, low: int, high: int) -> str:
        """生成匹配十进制区间 [low, high] 的正则片段（位数不限，前导零由调用方处理）。"""
        if low > high:
            low, high = high, low
        parts: List[str] = []
        for width in range(len(str(low)), len(str(high)) + 1):
            lo_w = max(low, 1 if width == 1 else 10 ** (width - 1))
            hi_w = min(high, 10 ** width - 1)
            if lo_w > hi_w:
                continue
            parts.append(cls._fixed_width_regex(lo_w, hi_w, width))
        if not parts:
            return str(low)
        return parts[0] if len(parts) == 1 else "(?:" + "|".join(parts) + ")"

    def _build_rules(self, ranges: List[Dict[str, Any]], mode: str) -> List[str]:
        """按区间生成识别词规则。"""
        if not ranges:
            return []
        front = self._sync_front or r"Conan\.S01E"
        back = self._sync_back or r"\.1996"
        if (mode or "range") == "tail":
            tail = ranges[-1]
            return [f"{front} <> {back} >> EP{tail['offset']:+d}"]
        rules: List[str] = []
        for item in ranges:
            if item["offset"] == 0:
                continue  # 偏移 0 = 拆分版号与 TMDB 号一致，不需要换算
            span = self._range_regex(item["start"], item["end"])
            # 区间断言放在前定位词后：只对该段集号生效，避免不同偏移互相打架；
            # 0* 容忍集号前导零（S01E0032 / S01E32 都能匹配）
            rules.append(f"{front}(?=0*(?:{span})(?:\\D|$)) <> {back} >> EP{item['offset']:+d}")
        return rules

    def _compose_words(self, existing: str, rules: List[str]) -> str:
        """把规则写进插件维护的注释区块，保留订阅里其它识别词（含手工写的旧版同步规则）。"""
        block = [SYNC_MARK_BEGIN, *rules, SYNC_MARK_END]
        front = self._sync_front or r"Conan\.S01E"
        kept: List[str] = []
        inside = False
        for line in (existing or "").splitlines():
            stripped = line.strip()
            if stripped == SYNC_MARK_BEGIN:
                inside = True
                continue
            if stripped == SYNC_MARK_END:
                inside = False
                continue
            if inside or not stripped:
                continue
            # 丢掉"前定位词 <> … >> EP…"这类旧的手工同步规则，避免与新区间规则抢命中
            if "<>" in stripped and ">>" in stripped:
                try:
                    if re.match(front, stripped):
                        continue
                except re.error:
                    pass
            kept.append(line.rstrip())
        return "\n".join(kept + block)

    def sync_words(self, manual: bool = False) -> Dict[str, Any]:
        """按当前映射刷新所选订阅的识别词。"""
        started = datetime.now()
        result: Dict[str, Any] = {
            "time": started.strftime("%Y-%m-%d %H:%M:%S"), "manual": manual,
            "dry_run": self._dry_run, "rules": 0, "changed": 0, "unchanged": 0,
            "errors": [], "detail": [],
        }
        data = self._load_mapping_data()
        mapping = self._build_mapping(data) if data else {}
        if not mapping:
            result["errors"].append("映射数据获取失败")
            self._last_sync = result
            return result
        ranges = self._compute_ranges(mapping)
        rules = self._build_rules(ranges, self._sync_mode)
        result["rules"] = len(rules)
        result["ranges"] = len(ranges)
        if not self._sync_sub_ids:
            result["errors"].append("未选择目标订阅")
            self._last_sync = result
            return result

        try:
            from app.db.oper.subscribe import SubscribeOper
        except Exception as e:
            result["errors"].append(f"无法访问订阅数据：{str(e)}")
            self._last_sync = result
            return result

        oper = SubscribeOper()
        for sid in self._sync_sub_ids:
            try:
                sub = oper.get(sid)
                if not sub:
                    result["errors"].append(f"订阅 {sid} 不存在")
                    continue
                existing = sub.custom_words or ""
                new_words = self._compose_words(existing if self._sync_keep_others else "", rules)
                if new_words.strip() == existing.strip():
                    result["unchanged"] += 1
                    result["detail"].append(f"{sub.name} 无需更新")
                    continue
                if self._dry_run:
                    result["changed"] += 1
                    result["detail"].append(f"[预演] {sub.name} 将写入 {len(rules)} 条规则")
                    continue
                oper.update(sid, {"custom_words": new_words})
                result["changed"] += 1
                result["detail"].append(f"{sub.name} 已更新 {len(rules)} 条规则")
                logger.info(f"柯南识别词同步：订阅 {sid}（{sub.name}）已写入 {len(rules)} 条规则")
            except Exception as e:
                result["errors"].append(f"订阅 {sid} 处理失败：{str(e)[:80]}")

        self._last_sync = result
        logger.info(f"柯南识别词同步完成：规则 {result['rules']} 条｜更新 {result['changed']}｜"
                    f"不变 {result['unchanged']}｜错误 {len(result['errors'])}")
        for err in result["errors"][:5]:
            logger.warning(err)
        if self._notify and (result["changed"] or result["errors"]):
            self.post_message(
                mtype=NotificationType.Manual,
                title=f"柯南识别词同步：更新 {result['changed']} 个订阅"
                      + ("（预演）" if self._dry_run else ""),
                text=f"规则 {result['rules']} 条｜错误 {len(result['errors'])}",
            )
        return result

    def _load_extra_map(self) -> Dict[int, str]:
        """补充映射，如 1262=S0E34。"""
        result: Dict[int, str] = {}
        for line in (self._extra_map or "").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            left, _, right = line.partition("=")
            try:
                result[int(left.strip())] = right.strip().upper()
            except (TypeError, ValueError):
                logger.warning(f"补充映射格式不对，已忽略：{line}")
        return result

    @staticmethod
    def _detect_episode(stem: str) -> Optional[int]:
        """从文件名里取拆分版集号。"""
        for pattern in EP_PATTERNS:
            match = pattern.search(stem)
            if match:
                return int(match.group(1))
        return None

    # ─── 主流程 ───

    def run_once(self, manual: bool = False) -> Dict[str, Any]:
        """扫描 → 映射 → 改名，返回统计结果。"""
        started = datetime.now()
        result: Dict[str, Any] = {
            "time": started.strftime("%Y-%m-%d %H:%M:%S"), "manual": manual,
            "scanned": 0, "renamed": 0, "skipped": 0, "manual": 0,
            "errors": [], "detail": [],
        }
        if not self._src_dir or not self._dst_dir:
            result["errors"].append("未配置监控目录或目标目录")
            self._last_result = result
            return result
        src_root, dst_root = Path(self._src_dir), Path(self._dst_dir)
        if not src_root.is_dir():
            result["errors"].append(f"监控目录不存在：{src_root}")
            self._last_result = result
            return result

        data = self._load_mapping_data()
        if not data:
            result["errors"].append("映射数据获取失败")
            self._last_result = result
            return result
        mapping = self._build_mapping(data)
        if not mapping:
            result["errors"].append("映射表为空")
            self._last_result = result
            return result
        extra = self._load_extra_map()
        logger.info(f"柯南映射重命名：映射 {len(mapping)} 条，最大拆分版 "
                    f"{max(mapping)}；开始扫描 {src_root}")

        # 1. 扫描（递归；跳过隐藏/临时目录）
        candidates: List[Tuple[Path, int, str]] = []
        for path in sorted(src_root.rglob("*")):
            try:
                if not path.is_file() or path.suffix.lower() not in MEDIA_EXT:
                    continue
                if any(p.startswith(".") or p.startswith("@") for p in path.parts):
                    continue
                if path.name.endswith(".part") or path.name.startswith("."):
                    continue
            except OSError:
                continue
            result["scanned"] += 1
            split_no = self._detect_episode(path.stem)
            if split_no is None:
                result["errors"].append(f"无法识别集号：{path.name}")
                continue
            candidates.append((path, split_no, path.stem))

        # 2. 分组：同一 TMDB 集号对应多个拆分版时加 -partN
        groups: Dict[int, List[int]] = {}
        for _, split_no, _ in candidates:
            tmdb = mapping.get(split_no)
            if tmdb is not None:
                groups.setdefault(tmdb, []).append(split_no)

        plan: List[Tuple[Path, Path]] = []
        manual_count = 0
        for path, split_no, stem in candidates:
            if split_no in extra:
                tag = extra[split_no]
            elif split_no in mapping:
                tmdb = mapping[split_no]
                members = sorted(groups.get(tmdb, []))
                tag = f"S01E{tmdb:04d}"
                if len(members) > 1:
                    tag = f"{tag}-part{members.index(split_no) + 1}"
            else:
                manual_count += 1
                result["detail"].append(f"待人工（映射里没有）：{path.name}")
                plan.append((path, dst_root / "_待人工" / path.name))
                continue
            new_stem = EP_TOKEN.sub(tag, stem, count=1)
            if new_stem == stem:
                new_stem = f"{stem}.{tag}"
            # 注意：stem 不含扩展名，必须拼回原后缀
            plan.append((path, dst_root / f"{new_stem}{path.suffix}"))
        result["manual"] = manual_count

        # 3. 执行（绝不覆盖已有文件）
        manifest: List[dict] = []
        for src, dst in plan:
            try:
                if dst.exists() and dst.resolve() != src.resolve():
                    result["skipped"] += 1
                    result["detail"].append(f"目标已存在，跳过：{dst.name}")
                    continue
                if self._dry_run:
                    result["renamed"] += 1
                    result["detail"].append(f"[预演] {src.name} → {dst.name}")
                    continue
                dst.parent.mkdir(parents=True, exist_ok=True)
                if self._mode == "link":
                    os.link(src, dst)
                else:
                    shutil.move(str(src), str(dst))
                result["renamed"] += 1
                manifest.append({"src": str(src), "dst": str(dst)})
            except Exception as e:
                result["errors"].append(f"{src.name} 处理失败：{str(e)[:80]}")

        # 4. 清单落盘 + 清理空目录
        if manifest:
            try:
                log_file = self.get_data_path() / f"rename-{started:%Y%m%d}.jsonl"
                with open(log_file, "a", encoding="utf-8") as f:
                    for item in manifest:
                        f.write(json.dumps(item, ensure_ascii=False) + "\n")
            except Exception as e:
                logger.error(f"写改名清单失败：{str(e)}")
            try:
                for path in sorted(src_root.rglob("*"), reverse=True):
                    if path.is_dir() and not any(path.iterdir()):
                        path.rmdir()
            except Exception:
                pass

        self._last_result = result
        logger.info(f"柯南映射重命名完成：扫描 {result['scanned']}｜改名 {result['renamed']}｜"
                    f"跳过 {result['skipped']}｜待人工 {result['manual']}｜"
                    f"错误 {len(result['errors'])}")
        for err in result["errors"][:5]:
            logger.warning(err)
        if self._notify and (result["renamed"] or result["errors"] or result["manual"]):
            self.post_message(
                mtype=NotificationType.Manual,
                title=f"柯南映射重命名：改名 {result['renamed']} 个"
                      + ("（预演）" if self._dry_run else ""),
                text=f"扫描 {result['scanned']}｜跳过 {result['skipped']}｜"
                     f"待人工 {result['manual']}｜错误 {len(result['errors'])}",
            )
        return result
