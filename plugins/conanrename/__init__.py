# coding=UTF-8
"""
柯南全流程入库（ConanRename 2.0.2）

一条插件搞定柯南的「监听 → 集号换算 → 硬链入库 → 订阅事实同步」，替代原先
「LinkMonitorPlus 硬链 + 本插件改名 + CloudLinkMonitor 入库」三段式。

集号换算优先级（v2.0.0 核心）：
    1. 补充映射（extra_map，手工指定，如 1262=S0E34）
    2. sbsub 精确映射
    3. **tail 偏移外推**：sbsub 对当周新集通常滞后数小时到一天。此时沿用
       映射表最后一段的偏移（与订阅识别词用的偏移同源），保证"下载时算出的集号"
       与"入库时算出的集号"永远一致。
    4. 超出宽限窗口 / 解析不出集号 → 隔离区，不碰媒体库，下轮重试并告警

外推记录会写进账本（source=extrapolated）。sbsub 后续补上精确映射且与外推值
不同时，插件自动把库里的文件改名到正确集号（连带 nfo/jpg），并发通知。

安全约定：
    · 入库走宿主 TransferChain，类型固定 link，源文件（做种）永不移动/删除
    · 目标已存在时不覆盖（overwrite_mode=never）
    · 同源文件按 (size, mtime, ino) 指纹判重，重下/替换会自动重跑
    · 支持「仅预演」：只打印计划，不调宿主、不写库
    · 识别词只写进插件自己维护的注释区块，订阅里其它识别词一律不动
"""
import json
import os
import re
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

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
# 账本文件名
LEDGER_FILE = "pipeline.json"
# 映射后备数据（仓库快照，sbsub 不可用时兜底）。按顺序尝试，先到先得。
FALLBACK_MAPPING_URLS = (
    "https://cdn.jsdelivr.net/gh/libremk66/MoviePilot-Plugins@main/conan-mapping/mapping.json",
    "https://raw.githubusercontent.com/libremk66/MoviePilot-Plugins/main/conan-mapping/mapping.json",
)


class _WatchHandler(FileSystemEventHandler):
    """目录监控回调：把新增/移动的文件交给插件主流程。"""

    def __init__(self, mon_path: str, plugin: "ConanRename"):
        super().__init__()
        self._mon_path = mon_path
        self._plugin = plugin

    def on_created(self, event):
        if not event.is_directory:
            self._plugin.enqueue_path(Path(event.src_path))

    def on_moved(self, event):
        if not event.is_directory:
            self._plugin.enqueue_path(Path(event.dest_path))


class ConanRename(_PluginBase):
    # 插件名称
    plugin_name = "柯南全流程入库"
    # 插件描述
    plugin_desc = (
        "柯南一站式入库：监听做种目录 → 按 sbsub 映射（缺失时用 tail 偏移外推）换算成 TMDB 集号 "
        "→ 硬链入库并刮削。替代「硬链 + 改名 + 目录监控」三段式，新集不再因映射滞后掉进待人工。"
    )
    # 插件图标
    plugin_icon = "Linkace_C.png"
    # 插件版本
    plugin_version = "2.0.2"
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
    # 2.0 全流程
    _src_dir: str = ""
    _library_dir: str = ""
    _mode: str = "fast"            # fast / compatibility
    _transfer_type: str = "link"
    _scrape: bool = True
    _overwrite_mode: str = "never"
    _media_id: str = ""            # TMDB ID，留空则按标题识别
    _media_title: str = ""
    _category_folder: bool = True
    _grace: int = 30               # 外推宽限窗口（集）
    _quarantine_dir: str = ""
    _bootstrap: bool = False       # 首次启用：把做种目录里已有的历史文件一次性入账（不重复入库）
    # 映射
    _mapping_url: str = "https://cloud.sbsub.com/data/data.json"
    _fallback_urls: List[str]
    _fallback_cache_minutes: int = 360
    _mapping_retry_minutes: int = 30
    _mapping_retry_at: datetime = datetime.min
    _mapping_source: str = ""
    _extra_map: str = ""
    _dry_run: bool = False
    _cache_minutes: int = 20
    # 旧版改名（仅作为兼容/兜底，可留空）
    _legacy_dst_dir: str = ""
    # 识别词同步
    _sync_enabled: bool = False
    _sync_onlyonce: bool = False
    _sync_cron: Optional[str] = None
    _sync_sub_ids: List[int] = []
    _sync_mode: str = "range"
    # 定位词要同时容忍「点」和「空格」两种分隔：本地文件名用点（Conan.S01E1215.1996），
    # 站点种子名用空格（Conan S01E1215 1996）。只认点会让规则在订阅搜索时静默失效。
    _sync_front: str = r"Conan[\s.]S01E"
    _sync_back: str = r"[\s.]1996"
    _sync_keep_others: bool = True
    # 运行时状态。**全部必须是实例级**：这些字段若在类上赋可变默认值，
    # 所有实例（含重载前后的新旧实例）会共享同一个列表/锁，导致
    # 「旧 observer 停不掉 + 新 observer 重复启动 + 锁被跨实例持有」。
    # 因此这里只做类型标注，实际对象在 init_plugin 里创建。
    _observers: List[Any]
    _queue: List[Path]
    _debounce_timer: Optional[threading.Timer]
    _last_result: Dict[str, Any]
    _last_sync: Dict[str, Any]

    # ─── 生命周期 ───

    def init_plugin(self, config: dict = None):
        """读取配置；勾选"立即运行一次"时安排一次执行。"""
        # 实例级状态：每次加载都是全新的，绝不与上一实例共享
        self._debounce_lock = threading.Lock()
        self._run_lock = threading.RLock()
        self._debounce_timer = None
        self._queue = []
        self._observers = []
        self._last_result = {}
        self._last_sync = {}
        self._mapping_retry_at = datetime.min
        self._mapping_source = ""
        self.stop_service()
        if config:
            self._enabled = bool(config.get("enabled"))
            self._notify = bool(config.get("notify"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._cron = config.get("cron")
            self._src_dir = str(config.get("src_dir") or "").strip()
            self._library_dir = str(config.get("library_dir") or "").strip()
            self._mode = config.get("mode") or "fast"
            self._transfer_type = config.get("transfer_type") or "link"
            self._scrape = bool(config.get("scrape", True))
            self._overwrite_mode = config.get("overwrite_mode") or "never"
            self._media_id = str(config.get("media_id") or "").strip()
            self._media_title = str(config.get("media_title") or "").strip()
            self._category_folder = bool(config.get("category_folder", True))
            try:
                self._grace = max(0, int(config.get("grace") or 30))
            except (TypeError, ValueError):
                self._grace = 30
            self._quarantine_dir = str(config.get("quarantine_dir") or "").strip()
            self._bootstrap = bool(config.get("bootstrap"))
            self._mapping_url = str(config.get("mapping_url") or "").strip() or \
                "https://cloud.sbsub.com/data/data.json"
            urls = [u.strip() for u in str(config.get("fallback_urls") or "").splitlines() if u.strip()]
            self._fallback_urls = urls or list(FALLBACK_MAPPING_URLS)
            try:
                self._fallback_cache_minutes = max(1, int(config.get("fallback_cache_minutes") or 360))
            except (TypeError, ValueError):
                self._fallback_cache_minutes = 360
            try:
                self._mapping_retry_minutes = max(1, int(config.get("mapping_retry_minutes") or 30))
            except (TypeError, ValueError):
                self._mapping_retry_minutes = 30
            self._extra_map = config.get("extra_map") or ""
            self._dry_run = bool(config.get("dry_run"))
            try:
                self._cache_minutes = max(1, int(config.get("cache_minutes") or 20))
            except (TypeError, ValueError):
                self._cache_minutes = 20
            self._legacy_dst_dir = str(config.get("legacy_dst_dir") or "").strip()
            # 识别词同步
            self._sync_enabled = bool(config.get("sync_enabled"))
            self._sync_onlyonce = bool(config.get("sync_onlyonce"))
            self._sync_cron = config.get("sync_cron")
            self._sync_sub_ids = self._normalize_sub_ids(config.get("sync_sub_ids"))
            self._sync_mode = config.get("sync_mode") or "range"
            self._sync_front = str(config.get("sync_front") or "").strip() or r"Conan[\s.]S01E"
            self._sync_back = str(config.get("sync_back") or "").strip() or r"[\s.]1996"
            self._sync_keep_others = bool(config.get("sync_keep_others", True))

        pending_jobs: List[Dict[str, Any]] = []
        if self._enabled and self._onlyonce:
            pending_jobs.append({"func": self.run_once, "name": "柯南全流程入库"})
        if self._enabled and self._sync_enabled and self._sync_onlyonce:
            pending_jobs.append({"func": self.sync_words, "name": "柯南识别词同步"})
        if pending_jobs:
            self._scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
            for job in pending_jobs:
                self._scheduler.add_job(func=job["func"], trigger="date",
                                        kwargs={"manual": False}, name=job["name"])
            self._scheduler.start()
            self._onlyonce = False
            self._sync_onlyonce = False
            self.__update_config()

        # fast 模式下启动实时监控
        if self._enabled and self._mode == "fast" and self._src_dir:
            self._start_watch()

    def _start_watch(self):
        """启动目录实时监控（fast 模式）。"""
        src = Path(self._src_dir)
        if not src.is_dir():
            logger.error(f"柯南全流程：监控目录不存在：{src}")
            return
        try:
            observer = PollingObserver(timeout=10) if self._mode == "compatibility" \
                else Observer(timeout=10)
            observer.schedule(_WatchHandler(str(src), self), path=str(src), recursive=True)
            observer.daemon = True
            observer.start()
            self._observers.append(observer)
            logger.info(f"柯南全流程：{src} 目录监控已启动（{self._mode}）")
        except Exception as e:
            err = str(e)
            logger.error(f"柯南全流程：目录监控启动失败：{err}")
            if "inotify" in err and "reached" in err:
                logger.warn("请提高 fs.inotify.max_user_watches 后重启 MoviePilot")

    def get_state(self) -> bool:
        return self._enabled

    def stop_service(self):
        """释放定时器与监控。"""
        lock = getattr(self, "_debounce_lock", None)
        if lock is not None:
            with lock:
                timer = getattr(self, "_debounce_timer", None)
                if timer:
                    timer.cancel()
                self._debounce_timer = None
        try:
            for observer in list(getattr(self, "_observers", []) or []):
                try:
                    observer.stop()
                    # 必须 join：watchdog 的 inotify 线程在 stop() 后仍需时间真正退出，
                    # 只 stop 不 join 会让重载后的新 observer 与旧线程并存，同一批文件被触发多次。
                    observer.join(timeout=5)
                except Exception as e:
                    logger.warning(f"柯南全流程：停止监控线程出错：{str(e)}")
            self._observers = []
        except Exception as e:
            logger.error(f"柯南全流程：停止监控出错：{str(e)}")
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"柯南全流程：停止服务出错：{str(e)}")

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时服务（扫描 + 识别词同步）。"""
        services: List[Dict[str, Any]] = []
        if not self._enabled:
            return services
        if self._cron:
            services.append({
                "id": "ConanRename",
                "name": "柯南全流程入库",
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
            "cron": self._cron, "src_dir": self._src_dir, "library_dir": self._library_dir,
            "mode": self._mode, "transfer_type": self._transfer_type, "scrape": self._scrape,
            "overwrite_mode": self._overwrite_mode, "media_id": self._media_id,
            "media_title": self._media_title, "category_folder": self._category_folder,
            "grace": self._grace, "quarantine_dir": self._quarantine_dir,
            "bootstrap": self._bootstrap,
            "mapping_url": self._mapping_url, "extra_map": self._extra_map,
            "fallback_urls": "\n".join(self._fallback_urls),
            "fallback_cache_minutes": self._fallback_cache_minutes,
            "mapping_retry_minutes": self._mapping_retry_minutes,
            "dry_run": self._dry_run, "cache_minutes": self._cache_minutes,
            "legacy_dst_dir": self._legacy_dst_dir,
            "sync_enabled": self._sync_enabled, "sync_cron": self._sync_cron,
            "sync_onlyonce": self._sync_onlyonce,
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
                    {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                        {"component": "VSwitch", "props": {"model": "dry_run", "label": "仅预演"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                        {"component": "VSwitch", "props": {"model": "onlyonce", "label": "保存后立即执行"}}]},
                ]},
                {"component": "VAlert", "props": {
                    "type": "info", "variant": "tonal", "class": "mb-2",
                    "text": "全流程：监听做种目录 → 按 sbsub 映射换算集号（映射没到时沿用最后一段偏移外推）"
                            "→ 硬链入库并刮削。源文件（做种）永不移动。"}},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "src_dir", "label": "做种目录（监听）",
                            "placeholder": "/你的盘/MOVIEPILOT/柯南/1柯南做种"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "library_dir", "label": "媒体库目录（入库目标）",
                            "placeholder": "/你的盘/MOVIEPILOT/LINK/TV"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSelect", "props": {
                            "model": "mode", "label": "监控模式",
                            "items": [
                                {"title": "fast（inotify 实时，推荐）", "value": "fast"},
                                {"title": "compatibility（轮询，兼容 SMB/NFS）", "value": "compatibility"},
                            ]}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSelect", "props": {
                            "model": "transfer_type", "label": "入库方式",
                            "items": [
                                {"title": "link（硬链，保留做种）", "value": "link"},
                                {"title": "copy（复制）", "value": "copy"},
                                {"title": "move（移动，⚠️ 会移走做种文件）", "value": "move"},
                            ]}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "cron", "label": "全量扫描定时（cron，留空=只靠实时）",
                            "placeholder": "*/15 * * * *"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "media_id", "label": "TMDB ID（留空则按标题自动识别）",
                            "placeholder": "30983"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "media_title", "label": "媒体标题（识别兜底用）",
                            "placeholder": "名侦探柯南"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                        {"component": "VSwitch", "props": {"model": "scrape", "label": "刮削"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 2}, "content": [
                        {"component": "VSwitch", "props": {"model": "category_folder", "label": "按类别建目录"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "mapping_url", "label": "映射数据地址（sbsub）"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "cache_minutes", "label": "映射缓存分钟数", "type": "number"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "grace", "label": "外推宽限窗口（集）", "type": "number"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextarea", "props": {
                            "model": "fallback_urls", "label": "后备映射地址（每行一个，按序尝试；留空用内置仓库快照）",
                            "rows": 2,
                            "placeholder": "https://cdn.jsdelivr.net/gh/用户/仓库@main/conan-mapping/mapping.json"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "fallback_cache_minutes", "label": "后备映射缓存分钟数",
                            "type": "number"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "mapping_retry_minutes", "label": "全源失败后重试间隔（分钟）",
                            "type": "number"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextarea", "props": {
                            "model": "extra_map", "label": "补充映射（每行：拆分版号=TMDB季集）",
                            "rows": 3, "placeholder": "1134=S0E32\n1262=S0E34"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "quarantine_dir", "label": "隔离目录（留空=媒体库下 _待人工）",
                            "placeholder": "/你的盘/MOVIEPILOT/柯南/_隔离"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VSwitch", "props": {
                            "model": "bootstrap",
                            "label": "首次启用：把做种目录里已有的历史文件一次性入账（只为防重复入库，第二次运行前请关闭）"}}]},
                ]},
                {"component": "VAlert", "props": {
                    "type": "warning", "variant": "tonal", "class": "mb-2",
                    "text": "隔离目录只放「真的判定不了」的文件（集号解析失败/超出外推窗口），"
                            "不会自动入库，也不应加入任何目录监控。"}},
                {"component": "VDivider", "props": {"class": "my-4"}},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "sync_enabled", "label": "启用识别词同步"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "sync_keep_others", "label": "保留订阅里其它识别词"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "sync_onlyonce", "label": "保存后立即同步一次"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "sync_cron", "label": "同步定时（cron，留空=只手动）",
                            "placeholder": "0 2 * * *"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VSelect", "props": {
                            "model": "sync_mode", "label": "识别词写法",
                            "items": [
                                {"title": "每个偏移区间一条（推荐）", "value": "range"},
                                {"title": "只写最新偏移一条（旧区间不管）", "value": "tail"},
                            ]}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VSelect", "props": {
                            "model": "sync_sub_ids", "label": "目标订阅（可多选）",
                            "multiple": True, "chips": True, "clearable": True,
                            "items": self._subscribe_items()}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "sync_front", "label": "前定位词（正则）",
                            "placeholder": "Conan[\\s.]S01E"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "sync_back", "label": "后定位词（正则）",
                            "placeholder": "[\\s.]1996"}}]},
                ]},
            ]}
        ], {
            "enabled": self._enabled, "notify": self._notify, "dry_run": self._dry_run,
            "onlyonce": self._onlyonce,
            "src_dir": self._src_dir, "library_dir": self._library_dir,
            "mode": self._mode, "transfer_type": self._transfer_type,
            "scrape": self._scrape, "category_folder": self._category_folder,
            "overwrite_mode": self._overwrite_mode,
            "media_id": self._media_id, "media_title": self._media_title,
            "grace": self._grace, "quarantine_dir": self._quarantine_dir,
            "bootstrap": self._bootstrap,
            "cron": self._cron, "mapping_url": self._mapping_url,
            "fallback_urls": "\n".join(self._fallback_urls),
            "cache_minutes": self._cache_minutes, "extra_map": self._extra_map,
            "legacy_dst_dir": self._legacy_dst_dir,
            "sync_enabled": self._sync_enabled, "sync_cron": self._sync_cron,
            "sync_onlyonce": self._sync_onlyonce,
            "sync_sub_ids": self._sync_sub_ids, "sync_mode": self._sync_mode,
            "sync_front": self._sync_front, "sync_back": self._sync_back,
            "sync_keep_others": self._sync_keep_others,
        }

    def get_page(self) -> List[dict]:
        """页面：上次运行 + 账本 + 映射 + 识别词同步结果。"""
        content: List[dict] = []

        result = self._last_result or {}
        if result:
            content.append({"component": "VAlert", "props": {
                "type": "warning" if (result.get("errors") or result.get("quarantined")) else "success",
                "variant": "tonal", "class": "mb-2",
                "text": f"上次运行 {result.get('time')}：扫描 {result.get('scanned')}｜"
                        f"入库 {result.get('imported')}｜外推 {result.get('extrapolated')}｜"
                        f"跳过 {result.get('skipped')}｜隔离 {result.get('quarantined')}｜"
                        f"纠正 {result.get('corrected')}｜错误 {len(result.get('errors') or [])}"}})
        else:
            content.append({"component": "VAlert", "props": {
                "type": "info", "variant": "tonal", "class": "mb-2", "text": "尚未运行过"}})

        sync = self._last_sync or {}
        if sync:
            detail = "｜".join(sync.get("detail") or []) or "无变化"
            content.append({"component": "VAlert", "props": {
                "type": "warning" if sync.get("errors") else "success",
                "variant": "tonal", "class": "mb-2",
                "text": f"上次识别词同步 {sync.get('time')}：规则 {sync.get('rules')} 条｜"
                        f"更新 {sync.get('changed')} 个订阅｜不变 {sync.get('unchanged')} 个"
                        + ("（预演）" if sync.get("dry_run") else "") + f"｜{detail}"}})

        # 映射概览
        try:
            data = self._load_mapping_data()
            mapping = self._build_mapping(data) if data else {}
        except Exception as e:
            logger.error(f"柯南全流程：页面读取映射失败：{str(e)}")
            mapping = {}
        if not mapping:
            content.append({"component": "VAlert", "props": {
                "type": "error", "variant": "tonal", "text": "映射数据不可用（请检查网络或映射数据地址）"}})
            return content

        ranges = self._compute_ranges(mapping)
        tail = ranges[-1]
        content.append({"component": "VAlert", "props": {
            "type": "info", "variant": "tonal", "class": "mb-2",
            "text": f"当前映射 {len(mapping)} 条（拆分版 {min(mapping)}–{max(mapping)}）"
                    f"｜来源 {self._mapping_source or '未记录'}｜"
                    f"偏移区间 {len(ranges)} 段｜最新偏移 {tail['offset']:+d}"
                    f"（拆分 {tail['start']}–{tail['end']} → TMDB {tail['tmdb_start']}–{tail['tmdb_end']}）｜"
                    f"外推上界 拆分 {max(mapping) + self._grace}"}})

        # 账本（最近 50 条）
        rows = []
        for src, item in sorted((self._load_ledger() or {}).items(),
                                key=lambda kv: kv[1].get("updated_at") or "", reverse=True)[:50]:
            rows.append({
                "file": Path(src).name,
                "split": item.get("split"),
                "tmdb": item.get("tmdb"),
                "source": {"exact": "精确", "extrapolated": "外推", "extra": "补充"}.get(
                    item.get("source"), item.get("source")),
                "stage": item.get("stage"),
                "lib": Path(item.get("lib_path") or "").name,
                "when": item.get("updated_at"),
            })
        if rows:
            content.append({"component": "VDataTableVirtual", "props": {
                "class": "text-sm",
                "headers": [
                    {"title": "文件", "key": "file"},
                    {"title": "拆分版", "key": "split"},
                    {"title": "TMDB", "key": "tmdb"},
                    {"title": "来源", "key": "source"},
                    {"title": "阶段", "key": "stage"},
                    {"title": "库内文件", "key": "lib"},
                    {"title": "更新时间", "key": "when"},
                ],
                "items": rows, "height": "min(50vh, 28rem)", "density": "compact",
                "fixed-header": True, "hide-no-data": True, "hover": True,
                "items-per-page": -1}})
        return content

    def get_command(self) -> List[Dict[str, Any]]:
        return [{"cmd": "/conan_rename", "event": EventType.PluginAction,
                 "desc": "柯南全流程入库（立即执行）", "category": "管理",
                 "data": {"action": "conan_rename"}},
                {"cmd": "/conan_sync_words", "event": EventType.PluginAction,
                 "desc": "柯南识别词同步", "category": "管理",
                 "data": {"action": "conan_sync_words"}}]

    def get_api(self) -> List[Dict[str, Any]]:
        return [{"path": "/conan_rename", "endpoint": self.api_run, "methods": ["GET"],
                 "summary": "立即执行一次柯南全流程入库"},
                {"path": "/conan_sync_words", "endpoint": self.api_sync_words, "methods": ["GET"],
                 "summary": "立即同步一次识别词到订阅"},
                {"path": "/conan_words_preview", "endpoint": self.api_words_preview, "methods": ["GET"],
                 "summary": "预览将要写入的识别词（不写库）"},
                {"path": "/conan_ledger", "endpoint": self.api_ledger, "methods": ["GET"],
                 "summary": "查看入库账本"},
                {"path": "/conan_drop_note", "endpoint": self.api_drop_note, "methods": ["GET"],
                 "summary": "从订阅 note 中移除指定集号（参数 sids=405&episodes=1279,1281）"}]

    @eventmanager.register(EventType.PluginAction)
    def remote_run(self, event: Event):
        if not event or not event.event_data:
            return
        action = event.event_data.get("action")
        if action == "conan_rename":
            result = self.run_once(manual=True)
            title = f"柯南全流程：入库 {result.get('imported')} 个"
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

    def api_ledger(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        return schemas.Response(success=True, data=self._load_ledger())

    def api_drop_note(self, apikey: str, sids: str = "", episodes: str = ""):
        """
        从订阅的 note（已下载集号）中移除指定集号。

        用于修复历史误入库：某些集号从未真正入库，却因早期流水线的错误被记成"已下载"，
        不清掉会让 MP 永远不再搜索这些集。只删不增，且不会触碰 episode_priority。
        """
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        try:
            drop = {int(x) for x in str(episodes).replace(" ", "").split(",") if x}
            target_sids = [int(x) for x in str(sids).replace(" ", "").split(",") if x]
        except ValueError:
            return schemas.Response(success=False, message="参数格式错误")
        if not drop or not target_sids:
            return schemas.Response(success=False, message="需要 sids 与 episodes 参数")

        from app.db.oper.subscribe import SubscribeOper
        oper = SubscribeOper()
        result = {"sids": {}, "dropped": sorted(drop)}
        for sid in target_sids:
            sub = oper.get(sid)
            if not sub:
                result["sids"][sid] = "订阅不存在"
                continue
            old = [int(x) for x in (sub.note or []) if str(x).isdigit()]
            new = [x for x in old if x not in drop]
            if new == old:
                result["sids"][sid] = f"无需修改（note={old}）"
                continue
            if self._dry_run:
                result["sids"][sid] = f"[预演] {old} -> {new}"
                continue
            oper.update(sid, {"note": new})
            result["sids"][sid] = f"{old} -> {new}"
            logger.info(f"柯南全流程：订阅 {sid} 的 note 已移除 {sorted(set(old) - set(new))}，"
                        f"现为 {new}")
        return schemas.Response(success=True, data=result)

    # ─── 映射数据 ───

    def _cache_file(self) -> Path:
        return self.get_data_path() / "sbsub-data.json"

    def _fallback_cache_file(self) -> Path:
        return self.get_data_path() / "mapping-fallback.json"

    @staticmethod
    def _read_json(path: Path) -> Optional[dict]:
        """读 JSON 文件，任何异常（不存在/损坏）都当没有。"""
        try:
            if path.exists():
                data = json.loads(path.read_text(encoding="utf-8"))
                return data if isinstance(data, dict) else None
        except Exception as e:
            logger.warning(f"读取 {path.name} 失败：{str(e)}")
        return None

    @staticmethod
    def _write_json(path: Path, data: dict) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            logger.warning(f"写入 {path.name} 失败：{str(e)}")

    @staticmethod
    def _file_age_minutes(path: Path) -> Optional[float]:
        try:
            return (datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)).total_seconds() / 60
        except Exception:
            return None

    def _mapping_score(self, data: Optional[dict]) -> int:
        """数据完整度评分：映射覆盖到的最大拆分版号。取新不取旧时用它比较。"""
        if not data:
            return 0
        try:
            mapping = self._build_mapping(data)
        except Exception:
            return 0
        return max(mapping) if mapping else 0

    def _better(self, *datas: Optional[dict]) -> Optional[dict]:
        """从多份候选数据里挑覆盖最全的一份（映射只增不减，集号越大越新）。"""
        best, best_score = None, 0
        for data in datas:
            score = self._mapping_score(data)
            if score > best_score:
                best, best_score = data, score
        return best

    def _mapping_fallback_urls(self) -> List[str]:
        return [u for u in self._fallback_urls if u]

    def _fetch_primary(self) -> Optional[dict]:
        """主源（sbsub 实时）。这是权威数据，成功即用，不与其他源比新旧。"""
        try:
            resp = requests.get(self._mapping_url, timeout=30,
                                headers={"User-Agent": "Mozilla/5.0 MoviePilot-Plugin/ConanRename"})
            resp.raise_for_status()
            data = resp.json()
            if not self._mapping_score(data):
                logger.error("主源映射数据结构不认识，按失败处理")
                return None
            self._write_json(self._cache_file(), data)
            logger.info("映射数据已更新")
            return data
        except Exception as e:
            logger.error(f"获取映射数据失败：{str(e)}")
            return None

    def _load_fallback_data(self, force: bool = False) -> Optional[dict]:
        """仓库快照（GitHub，定时同步自 sbsub）：多镜像按序尝试，带本地副本。"""
        cache = self._fallback_cache_file()
        cached = self._read_json(cache)
        if not force and cached is not None:
            age = self._file_age_minutes(cache)
            if age is not None and age < self._fallback_cache_minutes:
                return cached
        for url in self._mapping_fallback_urls():
            try:
                resp = requests.get(url, timeout=15,
                                    headers={"User-Agent": "Mozilla/5.0 MoviePilot-Plugin/ConanRename"})
                resp.raise_for_status()
                data = resp.json()
                if not self._mapping_from_slim(data):
                    logger.warning(f"仓库快照结构不认识，跳过：{url}")
                    continue
                # 快照可能比本地副本旧（CDN 缓存），取新的那份
                best = self._better(cached, data)
                self._write_json(cache, best)
                logger.warning(f"主源不可用，改用仓库快照映射：{url}")
                return best
            except Exception as e:
                logger.warning(f"仓库快照获取失败（{url}）：{str(e)[:80]}")
        return cached

    def _load_mapping_data(self) -> Optional[dict]:
        """
        分层取映射：未过期缓存 → 主源(sbsub) → 仓库快照 → 旧缓存/旧快照副本。

        全部失败时按 _mapping_retry_minutes 退避，避免 60s 轮询反复卡超时。
        """
        cache = self._cache_file()
        cached = self._read_json(cache)
        if cached is not None and self._cache_minutes > 0:
            age = self._file_age_minutes(cache)
            if age is not None and age < self._cache_minutes:
                self._mapping_source = "缓存"
                return cached

        if datetime.now() < self._mapping_retry_at:
            # 退避窗口内不再打网络，直接用现有副本
            best = self._better(cached, self._read_json(self._fallback_cache_file()))
            self._mapping_source = "本地副本（退避中）" if best else "不可用"
            return best

        data = self._fetch_primary()
        if data is not None:
            self._mapping_source = "sbsub 实时"
            self._mapping_retry_at = datetime.min
            return data

        fallback = self._load_fallback_data()
        best = self._better(cached, fallback)
        # 主源失败即进入退避：即使手里有过期副本，也不能让每轮防抖都去等
        # 主源 30s + 快照 15s×N 的超时，否则线程会堆积。
        self._mapping_retry_at = datetime.now() + timedelta(minutes=self._mapping_retry_minutes)
        if best is not None:
            self._mapping_source = "仓库快照" if best is fallback else "过期缓存"
            logger.warning(f"主源失败，映射改用{self._mapping_source}（覆盖至拆分版 "
                           f"{self._mapping_score(best)}），{self._mapping_retry_minutes} 分钟后再试主源")
            return best

        self._mapping_source = "不可用"
        logger.error(f"所有映射数据源均不可用，{self._mapping_retry_minutes} 分钟后重试")
        return None

    def _force_refresh_mapping(self) -> Optional[dict]:
        """遇到未映射集号时强制拉一次（限流：同一轮只拉一次，失败退回缓存/快照）。"""
        self._mapping_retry_at = datetime.min
        data = self._fetch_primary()
        if data is not None:
            self._mapping_source = "sbsub 实时"
            logger.info("柯南全流程：因存在未映射集号，已强制刷新映射数据")
            return data
        fallback = self._load_fallback_data(force=True)
        best = self._better(fallback, self._read_json(self._cache_file()),
                            self._read_json(self._fallback_cache_file()))
        self._mapping_retry_at = datetime.now() + timedelta(minutes=self._mapping_retry_minutes)
        if best is not None:
            self._mapping_source = "仓库快照" if best is fallback else "缓存"
            logger.warning(f"柯南全流程：强制刷新主源失败，改用{self._mapping_source}")
            return best
        logger.warning("柯南全流程：强制刷新失败且无可用副本")
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

    @staticmethod
    def _is_slim(data: Optional[dict]) -> bool:
        """仓库快照格式：{"source","version","mapping":{"拆分版号": TMDB 号}}。"""
        return bool(data) and isinstance(data.get("mapping"), dict)

    def _mapping_from_slim(self, data: Optional[dict]) -> Dict[int, int]:
        """解析仓库快照的 mapping 字段（键值都必须是正整数）。"""
        mapping: Dict[int, int] = {}
        if not self._is_slim(data):
            return mapping
        for key, value in data["mapping"].items():
            try:
                split, tmdb = int(key), int(value)
            except (TypeError, ValueError):
                continue
            if split > 0 and tmdb > 0:
                mapping.setdefault(split, tmdb)
        return mapping

    def _build_mapping(self, data: dict) -> Dict[int, int]:
        """拆分版集号 → TMDB 集号（sbsub 列表的键即为 TMDB 集号）。"""
        if not data:
            return {}
        if self._is_slim(data):
            return self._mapping_from_slim(data)
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
        front = self._sync_front or r"Conan[\s.]S01E"
        back = self._sync_back or r"[\s.]1996"
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
        """把规则写进插件维护的注释区块，保留订阅里其它识别词。"""
        block = [SYNC_MARK_BEGIN, *rules, SYNC_MARK_END]
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
            # 丢掉旧版手工写的柯南同步规则，避免与新区间规则抢命中。
            # 注意：识别词行里的前定位词本身是「正则文本」，不能拿它当正则去匹配这一行
            # （否则 Conan[\s.]S01E 永远匹配不上字面量 "Conan[\s.]S01E <> …"）。
            if "<>" in stripped and ">>" in stripped and "Conan" in stripped:
                try:
                    if re.match(r"Conan", stripped):
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

    # ─── 集号解析（2.0 核心） ───

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

    def _resolve_episode(self, split_no: int, mapping: Dict[int, int],
                         extra: Dict[int, str]) -> Tuple[Optional[int], Optional[int], str]:
        """
        拆分版集号 → (季, TMDB 集号)。

        :return: (season, episode, 来源)。来源取值 exact / extra / extrapolated / none
        """
        if split_no in extra:
            tag = extra[split_no]
            match = re.search(r"S(\d+)E(\d+)", tag, re.I)
            if match:
                return int(match.group(1)), int(match.group(2)), "extra"
        if split_no in mapping:
            return 1, mapping[split_no], "exact"
        if not mapping:
            return None, None, "none"
        # tail 偏移外推：与订阅识别词用的是同一段偏移，保证"下载算出的集号 == 入库算出的集号"
        ranges = self._compute_ranges(mapping)
        tail = ranges[-1]
        max_split = max(mapping)
        if split_no <= max_split + self._grace:
            return 1, split_no + tail["offset"], "extrapolated"
        return None, None, "none"

    def _mapping_signature(self, mapping: Dict[int, int]) -> str:
        """映射指纹：用于判断外推结论是否被新映射推翻。"""
        if not mapping:
            return ""
        ranges = self._compute_ranges(mapping)
        tail = ranges[-1]
        return f"{max(mapping)}:{tail['offset']}"

    # ─── 账本 ───

    def _ledger_path(self) -> Path:
        return self.get_data_path() / LEDGER_FILE

    def _load_ledger(self) -> Dict[str, dict]:
        path = self._ledger_path()
        try:
            if path.exists():
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
        except Exception as e:
            logger.error(f"读取入库账本失败：{str(e)}")
        return {}

    def _save_ledger(self, ledger: Dict[str, dict]) -> None:
        path = self._ledger_path()
        try:
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(ledger, ensure_ascii=False, indent=1), encoding="utf-8")
            os.replace(tmp, path)
        except Exception as e:
            logger.error(f"写入库账本失败：{str(e)}")

    @staticmethod
    def _fingerprint(path: Path) -> Optional[dict]:
        """文件指纹：大小 + 修改时间 + inode（重下/替换会自动失效）。"""
        try:
            st = path.stat()
        except OSError:
            return None
        return {"size": st.st_size, "mtime": int(st.st_mtime), "ino": st.st_ino}

    # ─── 实时队列 ───

    def enqueue_path(self, path: Path) -> None:
        """实时监控回调：放入待处理队列，并安排一次防抖执行。"""
        try:
            if not path.is_file():
                return
            if path.suffix.lower() not in MEDIA_EXT:
                return
            if path.name.startswith(".") or path.name.endswith(".part"):
                return
            if any(p.startswith(".") or p.startswith("@") for p in path.parts):
                return
            if path not in self._queue:
                self._queue.append(path)
            logger.info(f"柯南全流程：监控收到文件 {path.name}（队列 {len(self._queue)}）")
            self._schedule_debounced_run()
        except Exception as e:
            logger.error(f"柯南全流程：入队失败：{str(e)}")

    def _schedule_debounced_run(self) -> None:
        """下载中的文件大小会持续变化，静默 60 秒后再处理。"""
        with self._debounce_lock:
            if self._debounce_timer:
                self._debounce_timer.cancel()
            timer = threading.Timer(60.0, self._debounced_run)
            timer.daemon = True
            timer.start()
            self._debounce_timer = timer

    def _debounced_run(self) -> None:
        """防抖触发点；串行化由 run_once 内部的锁负责，这里不能重复取锁。"""
        try:
            self.run_once(manual=False)
        except Exception as e:
            logger.error(f"柯南全流程：实时处理失败：{str(e)}")

    # ─── 主流程 ───

    def run_once(self, manual: bool = False) -> Dict[str, Any]:
        """扫描 → 换算集号 → 硬链入库 → 核对账本。"""
        with self._run_lock:
            return self._run_once_locked(manual)

    def _run_once_locked(self, manual: bool) -> Dict[str, Any]:
        """主流程实体；由 run_once 持锁调用。"""
        started = datetime.now()
        result: Dict[str, Any] = {
            "time": started.strftime("%Y-%m-%d %H:%M:%S"), "manual": manual,
            "dry_run": self._dry_run, "scanned": 0, "imported": 0, "extrapolated": 0,
            "skipped": 0, "quarantined": 0, "corrected": 0, "errors": [], "detail": [],
        }
        if not self._src_dir:
            result["errors"].append("未配置做种目录")
            self._last_result = result
            return result
        src_root = Path(self._src_dir)
        if not src_root.is_dir():
            result["errors"].append(f"做种目录不存在：{src_root}")
            self._last_result = result
            return result
        if not self._library_dir:
            result["errors"].append("未配置媒体库目录")
            self._last_result = result
            return result

        data = self._load_mapping_data()
        mapping = self._build_mapping(data) if data else {}
        if not mapping:
            result["errors"].append("映射数据获取失败")
            self._last_result = result
            return result
        extra = self._load_extra_map()
        ledger = self._load_ledger()

        # 1. 收集候选：实时队列 + 递归扫描
        candidates: List[Path] = []
        seen: set = set()
        for path in list(self._queue) + sorted(src_root.rglob("*")):
            try:
                if not path.is_file() or path.suffix.lower() not in MEDIA_EXT:
                    continue
                if any(p.startswith(".") or p.startswith("@") for p in path.parts):
                    continue
                if path.name.endswith(".part") or path.name.startswith("."):
                    continue
            except OSError:
                continue
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(path)
        self._queue = []
        result["scanned"] = len(candidates)

        # 1.5 首次启用：把历史文件直接写进账本（stage=legacy），只登记不入库，
        #     避免插件上线后把 1280 个已入库的老文件重跑一遍。
        if self._bootstrap:
            bootstrapped = 0
            for path in candidates:
                key = str(path)
                if key in ledger:
                    continue
                fp = self._fingerprint(path)
                if not fp:
                    continue
                split_no = self._detect_episode(path.stem)
                season, tmdb, source = self._resolve_episode(split_no, mapping, extra) \
                    if split_no is not None else (None, None, "none")
                ledger[key] = {
                    **fp, "split": split_no, "season": season, "tmdb": tmdb, "source": source,
                    "stage": "legacy", "signature": self._mapping_signature(mapping),
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
                bootstrapped += 1
            self._save_ledger(ledger)
            result["detail"].append(f"首次启用：已登记 {bootstrapped} 个历史文件（不入库）")
            logger.info(f"柯南全流程：首次启用登记 {bootstrapped} 个历史文件")
            self._bootstrap = False
            self.__update_config()
            self._last_result = result
            return result

        # 2. 解析集号（遇到未映射先强制刷新一次）
        plans: List[Tuple[Path, int, int, int, str]] = []
        unresolved: List[Tuple[Path, int]] = []
        for path in candidates:
            split_no = self._detect_episode(path.stem)
            if split_no is None:
                self._quarantine(path, "无法识别集号", result)
                continue
            season, tmdb, source = self._resolve_episode(split_no, mapping, extra)
            if tmdb is None:
                unresolved.append((path, split_no))
                continue
            plans.append((path, split_no, season, tmdb, source))

        if unresolved:
            refreshed = self._force_refresh_mapping()
            if refreshed:
                mapping = self._build_mapping(refreshed)
                still: List[Tuple[Path, int]] = []
                for path, split_no in unresolved:
                    season, tmdb, source = self._resolve_episode(split_no, mapping, extra)
                    if tmdb is None:
                        still.append((path, split_no))
                    else:
                        plans.append((path, split_no, season, tmdb, source))
                unresolved = still
            for path, split_no in unresolved:
                self._quarantine(path, f"映射缺失且超出外推窗口（拆分版 {split_no}）", result)

        result["extrapolated"] = sum(1 for *_, s in plans if s == "extrapolated")
        signature = self._mapping_signature(mapping)

        # 3. 入库
        for path, split_no, season, tmdb, source in plans:
            try:
                self._import_one(path, split_no, season, tmdb, source, signature, ledger, result)
            except Exception as e:
                result["errors"].append(f"{path.name} 处理失败：{str(e)[:120]}")
                logger.error(f"柯南全流程：{path.name} 处理失败：{str(e)}")

        # 4. 事后自纠：映射补全后核对旧的外推结论
        try:
            self._verify_extrapolations(mapping, extra, ledger, result)
        except Exception as e:
            result["errors"].append(f"账本核对失败：{str(e)[:120]}")

        self._save_ledger(ledger)
        self._last_result = result
        self._cleanup_empty_dirs(src_root)
        logger.info(
            f"柯南全流程完成：扫描 {result['scanned']}｜入库 {result['imported']}｜"
            f"外推 {result['extrapolated']}｜跳过 {result['skipped']}｜"
            f"隔离 {result['quarantined']}｜纠正 {result['corrected']}｜错误 {len(result['errors'])}")
        for err in result["errors"][:5]:
            logger.warning(err)
        if self._notify and (result["imported"] or result["errors"] or result["quarantined"]):
            self.post_message(
                mtype=NotificationType.Manual,
                title=f"柯南全流程：入库 {result['imported']} 个"
                      + ("（预演）" if self._dry_run else ""),
                text=f"扫描 {result['scanned']}｜外推 {result['extrapolated']}｜"
                     f"隔离 {result['quarantined']}｜纠正 {result['corrected']}｜"
                     f"错误 {len(result['errors'])}",
            )
        return result

    def _import_one(self, path: Path, split_no: int, season: int, tmdb: int, source: str,
                    signature: str, ledger: Dict[str, dict], result: Dict[str, Any]) -> None:
        """单个文件：判重 → 构造 meta → 交给 TransferChain 入库。"""
        fp = self._fingerprint(path)
        if not fp:
            result["skipped"] += 1
            return
        record = ledger.get(str(path))
        if record and record.get("stage") in ("imported", "legacy") \
                and record.get("size") == fp["size"] and record.get("mtime") == fp["mtime"]:
            result["skipped"] += 1
            return

        if self._dry_run:
            result["detail"].append(
                f"[预演] {path.name} → S{season:02d}E{tmdb:04d}（{source}）")
            result["imported"] += 1
            return

        from app.chain.storage import StorageChain
        from app.chain.transfer import TransferChain
        from app.schemas import TransferDirectoryConf
        from app.schemas.types import MediaType
        from app.sdk.media import MetaInfoPath

        # 构造最终 meta：用拆分版文件名解析，再把集号换成 TMDB 号
        file_item = StorageChain().get_file_item(storage="local", path=path)
        if not file_item:
            result["errors"].append(f"{path.name} 未找到文件项")
            return

        file_meta = MetaInfoPath(path)
        if not file_meta:
            result["errors"].append(f"{path.name} 无法解析元数据")
            return
        file_meta.begin_season = season
        file_meta.begin_episode = tmdb
        file_meta.end_episode = None

        # 识别媒体：优先用配置的 TMDB ID，避免标题识别抖动
        chain = TransferChain()
        if self._media_id.isdigit():
            mediainfo = chain.recognize_media(
                media_source="themoviedb", media_id=self._media_id,
                mtype=MediaType.TV)
        else:
            probe = MetaInfoPath(path)
            if self._media_title:
                probe.cn_name = self._media_title
                probe.name = self._media_title
            mediainfo = chain.recognize_media(meta=probe)
        if not mediainfo:
            result["errors"].append(f"{path.name} 未识别到媒体信息")
            return

        episodes_info = None
        if mediainfo.tmdb_id:
            try:
                from app.chain.tmdb import TmdbChain
                episodes_info = TmdbChain().tmdb_episodes(
                    tmdbid=int(mediainfo.tmdb_id), season=season)
            except Exception as e:
                logger.warning(f"柯南全流程：获取 TMDB 集信息失败：{str(e)}")

        target_dir = TransferDirectoryConf()
        target_dir.library_path = self._library_dir
        target_dir.library_storage = "local"
        target_dir.transfer_type = self._transfer_type
        target_dir.scraping = self._scrape
        target_dir.renaming = True
        target_dir.notify = False
        target_dir.overwrite_mode = self._overwrite_mode
        target_dir.library_category_folder = self._category_folder

        transferinfo = chain.transfer(
            fileitem=file_item, meta=file_meta, mediainfo=mediainfo,
            target_directory=target_dir, episodes_info=episodes_info)

        if not transferinfo:
            result["errors"].append(f"{path.name} 整理模块无返回")
            return
        if not transferinfo.success:
            if getattr(transferinfo, "overwrite_skipped", False):
                result["skipped"] += 1
                ledger[str(path)] = {
                    **fp, "split": split_no, "season": season, "tmdb": tmdb, "source": source,
                    "stage": "skipped", "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
                return
            result["errors"].append(f"{path.name} 入库失败：{transferinfo.message}")
            return

        lib_path = str(transferinfo.target_item.path) if transferinfo.target_item else ""
        ledger[str(path)] = {
            **fp, "split": split_no, "season": season, "tmdb": tmdb, "source": source,
            "stage": "imported", "lib_path": lib_path,
            "signature": signature,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        result["imported"] += 1
        result["detail"].append(f"{path.name} → S{season:02d}E{tmdb:04d}（{source}）")
        logger.info(f"柯南全流程：{path.name} → {lib_path or f'S{season:02d}E{tmdb:04d}'}（{source}）")

    def _quarantine(self, path: Path, reason: str, result: Dict[str, Any]) -> None:
        """把判定不了的文件移到隔离目录（只在配置了隔离目录时才移动）。"""
        result["quarantined"] += 1
        result["detail"].append(f"隔离（{reason}）：{path.name}")
        logger.warning(f"柯南全流程：隔离 {path.name}，原因：{reason}")
        if self._dry_run:
            return
        target_root = Path(self._quarantine_dir) if self._quarantine_dir else \
            Path(self._library_dir) / "_待人工"
        try:
            target_root.mkdir(parents=True, exist_ok=True)
            target = target_root / path.name
            if target.exists():
                return
            os.link(path, target)
        except Exception as e:
            result["errors"].append(f"{path.name} 隔离失败：{str(e)[:80]}")

    def _verify_extrapolations(self, mapping: Dict[int, int], extra: Dict[int, str],
                               ledger: Dict[str, dict], result: Dict[str, Any]) -> None:
        """映射补全后，核对之前的外推结论；不一致就把库内文件改名到正确集号。"""
        for src, item in list(ledger.items()):
            if item.get("stage") != "imported" or item.get("source") != "extrapolated":
                continue
            split_no = item.get("split")
            if not isinstance(split_no, int):
                continue
            season, tmdb, source = self._resolve_episode(split_no, mapping, extra)
            if source == "extrapolated" or tmdb is None or tmdb == item.get("tmdb"):
                continue
            season = season or 1
            # 映射已补全且与外推不一致 → 纠正库内文件
            old_tmdb = item.get("tmdb")
            lib_path = item.get("lib_path")
            if not lib_path:
                continue
            old_file = Path(lib_path)
            if not old_file.exists():
                continue
            # 重命名模板除了 {{season_episode}} 还可能写 {{episode}}（如
            # 「… - S01E1222 - 第 1222 集.mp4」），两处集号都要换掉。
            new_name = EP_TOKEN.sub(f"S{season:02d}E{tmdb:04d}", old_file.name, count=1)
            if old_tmdb:
                new_name = re.sub(rf"(?<!\d){old_tmdb:04d}(?!\d)",
                                  f"{tmdb:04d}", new_name)
            new_file = old_file.with_name(new_name)
            if new_file == old_file or new_file.exists():
                continue
            if self._dry_run:
                result["detail"].append(f"[预演] 纠正 {old_file.name} → {new_name}")
                result["corrected"] += 1
                continue
            try:
                os.rename(old_file, new_file)
                # 同名 nfo/jpg/png 一起改
                for suffix in (".nfo", ".jpg", ".png", "-thumb.jpg"):
                    old_side = old_file.with_name(old_file.stem + suffix)
                    if old_side.exists():
                        os.rename(old_side, new_file.with_name(new_file.stem + suffix))
                item.update({"season": season, "tmdb": tmdb, "source": source,
                             "lib_path": str(new_file),
                             "updated_at": datetime.now().isoformat(timespec="seconds")})
                result["corrected"] += 1
                result["detail"].append(
                    f"纠正 E{old_tmdb:04d} → S{season:02d}E{tmdb:04d}：{new_file.name}")
                logger.info(f"柯南全流程：外推纠正 E{old_tmdb:04d} → S{season:02d}E{tmdb:04d}（{new_file.name}）")
            except Exception as e:
                result["errors"].append(f"纠正 {old_file.name} 失败：{str(e)[:80]}")

    @staticmethod
    def _cleanup_empty_dirs(root: Path) -> None:
        """清理做种目录下的空子目录（不动非空目录）。"""
        try:
            for path in sorted(root.rglob("*"), reverse=True):
                if path.is_dir() and not any(path.iterdir()):
                    path.rmdir()
        except Exception:
            pass
