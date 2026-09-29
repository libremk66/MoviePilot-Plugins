# coding=UTF-8
"""
柯南集数映射重命名（ConanRename）

按「银色子弹数据站」(sbsub) 的权威映射，把**拆分版集号**改名为 **TMDB 集号**，
解决"固定偏移（如 -60）在同一批次里也会变化"导致的错名问题。

典型流水线：
    订阅 → 1柯南做种 →【实时硬链接】→ 2柯南硬链 →【本插件】→ 3柯南重命名 →【目录实时监控】→ 入库刮削

安全约定：
    · 目标文件已存在时**跳过并告警**，绝不覆盖
    · 映射里查不到的集号 → 移到目标目录下的 `_待人工/`，不猜、不改
    · 支持「仅预演」，先看清单再实际执行
    · 每次实际改名都会写清单（插件数据目录 rename-YYYYMMDD.jsonl），可人工回滚
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


class ConanRename(_PluginBase):
    # 插件名称
    plugin_name = "柯南集数映射重命名"
    # 插件描述
    plugin_desc = (
        "按银色子弹数据站(sbsub)的权威映射，把拆分版集号改名为 TMDB 集号"
        "（自动处理 -partN 多段与特辑），解决固定偏移会漂移的问题。"
    )
    # 插件图标
    plugin_icon = "Linkace_C.png"
    # 插件版本
    plugin_version = "1.0.0"
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
    # 上次运行结果（供页面展示）
    _last_result: Dict[str, Any] = {}

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

        if self._enabled and self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
            self._scheduler.add_job(func=self.run_once, trigger="date",
                                    kwargs={"manual": False}, name="柯南映射重命名")
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
        """注册定时服务。"""
        if self._enabled and self._cron:
            return [{
                "id": "ConanRename",
                "name": "柯南集数映射重命名",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.run_once,
                "kwargs": {"manual": False},
            }]
        return []

    # ─── 配置与页面 ───

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled, "notify": self._notify, "onlyonce": self._onlyonce,
            "cron": self._cron, "src_dir": self._src_dir, "dst_dir": self._dst_dir,
            "mode": self._mode, "mapping_url": self._mapping_url,
            "extra_map": self._extra_map, "dry_run": self._dry_run,
            "cache_hours": self._cache_hours,
        })

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {"component": "VForm", "content": [
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "dry_run", "label": "仅预演（不实际改名）"}}]},
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
                            "model": "cron", "label": "定时（cron，留空=只手动）",
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
            ]}
        ], {
            "enabled": self._enabled, "notify": self._notify, "dry_run": self._dry_run,
            "src_dir": self._src_dir, "dst_dir": self._dst_dir, "mode": self._mode,
            "cron": self._cron, "mapping_url": self._mapping_url,
            "cache_hours": self._cache_hours, "extra_map": self._extra_map,
        }

    def get_page(self) -> List[dict]:
        result = self._last_result or {}
        if not result:
            return [{"component": "VAlert",
                     "props": {"type": "info", "variant": "tonal", "text": "尚未运行过"}}]
        return [{"component": "VAlert", "props": {
            "type": "warning" if (result.get("errors") or result.get("manual")) else "success",
            "variant": "tonal",
            "text": f"上次运行 {result.get('time')}：扫描 {result.get('scanned')}｜"
                    f"改名 {result.get('renamed')}｜跳过 {result.get('skipped')}｜"
                    f"待人工 {result.get('manual')}｜错误 {len(result.get('errors') or [])}"}}]

    def get_command(self) -> List[Dict[str, Any]]:
        return [{"cmd": "/conan_rename", "event": EventType.PluginAction,
                 "desc": "柯南映射重命名", "category": "管理",
                 "data": {"action": "conan_rename"}}]

    def get_api(self) -> List[Dict[str, Any]]:
        return [{"path": "/conan_rename", "endpoint": self.api_run, "methods": ["GET"],
                 "summary": "立即执行一次柯南映射重命名"}]

    @eventmanager.register(EventType.PluginAction)
    def remote_run(self, event: Event):
        if not event or not event.event_data:
            return
        if event.event_data.get("action") != "conan_rename":
            return
        result = self.run_once(manual=True)
        if event.event_data.get("channel"):
            self.post_message(channel=event.event_data.get("channel"),
                              title=f"柯南映射重命名：改名 {result.get('renamed')} 个",
                              userid=event.event_data.get("user"))

    def api_run(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        return schemas.Response(success=True, data=self.run_once(manual=True))

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
