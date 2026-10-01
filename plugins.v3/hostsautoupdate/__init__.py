# coding=UTF-8
"""
Hosts 自动更新（HostsAutoUpdate）

定期用 **DoH（加密 DNS）** 查询指定域名的真实 IP，写进**容器内的 /etc/hosts**，
用来绕开**明文 DNS 被污染**导致解析失败的问题。

解决的真实场景（2026-10-01 排查）：
    MoviePilot 探索页 Bangumi 封面全裂 —— 根因不是白名单、不是代理，
    而是 `*.bgm.tv` 被 DNS 污染：本地明文 DNS 要么 SERVFAIL、要么返回假 IP，
    只有加密 DNS 能拿到真的 Cloudflare IP。
    而 MP 跑在容器里，它的 DNS 走宿主机 → 路由器，压根不经过 Clash。

    hosts 的作用就是「绕过 DNS 直接给 IP」，跟 docker-compose 的 extra_hosts 同理，
    但本插件能**自动跟随 IP 变化**，且**容器重建后会自动重新写入**。

安全约定：
    · 只写插件自己的注释区块（`# >>> HostsAutoUpdate …` / `# <<<`），文件其它内容一律不动
    · 每次写入前备份到插件数据目录（hosts.bak.YYYYMMDD-HHMMSS）
    · DoH 查询失败的域名**保留上一次的值**，不会被清掉
    · 没有变化时不写文件（减少无谓写入）
    · /etc/hosts 在容器内是 Docker bind mount，**不能 os.replace**（会 EBUSY），
      必须就地覆盖写 —— 见 _write_hosts()
"""
import re
from datetime import datetime
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

# 容器内 hosts 路径（Docker 挂载；容器重建后内容会重置，本插件下次运行会重新写入）
HOSTS_PATH = "/etc/hosts"
# 插件维护的注释区块标记
MARK_BEGIN = "# >>> HostsAutoUpdate 自动维护（插件托管，请勿手改）"
MARK_END = "# <<< HostsAutoUpdate 自动维护结束"

# 默认配置
DEFAULT_DOMAINS = "lain.bgm.tv\napi.bgm.tv\nbgm.tv"
# 默认给两个**互相验证**的 DoH：
#   · doh.pub      —— 国内直连可达，即使没配代理也至少有一个来源能用
#   · cloudflare   —— 结果最权威（需走代理）
#
# 为什么要两个：单一来源无法判断对错。实测 doh.pub 对 api.bgm.tv **偶尔**会返回
# 假 IP（抓到过一次 179.60.193.16，随后连续 10 次又都正确）—— 污染是间歇性的。
# 两个来源取交集，能兜住"只有一个被污染"的情况。
DEFAULT_DOH = "https://doh.pub/dns-query\nhttps://cloudflare-dns.com/dns-query"
DEFAULT_CRON = "0 */6 * * *"


class HostsAutoUpdate(_PluginBase):
    # 插件名称
    plugin_name = "Hosts 自动更新"
    # 插件描述
    plugin_desc = (
        "定期用 DoH（加密 DNS）查询指定域名的真实 IP，写入容器 /etc/hosts，"
        "绕开明文 DNS 污染导致的解析失败（如 *.bgm.tv）；自动跟随 IP 变化、"
        "容器重建后自动重写。只维护自己的注释区块，文件其它内容不动。"
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
    plugin_config_prefix = "hostsautoupdate_"
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
    _domains: str = DEFAULT_DOMAINS
    _doh_servers: List[str] = []
    _use_proxy: bool = True

    # ─── 生命周期 ───

    def init_plugin(self, config: dict = None):
        """
        读取配置。
        ⚠️ 每次 MoviePilot 启动都会走到这里 —— 容器重建后 /etc/hosts 会被 Docker 重置，
           所以这里**固定**安排一次启动后执行，把区块重新写回去（这是本插件的核心价值之一）。
        """
        self.stop_service()
        if config:
            self._enabled = bool(config.get("enabled"))
            self._notify = bool(config.get("notify"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._cron = config.get("cron") or DEFAULT_CRON
            self._domains = str(config.get("domains") or "").strip() or DEFAULT_DOMAINS
            self._doh_servers = self._parse_domain_list(
                str(config.get("doh_server") or "").strip() or DEFAULT_DOH)
            self._use_proxy = bool(config.get("use_proxy", True))

        jobs: List[Dict[str, Any]] = []
        if self._enabled:
            # 启动后跑一次：补回容器重建丢失的区块
            jobs.append({"func": self.update_hosts, "name": "Hosts 自动更新（启动补写）"})
        if self._enabled and self._onlyonce:
            jobs.append({"func": self.update_hosts, "name": "Hosts 自动更新（手动触发）"})
            self._onlyonce = False

        if jobs:
            self._scheduler = BackgroundScheduler(timezone="Asia/Shanghai")
            for job in jobs:
                self._scheduler.add_job(
                    func=job["func"], trigger="date",
                    kwargs={"manual": False}, name=job["name"],
                )
            self._scheduler.start()
            if config:
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
            logger.error(f"Hosts 自动更新：停止服务出错：{str(e)}")

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时服务。"""
        if not (self._enabled and self._cron):
            return []
        return [{
            "id": "HostsAutoUpdate",
            "name": "Hosts 自动更新",
            "trigger": CronTrigger.from_crontab(self._cron),
            "func": self.update_hosts,
            "kwargs": {"manual": False},
        }]

    # ─── 核心逻辑 ───

    @staticmethod
    def _parse_domain_list(raw: str) -> List[str]:
        """解析域名清单：一行一个，# 开头为注释。"""
        out: List[str] = []
        for line in (raw or "").splitlines():
            d = line.strip()
            if not d or d.startswith("#"):
                continue
            # 容忍误粘贴的完整 URL
            d = re.sub(r"^https?://", "", d).split("/")[0].strip()
            if d and d not in out:
                out.append(d)
        return out

    @staticmethod
    def _is_valid_ipv4(value: str) -> bool:
        parts = (value or "").split(".")
        if len(parts) != 4:
            return False
        for p in parts:
            if not p.isdigit() or not 0 <= int(p) <= 255:
                return False
        return True

    def _get_proxies(self) -> Optional[dict]:
        """按 MP 的约定取系统代理；未启用或未配置则返回 None（直连）。"""
        if not self._use_proxy:
            return None
        try:
            proxy = getattr(settings, "PROXY", None)
            if proxy:
                return {"http": proxy, "https": proxy}
            logger.warning("Hosts 自动更新：勾了「使用代理」但系统代理未配置，本次直连")
        except Exception as e:
            logger.error(f"Hosts 自动更新：读取系统代理失败：{str(e)}")
        return None

    def _doh_query(self, server: str, domain: str) -> set:
        """
        用某个 DoH 服务器查 A 记录，返回**全部**有效 IPv4 的集合（失败返回空集）。

        ⚠️ 返回全集而不是第一个：不同 DoH 返回的 IP **顺序本来就不同**
        （Cloudflare 和 Google 对同一域名给的三个 IP 顺序就不一样），
        只比首个会误判成"来源不一致"。跨来源比对必须用集合。
        """
        ips: set = set()
        try:
            resp = requests.get(
                server,
                params={"name": domain, "type": "A"},
                headers={"accept": "application/dns-json"},
                timeout=10,
                proxies=self._get_proxies(),
            )
            resp.raise_for_status()
            for ans in (resp.json() or {}).get("Answer", []):
                data = ans.get("data", "")
                if ans.get("type") == 1 and self._is_valid_ipv4(data):
                    ips.add(data)
            if not ips:
                logger.warning(f"Hosts 自动更新：{server} 未返回 {domain} 的有效 IPv4")
        except Exception as e:
            logger.warning(f"Hosts 自动更新：查询 {domain} 失败（{server}）：{str(e)}")
        return ips

    @staticmethod
    def _short_name(server: str) -> str:
        return re.sub(r"^https?://", "", server or "").split("/")[0]

    def _resolve(self, domain: str) -> Tuple[Optional[str], str]:
        """
        多 DoH **交叉验证**：取各来源 IP 的**交集**。

        交集非空 → 采用（多个独立来源一致，基本可排除污染）。
        交集为空 → 判定来源不一致（很可能是 DNS 污染），**不采用**并返回详情。
        只有一个来源 → 采用，但注明"未交叉验证"（单品来源无法判断对错）。
        """
        per_source: List[Tuple[str, set]] = []
        for server in self._doh_servers:
            ips = self._doh_query(server, domain)
            if ips:
                per_source.append((self._short_name(server), ips))
        if not per_source:
            return None, "全部 DoH 查询失败"
        if len(per_source) == 1:
            name, ips = per_source[0]
            picked = sorted(ips)[0]
            return picked, f"仅 {name}（未交叉验证）"
        common = set.intersection(*[ips for _, ips in per_source])
        detail = "｜".join(f"{n}={'/'.join(sorted(i))}" for n, i in per_source)
        if not common:
            return None, f"⚠️ 各来源不一致，疑似污染 → {detail}"
        return sorted(common)[0], detail

    @staticmethod
    def _read_hosts_lines() -> Optional[List[str]]:
        try:
            return Path(HOSTS_PATH).read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception as e:
            logger.error(f"Hosts 自动更新：读取 {HOSTS_PATH} 失败：{str(e)}")
            return None

    @staticmethod
    def _strip_block(lines: List[str]) -> List[str]:
        """去掉插件维护的区块，返回其余内容。"""
        kept: List[str] = []
        inside = False
        for ln in lines:
            s = ln.strip()
            if s == MARK_BEGIN:
                inside = True
                continue
            if s == MARK_END:
                inside = False
                continue
            if not inside:
                kept.append(ln)
        return kept

    def _write_hosts(self, final_lines: List[str]) -> bool:
        """
        就地覆盖写 /etc/hosts。

        ⚠️ 不能先写临时文件再 os.replace —— /etc/hosts 在容器里是 Docker 的 bind mount，
           替换挂载点会报 EBUSY。必须直接 open(...,'w') 覆盖同一个 inode。
        """
        content = "\n".join(final_lines).rstrip("\n") + "\n"
        try:
            # 备份到插件数据目录
            try:
                backup_dir = Path(self.get_data_path())
                backup_dir.mkdir(parents=True, exist_ok=True)
                old = Path(HOSTS_PATH).read_text(encoding="utf-8", errors="replace")
                (backup_dir / f"hosts.bak.{datetime.now():%Y%m%d-%H%M%S}").write_text(old, encoding="utf-8")
            except Exception as e:
                logger.warning(f"Hosts 自动更新：备份失败（不影响写入）：{str(e)}")

            with open(HOSTS_PATH, "w", encoding="utf-8") as f:
                f.write(content)
            return True
        except Exception as e:
            logger.error(f"Hosts 自动更新：写入 {HOSTS_PATH} 失败：{str(e)}")
            return False

    def update_hosts(self, manual: bool = False) -> Dict[str, Any]:
        """
        主流程：DoH 查真实 IP → 重建插件区块 → 有变化才写。
        """
        result: Dict[str, Any] = {
            "checked": 0, "resolved": 0, "failed": [], "changed": False, "written": False,
        }
        domains = self._parse_domain_list(self._domains)
        if not domains:
            logger.error("Hosts 自动更新：域名清单为空，什么也不做")
            result["error"] = "域名清单为空"
            return result

        lines = self._read_hosts_lines()
        if lines is None:
            result["error"] = f"读取 {HOSTS_PATH} 失败"
            return result

        # 解析出上一次写入的值，用于失败时回退
        previous: Dict[str, str] = {}
        for ln in lines:
            parts = ln.split()
            if len(parts) == 2 and self._is_valid_ipv4(parts[0]) and parts[1] in domains:
                previous.setdefault(parts[1], parts[0])

        # 逐个查询
        mapping: Dict[str, str] = {}
        for dom in domains:
            result["checked"] += 1
            ip, detail = self._resolve(dom)
            if ip:
                mapping[dom] = ip
                result["resolved"] += 1
                logger.info(f"Hosts 自动更新：{dom} → {ip}（{detail}）")
            elif dom in previous:
                # 查询失败 → 保留上次的值，避免把已有记录清掉
                mapping[dom] = previous[dom]
                result["failed"].append(f"{dom}(保留旧值)")
                logger.warning(f"Hosts 自动更新：{dom} 查询失败，保留上次的 {previous[dom]}")
            else:
                result["failed"].append(dom)
                logger.warning(f"Hosts 自动更新：{dom} 查询失败且无旧值，跳过")

        if not mapping:
            logger.error("Hosts 自动更新：所有域名都没解析出来，不写文件")
            result["error"] = "全部解析失败"
            return result

        # 组装新内容
        block = [MARK_BEGIN]
        for dom in domains:
            if dom in mapping:
                block.append(f"{mapping[dom]} {dom}")
        block.append(f"# 最后更新：{datetime.now():%Y-%m-%d %H:%M:%S}")
        block.append(MARK_END)

        kept = self._strip_block(lines)
        # 去掉尾部的空行，保持整洁
        while kept and not kept[-1].strip():
            kept.pop()
        final = kept + [""] + block

        old_block = [ln for ln in lines if ln.split()[-1:] and ln.split()[-1] in domains]
        new_block = [ln for ln in block if ln.split() and ln.split()[-1] in domains]
        if sorted(x.strip() for x in old_block) == sorted(x.strip() for x in new_block):
            logger.info(f"Hosts 自动更新：{len(mapping)} 个域名解析结果无变化，跳过写入")
            result["changed"] = False
            if self._notify:
                self.__notify("Hosts 自动更新：无变化", f"{len(mapping)} 个域名解析结果未变，未写入")
            return result

        if self._write_hosts(final):
            result["changed"] = True
            result["written"] = True
            logger.info(f"Hosts 自动更新：已写入 {len(mapping)} 条 → {HOSTS_PATH}")
        else:
            result["error"] = "写入失败"

        if self._notify:
            lines_msg = [f"{ip}  {d}" for d, ip in mapping.items()]
            title = f"Hosts 自动更新：{'已更新 ' + str(len(mapping)) + ' 条' if result['written'] else '无变化'}"
            if result["failed"]:
                lines_msg.append(f"\n⚠️ 查询失败：{', '.join(result['failed'])}")
            self.__notify(title, "\n".join(lines_msg))
        return result

    def __notify(self, title: str, text: str) -> None:
        """发送通知（是否发由调用方按 _notify 决定）。"""
        try:
            self.post_message(mtype=NotificationType.Manual, title=title, text=text)
        except Exception as e:
            logger.error(f"Hosts 自动更新：发送通知失败：{str(e)}")

    def __update_config(self):
        """把重置后的 onlyonce 写回配置。"""
        try:
            self.update_config({
                "enabled": self._enabled, "notify": self._notify,
                "onlyonce": self._onlyonce, "cron": self._cron,
                "domains": self._domains,
                "doh_server": "\n".join(self._doh_servers),
                "use_proxy": self._use_proxy,
            })
        except Exception as e:
            logger.error(f"Hosts 自动更新：回写配置失败：{str(e)}")

    # ─── 配置与页面 ───

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {"component": "VForm", "content": [
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                        {"component": "VSwitch", "props": {"model": "onlyonce", "label": "保存后立即运行一次"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VTextarea", "props": {
                            "model": "domains",
                            "label": "域名清单（一行一个，# 开头为注释）",
                            "rows": 5,
                            "placeholder": DEFAULT_DOMAINS}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VTextarea", "props": {
                            "model": "doh_server",
                            "label": "DoH 服务器（一行一个；填 ≥2 个会做交叉验证，能自动识别污染）",
                            "rows": 2,
                            "placeholder": DEFAULT_DOH}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VSwitch", "props": {
                            "model": "use_proxy",
                            "label": "DoH 请求走系统代理（国外 DoH 国内直连不通，需开）"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                        {"component": "VTextField", "props": {
                            "model": "cron", "label": "定时更新（cron）",
                            "placeholder": DEFAULT_CRON}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12}, "content": [
                        {"component": "VAlert", "props": {
                            "type": "info", "variant": "tonal", "class": "mb-2",
                            "text": "原理：DoH 查真实 IP → 写入容器 /etc/hosts 的插件区块，用于绕开明文 DNS 污染。"
                                    "填多个 DoH 时取各来源 IP 的交集：交集为空 = 来源不一致 = 疑似污染，会跳过并告警。"
                                    "每次写入前自动备份；查询失败的域名保留上一次的值；容器重建后自动重写。"}}]},
                ]},
            ]},
        ], {
            "enabled": False,
            "notify": False,
            "onlyonce": False,
            "domains": DEFAULT_DOMAINS,
            "doh_server": DEFAULT_DOH,
            "use_proxy": True,
            "cron": DEFAULT_CRON,
        }

    def get_page(self) -> List[dict]:
        domains = self._parse_domain_list(self._domains)
        lines = self._read_hosts_lines() or []
        current = [ln for ln in lines
                   if len(ln.split()) == 2 and self._is_valid_ipv4(ln.split()[0])
                   and ln.split()[1] in domains]
        text = "\n".join(current) if current else "（当前 /etc/hosts 里没有这些域名的记录）"
        return [
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [
                    {"component": "VAlert", "props": {
                        "type": "success" if current else "warning",
                        "variant": "tonal",
                        "text": f"当前生效的 hosts 记录（{len(current)}/{len(domains)}）：\n{text}"}}]},
            ]},
        ]

    def get_command(self) -> List[Dict[str, Any]]:
        return [{"cmd": "/hosts_update", "event": EventType.PluginAction,
                 "desc": "立即更新一次 hosts", "category": "管理",
                 "data": {"action": "hosts_autoupdate"}}]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {"path": "/hosts_update", "endpoint": self.api_update, "methods": ["GET"],
             "summary": "立即更新一次 hosts"},
            {"path": "/hosts_status", "endpoint": self.api_status, "methods": ["GET"],
             "summary": "查看当前 hosts 里这些域名的记录"},
        ]

    def api_update(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        return schemas.Response(success=True, data=self.update_hosts(manual=True))

    def api_status(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        domains = self._parse_domain_list(self._domains)
        lines = self._read_hosts_lines() or []
        current = {ln.split()[1]: ln.split()[0] for ln in lines
                   if len(ln.split()) == 2 and self._is_valid_ipv4(ln.split()[0])
                   and ln.split()[1] in domains}
        return schemas.Response(success=True, data={
            "domains": domains,
            "hosts_path": HOSTS_PATH,
            "doh_servers": self._doh_servers,
            "use_proxy": self._use_proxy,
            "current": current,
        })

    @eventmanager.register(EventType.PluginAction)
    def remote_run(self, event: Event):
        if not event or not event.event_data:
            return
        if event.event_data.get("action") != "hosts_autoupdate":
            return
        result = self.update_hosts(manual=True)
        title = f"Hosts 自动更新：写入 {result.get('resolved')} 条" \
            if result.get("written") else "Hosts 自动更新：无变化"
        if event.event_data.get("channel"):
            self.post_message(channel=event.event_data.get("channel"), title=title,
                              userid=event.event_data.get("user"))
