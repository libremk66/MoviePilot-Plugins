# 更新日志

## v1.7.2

基于 MoviePilot 内置插件 `LinkMonitor`（实时硬链接）**1.7.1** 修改。原版行为：仅判断"目标路径是否存在文件"，因此硬链被移动/整理入库后，下次同步会再次生成。

本次改动：

1. **新增「已处理文件记录」**
   - 记录内容：`源文件绝对路径 → {size, mtime, ino, target}`
   - 落盘位置：`/config/plugins/LinkMonitorPlus/linked.json`（持久化，容器重建不丢）
2. **跳过判据（主）**：记录存在 + 文件指纹一致 + 目标目录未变 → 跳过（目标已被搬走也视为已处理）
3. **自动补登记**：链接成功时登记；目标文件已存在时也登记 → 升级后跑一次全量同步即可把存量文件纳入记录
4. **兜底判据**：记录不存在时，用 `st_nlink > 1`（源文件已有其它硬链接）判断为"此前已生成过"
5. **重新处理**：文件被替换/重新下载（大小或修改时间变化）、目标目录被修改 → 正常重新链接
6. **新增清空入口**：
   - 命令：`/realtime_link_clear`
   - 接口：`GET /api/v1/plugin/LinkMonitorPlus/realtime_link_clear?apikey=<MP_API_TOKEN>`
7. **日志与通知**：跳过（记录命中 / 已有硬链 / 目标已存在）时只记日志，不再发送"硬链接完成"通知
8. **插件标识**：类名 `LinkMonitorPlus`、`plugin_name = 实时硬链接+`、`plugin_config_prefix = linkmonitorplus_`，避免与内置插件冲突

---

## 原版历史

- v1.7.1 及之前：见 MoviePilot 内置插件 `LinkMonitor`
