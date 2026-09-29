# 实时硬链接+ （LinkMonitorPlus）

MoviePilot 插件：监控目录文件变化，按原文件名硬链接。

增强点：**记录已处理文件**（源文件路径 + 大小/修改时间/inode 指纹 + 目标目录），
硬链被移动、改名、整理入库后**不再重复生成**。

- 记录文件：`/config/plugins/LinkMonitorPlus/linked.json`（持久化，容器重建不丢）
- 清空记录：命令 `/realtime_link_clear`，或删除上述记录文件
- 基于 MoviePilot 内置插件 `LinkMonitor` 1.7.1（作者 jxxghp）修改，GPL-3.0

完整说明见仓库根目录的 `README.md`。
