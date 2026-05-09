# BoardGame StarMap 桌游星图

3D 力导向网络可视化：BGG Top 1000 桌游与设计师、美术、出版商的关系星图。

**在线访问：** [bgstarmap.naughtycat.com.cn](http://bgstarmap.naughtycat.com.cn)

---

## 架构

```
本地服务器（数据工厂，每日 09:00 自动执行）
  ↓ 爬取 → 清洗 → git push
GitHub（代码 + 数据中转）
  ↓ Webhook 秒级触发
远程服务器（Nginx 静态托管）
  → bgstarmap.naughtycat.com.cn
```

## 每日自动流程

| 步骤 | 说明 | 耗时 |
|------|------|------|
| 1. 下载 BGG Ranks CSV | 登录 BGG → 下载 zip → 提取 Top 1000 | ~5 秒 |
| 2. 采集新游戏 | 新增核心桌游 + 关联桌游（跳过已有） | ~0 分钟 |
| 3a. CSV 快速更新排名 | 从 CSV 更新 rank/average/usersrated | < 1 秒 |
| 3b. API 全量刷新（周一） | 补充 weight/numowned/best_players | ~40 分钟 |
| 4. 出版商/人物补充 | 新增的逐个抓取 | ~0 分钟 |
| 5. 数据清洗 | BFS 距离过滤 + 生成 graph_data.json | ~1 分钟 |
| 6. 推送 | git push → Webhook → 远程服务器自动拉取 | ~10 秒 |

日常约 2 分钟完成，周一约 45 分钟（含 API 全量刷新）。

## 目录结构

```
├── index.html                          # 前端（Three.js 3D 力导向星图）
├── graph_data.json                     # 可视化数据（每日自动更新）
├── config/
│   ├── config.example.yaml             # 配置模板
│   └── config.yaml                     # 真实配置（含 Token，不上传）
├── crawlers/
│   ├── get_top_ids.py                  # BGG Ranks CSV 下载 + Top 1000 提取
│   ├── crawl_api.py                    # BGG XML API2 爬虫（主力）
│   ├── crawl_legacy.py                 # 旧版 geekdo API 爬虫（备用）
│   ├── crawl_publishers.py             # 出版商详情
│   ├── crawl_persons.py                # 人物详情
│   └── patch_primary.py                # 补查 primarylink
├── cleaning/
│   └── clean_data_v3.py                # 数据清洗 + BFS 距离过滤 + 输出
├── data/
│   ├── bgg_starmap.db                  # SQLite 数据库（不上传）
│   ├── boardgames_ranks.csv            # BGG 每日排名 CSV（不上传）
│   ├── top_1000_ids.txt                # 核心桌游 ID 列表
│   ├── publisher_country_manual.csv    # 出版商国家人工标注（累积）
│   ├── game_name_override.csv          # 中文译名手动修正
│   └── publisher_to_fill.csv           # 待补充国家的出版商（自动生成）
├── scripts/
│   ├── daily_update.sh                 # 每日自动化调度脚本
│   └── com.naughtycat.starmap.daily.plist  # macOS launchd 定时任务
├── logs/                               # 运行日志
└── output/                             # 清洗输出
```

## 数据规模

| 类型 | 数量 |
|------|------|
| 桌游节点 | 6,837（核心 1,000 + 关联 5,837）|
| 人物节点 | 4,656 |
| 出版商节点 | 2,337 |
| 连线 | 75,446 |

> 以上数据由 `clean_data_v3.py` 每日自动更新。

**最近数据更新：** 2026-05-09 09:00

## 数据来源与技术

- 数据来源：[BoardGameGeek](https://boardgamegeek.com)（BGG XML API2 + 官方 Ranks CSV）
- 前端可视化：Three.js 3D force-directed graph
- 数据清洗：BFS 距离过滤（核心桌游 ≤ 2 步），确保图谱紧凑有意义

## 手动维护

**修正中文译名：** 编辑 `data/game_name_override.csv`

**补充出版商国家：** 查看 `data/publisher_to_fill.csv`，在 `data/publisher_country_manual.csv` 中追加国家代码

## 版本历史

| 版本 | 日期 | 说明 |
|------|------|------|
| V3.2 | 2026-03-23 | 前端：搜索增强（#排名 / id编号精确搜索、键盘↑↓选择）、详情面板显示 BGG ID |
| V3.1 | 2026-03-23 | CSV 排名快速更新 + BFS 距离过滤 + GitHub Webhook 自动部署 |
| V3 | 2026-03 | BGG XML API2 + 自动化每日更新 |
| V2 | 2026-02 | 出版商国家标注 + 联合出版商 + OpenCC 中文名改进 |
| V1 | 2026-01 | 初版，geekdo 内部 API 爬取 + Three.js 3D 可视化 |