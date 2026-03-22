# BoardGame StarMap 桌游星图

3D 力导向网络可视化：BGG Top 1000 桌游与设计师、美术、出版商的关系星图。

**在线访问：** [bgstarmap.naughtycat.com.cn](http://bgstarmap.naughtycat.com.cn)

---

## 架构

```
Mac Mini（数据工厂，每日 09:00 自动执行）
  ↓ 爬取 → 清洗 → git push
GitHub（代码 + 数据中转）
  ↓ 服务器 cron 定时 git pull
火山引擎服务器（Nginx 静态托管）
  → bgstarmap.naughtycat.com.cn
```

## 目录结构

```
├── index.html                          # 前端（3D 力导向星图）
├── graph_data.json                     # 可视化数据（~30MB，Gzip ~8MB）
├── config/
│   ├── config.example.yaml             # 配置模板
│   └── config.yaml                     # 真实配置（含 Token，不上传 GitHub）
├── crawlers/
│   ├── crawl_api.py                    # BGG XML API2 爬虫（主力，支持批量）
│   ├── crawl_legacy.py                 # 旧版 geekdo JSON API 爬虫（备用）
│   ├── crawl_publishers.py             # 出版商详情
│   ├── crawl_persons.py                # 人物详情
│   ├── patch_primary.py                # 补查新增游戏的 primarylink
│   └── get_top_ids.py                  # Top 1000 排行榜 ID 爬取
├── cleaning/
│   └── clean_data_v3.py                # 数据清洗 + graph_data.json 生成
├── data/
│   ├── bgg_starmap.db                  # SQLite 数据库（不上传 GitHub）
│   ├── top_1000_ids.txt                # 核心桌游 ID 列表
│   ├── publisher_country_manual.csv    # 出版商国家人工标注（累积）
│   ├── game_name_override.csv          # 中文译名手动修正
│   └── publisher_to_fill.csv           # 待补充国家的出版商（脚本自动生成）
├── scripts/
│   ├── daily_update.sh                 # 每日自动化调度脚本
│   └── com.naughtycat.starmap.daily.plist  # macOS launchd 定时任务
├── logs/                               # 运行日志
└── output/                             # 清洗输出
```

## 数据规模

| 类型 | 数量 |
|------|------|
| 桌游节点 | 6,662（核心 999 + 关联 5,663）|
| 人物节点 | 4,560 |
| 出版商节点 | 2,325 |
| 连线 | 74,029（7 种关系类型）|

## 手动维护

**修正中文译名：** 编辑 `data/game_name_override.csv`

**补充出版商国家：** 查看 `data/publisher_to_fill.csv`，在 `data/publisher_country_manual.csv` 中追加国家代码

## 数据来源

Powered by [BoardGameGeek](https://boardgamegeek.com)

## 版本历史

| 版本 | 日期 | 说明 |
|------|------|------|
| V3 | 2026-03 | BGG XML API2 + 自动化每日更新 + Mac Mini 数据工厂 |
| V2 | 2026-02 | 出版商国家标注 + 联合出版商 + OpenCC 中文名改进 |
| V1 | 2026-01 | 初版，geekdo 内部 API 爬取 + Three.js 3D 可视化 |
