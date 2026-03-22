# 桌游星图 V3 — 配置教程与运维手册

---

## 一、各设备配置步骤

### 1.1 GitHub

1. 将本 zip 包中 **除 `config/config.yaml` 和 `data/bgg_starmap.db` 之外** 的所有文件上传到仓库 `https://github.com/WANGGX-Gex/boardgame-starmap/`
2. 将你本地已有的 `data/top_1000_ids.txt` 和 `data/publisher_country_manual.csv` 也放到仓库的 `data/` 目录
3. 将你刚才清洗生成的 `output/graph_data.json` 复制到仓库根目录作为 `graph_data.json`

> `config/config.yaml` 含 Token，已在 `.gitignore` 中自动排除，不会被上传。  
> `data/bgg_starmap.db` 太大（300MB+），也已排除。

### 1.2 Mac Mini（首次配置）

```bash
# ── 安装基础工具 ──
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zshrc
source ~/.zshrc
brew install python@3.12 git

# ── 配置 Git ──
git config --global user.name "WANGGX-Gex"
git config --global user.email "你的邮箱"

# ── SSH 密钥（GitHub + 服务器免密）──
ssh-keygen -t ed25519
pbcopy < ~/.ssh/id_ed25519.pub
# → 去 GitHub Settings → SSH keys → 粘贴公钥
ssh-copy-id root@163.7.3.98
cat >> ~/.ssh/config << 'EOF'
Host volcano
    HostName 163.7.3.98
    User root
EOF

# ── 克隆项目 ──
mkdir -p ~/Coding && cd ~/Coding
git clone git@github.com:WANGGX-Gex/boardgame-starmap.git BoardGame_StarMap
cd BoardGame_StarMap

# ── Python 环境 ──
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# ── 配置文件（Token 已预填，直接复制即可）──
# config/config.yaml 已包含在 zip 中，如果 git clone 时没有
# （因为 .gitignore 排除了），手动复制 zip 里的 config/config.yaml 到项目中

# ── 下载数据库 ──
scp -C volcano:/root/BoardGame_StarMap/bgg_starmap.db data/

# ── 验证 ──
python3 cleaning/clean_data_v3.py
# 看到 🎉 完成！ 即为成功

# ── 安装定时任务 ──
chmod +x scripts/daily_update.sh
cp scripts/com.naughtycat.starmap.daily.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.naughtycat.starmap.daily.plist

# ── Mac Mini 防休眠 ──
sudo pmset -c sleep 0
sudo pmset -c displaysleep 10
```

### 1.3 火山引擎服务器

```bash
ssh volcano

# 克隆仓库（如果是新路径）
cd /root
git clone https://github.com/WANGGX-Gex/boardgame-starmap.git

# 确认 Nginx root 指向正确
# 编辑 Nginx 配置，确保 root 为 /root/boardgame-starmap
nginx -t && systemctl reload nginx

# 添加定时拉取（每天 09:30 UTC+8 = 01:30 UTC）
crontab -e
# 添加：
# 30 1 * * * cd /root/boardgame-starmap && git pull origin main >> /var/log/starmap_pull.log 2>&1
```

### 1.4 MacBook Pro（开发用，可选）

```bash
cd ~/Coding
git clone git@github.com:WANGGX-Gex/boardgame-starmap.git BoardGame_StarMap
cd BoardGame_StarMap
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
# 如需本地测试清洗，也要下载 db：
# scp -C volcano:/root/BoardGame_StarMap/bgg_starmap.db data/
```

---

## 二、日常数据维护

### 2.1 修正中文译名

当发现某个游戏的中文名不对时：

1. 打开 `data/game_name_override.csv`
2. 添加一行：

```csv
bgg_id,name_zh_override,note
224517,黄铜：伯明翰,BGG 上的中文名格式不标准
342942,方舟：觉醒,BGG 上没有收录中文名
```

3. 提交推送：

```bash
git add data/game_name_override.csv
git commit -m "修正中文译名"
git push
```

次日 09:00 Mac Mini 自动生效。

**如何找到 BGG ID？** 在星图中点击游戏节点，详情面板会显示 BGG ID；或者在 BGG 网站 URL 中找（如 `boardgamegeek.com/boardgame/224517` 里的 `224517`）。

### 2.2 补充出版商国家

1. 查看 `data/publisher_to_fill.csv`（每次清洗自动更新，列出未匹配国家的出版商）
2. 在 `data/publisher_country_manual.csv` 中**追加**新行（不要删旧行！这是累积文件）
3. 常用国家代码：

| 代码 | 国家 | 代码 | 国家 | 代码 | 国家 |
|------|------|------|------|------|------|
| US | 美国 | DE | 德国 | FR | 法国 |
| UK | 英国 | JP | 日本 | KR | 韩国 |
| CN | 中国 | TW | 中国台湾 | HK | 中国香港 |
| IT | 意大利 | ES | 西班牙 | PL | 波兰 |
| CZ | 捷克 | CA | 加拿大 | AU | 澳大利亚 |
| BR | 巴西 | RU | 俄罗斯 | SE | 瑞典 |

4. 提交推送，次日自动生效。

---

## 三、监控与故障排查

### 3.1 每日自动发生的事

| 时间 (UTC+8) | 事件 | 设备 |
|-------------|------|------|
| 09:00 | `daily_update.sh` 自动执行 | Mac Mini |
| 09:00~09:20 | 爬取 → 清洗 → git push | Mac Mini |
| 09:30 | `git pull` 拉取最新数据 | 火山引擎 |

### 3.2 查看运行日志

```bash
# 今天的日志
cat ~/Coding/BoardGame_StarMap/logs/daily_update_$(date +%Y%m%d).log

# 最近 7 天摘要
for i in {0..6}; do
  d=$(date -v-${i}d +%Y%m%d)
  f=~/Coding/BoardGame_StarMap/logs/daily_update_${d}.log
  [ -f "$f" ] && echo "=== $d ===" && tail -3 "$f" && echo
done
```

### 3.3 常见问题处理

**Token 过期（日志出现 "⚠️ XML API 失败"）：**
1. 登录 https://boardgamegeek.com/applications
2. 重新生成 Token
3. 编辑 Mac Mini 上的 `config/config.yaml`，替换 `xml_api_token`
4. Token 过期期间不会丢数据（自动回退旧版爬虫）

**Mac Mini 关机/重启后：**
无需操作。launchd 会在下个 09:00 自动恢复，错过的任务会补跑。

**网站没更新：**
```bash
ssh volcano
cd /root/boardgame-starmap && git pull origin main
```

**手动立即执行一次更新：**
```bash
cd ~/Coding/BoardGame_StarMap
source venv/bin/activate
bash scripts/daily_update.sh
```

**只跑清洗（不爬取）：**
```bash
cd ~/Coding/BoardGame_StarMap
source venv/bin/activate
python3 cleaning/clean_data_v3.py
```

### 3.4 日志清理（每月一次）

```bash
find ~/Coding/BoardGame_StarMap/logs -name "*.log" -mtime +30 -delete
```

---

## 四、Token 说明

BGG API Token 已预填在 `config/config.yaml` 中：

```
67aa352a-290e-4e84-bf1e-87b65eea8956
```

此文件通过 `.gitignore` 排除，**不会上传到 GitHub**。你只需要在 Mac Mini 上保证这个文件存在即可，不需要手动配置。如果用 `git clone` 拉下来后发现没有这个文件，从 zip 包里的 `config/config.yaml` 手动复制过去。

---

## 五、未来迭代方向

| 方向 | 说明 | 优先级 |
|------|------|--------|
| OpenClaw 每日汇报 | 配置好后接入，每天通知更新情况 | 中 |
| CDN 部署 | 火山引擎 CDN，需完成 ICP 备案 | 低 |
| 增量更新优化 | 只更新排名变动的游戏，减少请求量 | 低 |
| Hot 100 热度榜 | 抓取 BGG 热门榜，补充最新热门游戏 | 低 |
| 数据库瘦身 | 定期 VACUUM，清理 WAL 日志 | 低 |

如需开发迭代，建议在此文档的"版本历史"部分记录每次重大变更即可。小项目不需要单独的迭代文档，README 的版本历史表 + git commit 记录已经足够。
