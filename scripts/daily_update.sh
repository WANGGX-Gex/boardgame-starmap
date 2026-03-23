#!/bin/bash
# ============================================================
# 桌游星图 — 每日自动更新
# 运行时间：每天 09:00 (UTC+8)，由 macOS launchd 调度
# 位置：~/Coding/BoardGame_StarMap/scripts/daily_update.sh
# ============================================================

set -euo pipefail

PROJECT_DIR="$HOME/Coding/BoardGame_StarMap"
LOG_DIR="$PROJECT_DIR/logs"
DATE=$(date +%Y%m%d)
LOG_FILE="$LOG_DIR/daily_update_${DATE}.log"

mkdir -p "$LOG_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "=========================================="
echo "🚀 桌游星图每日更新 — $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="

cd "$PROJECT_DIR"

# 激活虚拟环境
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    echo "❌ 找不到 venv/bin/activate，请先创建虚拟环境"
    exit 1
fi

# ── 0. 从 GitHub 同步（拉取手动修正的 CSV 等）──
echo ""
echo "📥 [0/5] 同步 GitHub..."
git pull origin main --rebase || git pull origin main

# ── 1. 更新 Top 1000（每天下载最新 CSV）──
echo ""
echo "📋 [1/5] 下载 BGG ranks CSV 并更新 Top 1000..."
python3 crawlers/get_top_ids.py || echo "⚠️ Top 1000 更新失败，使用上次的列表"

# ── 2. 采集桌游数据（API 优先，Legacy 兜底）──
echo ""
echo "🎲 [2/5] 采集桌游数据..."
if python3 crawlers/crawl_api.py 2>&1; then
    echo "✅ XML API 采集成功"
else
    echo "⚠️ XML API 失败，回退到 Legacy 爬虫..."
    if python3 crawlers/crawl_legacy.py 2>&1; then
        echo "✅ Legacy 爬虫采集成功"
    else
        echo "❌ Legacy 爬虫也失败了，跳过桌游数据更新"
    fi
fi

# ── 2b. 周二补查 primarylink（旧版API，只查新增游戏）──
DAY_OF_WEEK=$(date +%u)
if [ "$DAY_OF_WEEK" -eq 2 ]; then
    echo ""
    echo "🔧 [2b] 周二：补查新增游戏的 primarylink..."
    python3 crawlers/patch_primary.py || echo "⚠️ primarylink 补查异常"
fi

# ── 3. 补充出版商和人物（始终用旧版 API，各最多 30 分钟）──
# macOS 没有 timeout 命令，用 perl 替代
_timeout() { perl -e 'alarm shift; exec @ARGV' "$@"; }

echo ""
echo "🏢 [3/5] 补充出版商详情..."
_timeout 1800 python3 crawlers/crawl_publishers.py || echo "⚠️ 出版商采集异常或超时（30分钟），跳过"

echo ""
echo "👤 [3/5] 补充人物详情..."
_timeout 1800 python3 crawlers/crawl_persons.py || echo "⚠️ 人物采集异常或超时（30分钟），跳过"

# ── 4. 数据清洗 ──
echo ""
echo "🧹 [4/5] 数据清洗..."
python3 cleaning/clean_data_v3.py

# ── 5. 推送到 GitHub ──
echo ""
echo "📤 [5/5] 推送到 GitHub..."

# 复制输出文件到仓库根目录
cp output/graph_data.json ./graph_data.json 2>/dev/null || true

# 提交变更
git add -A
if git diff --cached --quiet; then
    echo "  无变更，跳过推送"
else
    git commit -m "📊 Daily update: $(date '+%Y-%m-%d')"
    git push origin main
    echo "  ✅ 已推送到 GitHub"
fi

echo ""
echo "=========================================="
echo "🎉 每日更新完成 — $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="