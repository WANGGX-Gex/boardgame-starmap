"""
桌游星图 (BoardGame StarMap) - 第 1+2 轮爬虫
使用 BGG 内部 JSON API (api.geekdo.com) 抓取桌游详情
无需 API Token！

用法：
  python3 crawl_games.py

输出：
  bgg_starmap.db - SQLite 数据库，包含：
    - games_raw: 原始 JSON（数据湖，永不丢失）
    - games_dynamic: 排名/评分 JSON
"""

import os
import sys
import sqlite3
import json
import time
import re
import cloudscraper

# ============================================================
# 配置
# ============================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "bgg_starmap.db")
IDS_FILE = os.path.join(BASE_DIR, "data", "top_1000_ids.txt")

# 请求间隔（秒）- 礼貌爬虫
SLEEP_BETWEEN = 2
SLEEP_ON_ERROR = 15
MAX_RETRIES = 3

# ============================================================
# 数据库初始化
# ============================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 原始 geekitems JSON（数据湖）
    c.execute('''CREATE TABLE IF NOT EXISTS games_raw (
        game_id TEXT PRIMARY KEY,
        json_data TEXT,
        fetch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # dynamicinfo JSON（排名/评分）
    c.execute('''CREATE TABLE IF NOT EXISTS games_dynamic (
        game_id TEXT PRIMARY KEY,
        json_data TEXT,
        fetch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # 记录哪些 ID 是"第2轮发现的关联桌游"
    c.execute('''CREATE TABLE IF NOT EXISTS discovered_ids (
        game_id TEXT PRIMARY KEY,
        source_game_id TEXT,
        relation_type TEXT,
        discovered_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    conn.commit()
    return conn

# ============================================================
# 网络请求（带重试和限速）
# ============================================================
scraper = cloudscraper.create_scraper(browser={
    'browser': 'chrome',
    'platform': 'windows',
    'desktop': True
})

def fetch_json(url, retries=MAX_RETRIES):
    """请求 JSON 接口，带重试机制"""
    for attempt in range(retries):
        try:
            r = scraper.get(url, timeout=20)
            
            if r.status_code == 200:
                ct = r.headers.get('content-type', '')
                if 'json' in ct:
                    return r.json()
                else:
                    print(f"  ⚠️ 状态200但非JSON: {ct}")
                    return None
            elif r.status_code == 429:
                print(f"  ⚠️ 429 限速，等待{SLEEP_ON_ERROR}秒...")
                time.sleep(SLEEP_ON_ERROR)
            elif r.status_code == 404:
                return None  # 确实不存在，不重试
            else:
                print(f"  ❌ 状态码 {r.status_code}")
                time.sleep(5)
                
        except Exception as e:
            print(f"  🌐 网络错误: {e} (重试 {attempt+1}/{retries})")
            time.sleep(5)
    
    return None

def fetch_geekitems(game_id):
    """获取桌游/出版商/人物的详细信息"""
    url = f"https://api.geekdo.com/api/geekitems?objectid={game_id}&objecttype=thing"
    return fetch_json(url)

def fetch_dynamicinfo(game_id):
    """获取排名和评分信息"""
    url = f"https://api.geekdo.com/api/dynamicinfo?objectid={game_id}&objecttype=thing"
    return fetch_json(url)

# ============================================================
# 数据库操作
# ============================================================
def is_fetched(conn, table, game_id):
    """检查是否已经抓过（断点续传）"""
    c = conn.cursor()
    c.execute(f"SELECT 1 FROM {table} WHERE game_id = ?", (game_id,))
    return c.fetchone() is not None

def save_raw(conn, table, game_id, data):
    """保存原始 JSON 到数据库"""
    c = conn.cursor()
    c.execute(f"INSERT OR REPLACE INTO {table} (game_id, json_data) VALUES (?, ?)",
              (game_id, json.dumps(data, ensure_ascii=False)))
    conn.commit()

def save_discovered(conn, game_id, source_id, relation_type):
    """记录新发现的关联桌游 ID"""
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO discovered_ids (game_id, source_game_id, relation_type) VALUES (?, ?, ?)",
              (game_id, source_id, relation_type))
    conn.commit()

# ============================================================
# 从 geekitems 响应中提取关联桌游 ID
# ============================================================
def extract_related_ids(data, source_id):
    """从 geekitems JSON 中提取所有关联桌游的 ID 和关系类型"""
    related = []
    item = data.get('item', {})
    links = item.get('links', {})
    
    # 需要追踪的关系类型
    relation_map = {
        'boardgameexpansion': 'expansion',       # 扩展包
        'expandsboardgame': 'expands',           # 是某游戏的扩展
        'reimplementation': 'reimplemented_by',  # 被重制为
        'reimplements': 'reimplements',          # 重制了
        'boardgameintegration': 'integrates',    # 可整合
        'contains': 'contains',                  # 包含
        'containedin': 'contained_in',           # 被包含于
    }
    
    for link_type, relation_name in relation_map.items():
        items = links.get(link_type, [])
        for linked in items:
            linked_id = str(linked.get('objectid', ''))
            linked_name = linked.get('name', '')
            if linked_id and linked.get('objecttype') == 'thing':
                # 过滤 Promo（名字中含 Promo 的跳过）
                if linked_name and 'promo' in linked_name.lower():
                    continue
                related.append({
                    'id': linked_id,
                    'name': linked_name,
                    'relation': relation_name,
                    'source_id': source_id,
                })
    
    return related

# ============================================================
# 主流程
# ============================================================
def load_target_ids():
    """从 top_1000_ids.txt 加载目标 ID"""
    ids = []
    try:
        with open(IDS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    game_id = line.split(',')[0].strip()
                    ids.append(game_id)
        print(f"📦 从文件加载了 {len(ids)} 个目标 ID")
    except FileNotFoundError:
        print(f"❌ 找不到 {IDS_FILE}")
        sys.exit(1)
    return ids

def crawl_batch(conn, target_ids, batch_name="核心桌游"):
    """抓取一批桌游的 geekitems + dynamicinfo"""
    total = len(target_ids)
    
    # 过滤已抓取的（断点续传）
    todo = [gid for gid in target_ids if not is_fetched(conn, 'games_raw', gid)]
    skipped = total - len(todo)
    
    if skipped > 0:
        print(f"⏭️  跳过已抓取的 {skipped} 个，剩余 {len(todo)} 个")
    
    if not todo:
        print(f"🎉 {batch_name}全部已抓取完毕！")
        return
    
    print(f"🎯 开始抓取 {batch_name}：{len(todo)} 个目标\n")
    
    all_discovered = []
    
    for i, gid in enumerate(todo):
        progress = f"[{i+1}/{len(todo)}]"
        
        # 1) 抓 geekitems
        print(f"{progress} 🔍 抓取 ID={gid} 的详情...")
        data = fetch_geekitems(gid)
        
        if data and 'item' in data:
            item = data['item']
            name = item.get('name', '???')
            print(f"  ✅ {name}")
            
            # 保存原始 JSON
            save_raw(conn, 'games_raw', gid, data)
            
            # 提取关联桌游 ID
            related = extract_related_ids(data, gid)
            if related:
                rel_summary = {}
                for r in related:
                    rel_summary[r['relation']] = rel_summary.get(r['relation'], 0) + 1
                print(f"  🔗 发现关联: {rel_summary}")
                all_discovered.extend(related)
        else:
            print(f"  ❌ 抓取失败或无数据")
        
        time.sleep(SLEEP_BETWEEN)
        
        # 2) 抓 dynamicinfo（排名/评分）
        if not is_fetched(conn, 'games_dynamic', gid):
            dyn = fetch_dynamicinfo(gid)
            if dyn and 'item' in dyn:
                save_raw(conn, 'games_dynamic', gid, dyn)
                # 简要显示排名
                rankings = dyn['item'].get('rankinfo', [])
                if rankings:
                    r0 = rankings[0]
                    print(f"  📊 排名#{r0.get('rank','?')} Geek评分={r0.get('baverage','?')}")
            
            time.sleep(SLEEP_BETWEEN)
        
        # 每 50 个打印一次进度汇总
        if (i + 1) % 50 == 0:
            print(f"\n{'='*50}")
            print(f"📈 进度: {i+1}/{len(todo)} 完成")
            print(f"🔗 累计发现 {len(all_discovered)} 个关联桌游")
            print(f"{'='*50}\n")
    
    # 保存所有发现的关联 ID
    for r in all_discovered:
        save_discovered(conn, r['id'], r['source_id'], r['relation'])
    
    print(f"\n🏁 {batch_name}抓取完毕！共发现 {len(all_discovered)} 个关联桌游")

def get_discovered_todo(conn, existing_ids):
    """获取第2轮需要抓取的关联桌游 ID（去重，排除已有的）"""
    c = conn.cursor()
    c.execute("SELECT DISTINCT game_id FROM discovered_ids")
    discovered = set(row[0] for row in c.fetchall())
    
    # 排除已经在第1轮抓过的
    existing = set(existing_ids)
    c.execute("SELECT game_id FROM games_raw")
    already_fetched = set(row[0] for row in c.fetchall())
    
    todo = discovered - existing - already_fetched
    return list(todo)

def main():
    print("🚀 桌游星图 - 数据采集系统启动！")
    print(f"📁 数据库: {DB_PATH}")
    print(f"🔧 接口: api.geekdo.com (无需Token)")
    print()
    
    conn = init_db()
    
    # =====================
    # 第 1 轮：核心桌游
    # =====================
    print("="*60)
    print("📡 第 1 轮：抓取 Top 1000 核心桌游")
    print("="*60)
    
    core_ids = load_target_ids()
    crawl_batch(conn, core_ids, "Top 1000 核心桌游")
    
    # =====================
    # 第 2 轮：关联桌游
    # =====================
    print("\n" + "="*60)
    print("📡 第 2 轮：抓取关联桌游（扩展包/同源/整合）")
    print("="*60)
    
    discovered_todo = get_discovered_todo(conn, core_ids)
    
    if discovered_todo:
        print(f"🔍 发现 {len(discovered_todo)} 个需要补充抓取的关联桌游")
        crawl_batch(conn, discovered_todo, "关联桌游")
        
        # 第2轮可能又发现新的关联，再做一轮（最多递归1次）
        discovered_todo_2 = get_discovered_todo(conn, core_ids)
        if discovered_todo_2:
            print(f"\n🔍 第2轮又发现了 {len(discovered_todo_2)} 个新关联，继续补充...")
            crawl_batch(conn, discovered_todo_2, "第2轮新增关联")
    else:
        print("✨ 没有需要补充的关联桌游")
    
    # =====================
    # 统计汇总
    # =====================
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM games_raw")
    total_games = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM games_dynamic")
    total_dynamic = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM discovered_ids")
    total_discovered = c.fetchone()[0]
    
    print("\n" + "="*60)
    print("🏆 采集完毕！统计汇总：")
    print(f"  📦 桌游详情 (games_raw): {total_games} 条")
    print(f"  📊 排名评分 (games_dynamic): {total_dynamic} 条")
    print(f"  🔗 关联记录 (discovered_ids): {total_discovered} 条")
    print(f"  💾 数据库文件: {DB_PATH}")
    print("="*60)
    
    conn.close()

if __name__ == "__main__":
    main()
