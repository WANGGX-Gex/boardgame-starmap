"""
桌游星图 - 第 3 轮：抓取出版商详情
从 bgg_starmap.db 中提取所有出版商 ID，
用 api.geekdo.com/api/geekitems?objecttype=company 抓取详情

用法：python3 crawl_publishers.py
"""
import os
import sqlite3
import json
import time
import re
import cloudscraper
import requests

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "bgg_starmap.db")

SLEEP_BETWEEN = 2
SLEEP_ON_ERROR = 15
MAX_RETRIES = 3

# ============================================================
# 数据库
# ============================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS publishers_raw (
        publisher_id TEXT PRIMARY KEY,
        json_data TEXT,
        fetch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    conn.commit()
    return conn

# ============================================================
# 网络请求
# ============================================================
scraper = cloudscraper.create_scraper(browser={
    'browser': 'chrome',
    'platform': 'windows',
    'desktop': True
})

def _recreate_scraper():
    global scraper
    scraper = cloudscraper.create_scraper(browser={
        'browser': 'chrome', 'platform': 'windows', 'desktop': True
    })

def fetch_json(url):
    for attempt in range(MAX_RETRIES):
        try:
            r = scraper.get(url, timeout=(10, 20))  # (connect_timeout, read_timeout)
            if r.status_code == 200 and 'json' in r.headers.get('content-type', ''):
                return r.json()
            elif r.status_code == 429:
                print(f"  ⚠️ 429 限速，等待{SLEEP_ON_ERROR}秒...")
                time.sleep(SLEEP_ON_ERROR)
            elif r.status_code == 404:
                return None
            elif r.status_code == 403:
                print(f"  ⚠️ 403 被拦截（Cloudflare），等待{SLEEP_ON_ERROR}秒...")
                time.sleep(SLEEP_ON_ERROR)
                _recreate_scraper()
            else:
                print(f"  ❌ 状态码 {r.status_code}")
                time.sleep(5)
        except requests.exceptions.Timeout:
            print(f"  ⏰ 请求超时 (重试 {attempt+1}/{MAX_RETRIES})")
            time.sleep(5)
        except Exception as e:
            print(f"  🌐 网络错误: {e} (重试 {attempt+1}/{MAX_RETRIES})")
            time.sleep(5)
    return None

# ============================================================
# 从桌游数据中提取所有出版商 ID
# ============================================================
def collect_publisher_ids(conn):
    """遍历所有桌游的 JSON，提取不重复的出版商 ID 和名字"""
    c = conn.cursor()
    c.execute("SELECT game_id, json_data FROM games_raw")
    
    publishers = {}  # id -> {name, is_primary_for: [game_ids]}
    
    for row in c.fetchall():
        game_id = row[0]
        try:
            data = json.loads(row[1])
            pubs = data["item"].get("links", {}).get("boardgamepublisher", [])
            for p in pubs:
                pid = str(p.get("objectid", ""))
                if not pid:
                    continue
                if pid not in publishers:
                    publishers[pid] = {
                        "name": p.get("name", ""),
                        "is_primary_count": 0,
                        "total_count": 0,
                    }
                publishers[pid]["total_count"] += 1
                if p.get("primarylink") == 1:
                    publishers[pid]["is_primary_count"] += 1
        except:
            pass
    
    return publishers

# ============================================================
# 主流程
# ============================================================
def main():
    print("🚀 桌游星图 - 第 3 轮：出版商详情采集")
    conn = init_db()
    
    # 1. 收集所有出版商 ID
    print("\n📦 从桌游数据中提取出版商 ID...")
    publishers = collect_publisher_ids(conn)
    print(f"  共发现 {len(publishers)} 个不重复的出版商")
    
    # 统计
    primary_count = sum(1 for p in publishers.values() if p["is_primary_count"] > 0)
    print(f"  其中 {primary_count} 个曾作为原始出版商 (primarylink=1)")
    
    # 按关联游戏数排序，展示 Top 10
    top_pubs = sorted(publishers.items(), key=lambda x: x[1]["total_count"], reverse=True)
    print(f"\n  📊 关联游戏数 Top 10 出版商:")
    for pid, info in top_pubs[:10]:
        primary_tag = " ⭐" if info["is_primary_count"] > 0 else ""
        print(f"    ID={pid:>6s}  关联{info['total_count']:>4d}款  {info['name']}{primary_tag}")
    
    # 2. 过滤已抓取的（断点续传）
    c = conn.cursor()
    c.execute("SELECT publisher_id FROM publishers_raw")
    already_fetched = set(row[0] for row in c.fetchall())
    
    todo_ids = [pid for pid in publishers.keys() if pid not in already_fetched]
    skipped = len(publishers) - len(todo_ids)
    
    if skipped > 0:
        print(f"\n⏭️  跳过已抓取的 {skipped} 个，剩余 {len(todo_ids)} 个")
    
    if not todo_ids:
        print("\n🎉 所有出版商都已抓取完毕！")
        show_summary(conn, publishers)
        conn.close()
        return
    
    # 3. 逐个抓取
    print(f"\n🎯 开始抓取 {len(todo_ids)} 个出版商详情...\n")
    
    for i, pid in enumerate(todo_ids):
        pub_name = publishers.get(pid, {}).get("name", "???")
        print(f"[{i+1}/{len(todo_ids)}] 🔍 ID={pid} {pub_name}...")
        
        url = f"https://api.geekdo.com/api/geekitems?objectid={pid}&objecttype=company"
        data = fetch_json(url)
        
        if data and "item" in data:
            item = data["item"]
            name = item.get("name", "???")
            
            # 检查有没有中文别名
            alt_names = item.get("alternatenames", [])
            zh_names = []
            for n in alt_names:
                n_str = n.get("name", "")
                if n_str and any('\u4e00' <= ch <= '\u9fff' for ch in n_str):
                    zh_names.append(n_str)
            
            zh_tag = f" 中文: {zh_names[0]}" if zh_names else ""
            print(f"  ✅ {name}{zh_tag}")
            
            # 保存
            c.execute("INSERT OR REPLACE INTO publishers_raw (publisher_id, json_data) VALUES (?, ?)",
                      (pid, json.dumps(data, ensure_ascii=False)))
            conn.commit()
        else:
            print(f"  ❌ 抓取失败")
        
        time.sleep(SLEEP_BETWEEN)
        
        # 每 100 个打印进度
        if (i + 1) % 100 == 0:
            print(f"\n  📈 进度: {i+1}/{len(todo_ids)}\n")
    
    show_summary(conn, publishers)
    conn.close()

def show_summary(conn, publishers):
    """最终统计"""
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM publishers_raw")
    total = c.fetchone()[0]
    
    # 统计有中文名的
    c.execute("SELECT publisher_id, json_data FROM publishers_raw")
    has_zh = 0
    has_description = 0
    for row in c.fetchall():
        try:
            data = json.loads(row[1])
            item = data["item"]
            
            for n in item.get("alternatenames", []):
                if n.get("name") and any('\u4e00' <= ch <= '\u9fff' for ch in n["name"]):
                    has_zh += 1
                    break
            
            desc = item.get("description", "")
            if desc and len(desc) > 20:
                has_description += 1
        except:
            pass
    
    print("\n" + "=" * 60)
    print("🏆 出版商采集完毕！")
    print(f"  📦 总出版商数: {total}")
    print(f"  🇨🇳 有中文名: {has_zh}")
    print(f"  📝 有描述: {has_description}")
    print(f"  💾 数据表: publishers_raw in {DB_PATH}")
    print("=" * 60)

if __name__ == "__main__":
    main()