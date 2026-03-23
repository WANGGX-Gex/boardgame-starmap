"""
桌游星图 - 第 4 轮：抓取人物详情（Designer + Artist）
用法：python3 crawl_persons.py
"""
import os
import sqlite3
import json
import time
import cloudscraper
import requests

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "bgg_starmap.db")
SLEEP_BETWEEN = 2
SLEEP_ON_ERROR = 15
MAX_RETRIES = 3

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS persons_raw (
        person_id TEXT PRIMARY KEY,
        json_data TEXT,
        roles TEXT,
        fetch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    conn.commit()
    return conn

scraper = cloudscraper.create_scraper(browser={
    'browser': 'chrome', 'platform': 'windows', 'desktop': True
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

def collect_person_ids(conn):
    """从桌游数据中提取所有 Designer 和 Artist 的 ID"""
    c = conn.cursor()
    c.execute("SELECT game_id, json_data FROM games_raw")
    
    persons = {}  # id -> {name, roles: set()}
    
    for row in c.fetchall():
        try:
            data = json.loads(row[1])
            links = data["item"].get("links", {})
            
            for d in links.get("boardgamedesigner", []):
                pid = str(d.get("objectid", ""))
                if pid:
                    if pid not in persons:
                        persons[pid] = {"name": d.get("name", ""), "roles": set(), "game_count": 0}
                    persons[pid]["roles"].add("designer")
                    persons[pid]["game_count"] += 1
            
            for a in links.get("boardgameartist", []):
                pid = str(a.get("objectid", ""))
                if pid:
                    if pid not in persons:
                        persons[pid] = {"name": a.get("name", ""), "roles": set(), "game_count": 0}
                    persons[pid]["roles"].add("artist")
                    persons[pid]["game_count"] += 1
        except:
            pass
    
    return persons

def main():
    print("🚀 桌游星图 - 第 4 轮：人物详情采集")
    conn = init_db()
    
    # 1. 收集人物 ID
    print("\n📦 从桌游数据中提取人物 ID...")
    persons = collect_person_ids(conn)
    print(f"  共发现 {len(persons)} 个不重复的人物")
    
    designers = sum(1 for p in persons.values() if "designer" in p["roles"])
    artists = sum(1 for p in persons.values() if "artist" in p["roles"])
    both = sum(1 for p in persons.values() if "designer" in p["roles"] and "artist" in p["roles"])
    print(f"  设计师: {designers} | 美术: {artists} | 双修: {both}")
    
    # Top 10
    top = sorted(persons.items(), key=lambda x: x[1]["game_count"], reverse=True)
    print(f"\n  📊 关联游戏数 Top 10:")
    for pid, info in top[:10]:
        roles_str = "+".join(sorted(info["roles"]))
        print(f"    ID={pid:>7s}  {info['game_count']:>4d}款  [{roles_str:>15s}]  {info['name']}")
    
    # 2. 断点续传
    c = conn.cursor()
    c.execute("SELECT person_id FROM persons_raw")
    already = set(row[0] for row in c.fetchall())
    
    todo = [pid for pid in persons.keys() if pid not in already]
    skipped = len(persons) - len(todo)
    
    if skipped > 0:
        print(f"\n⏭️  跳过已抓取的 {skipped} 个，剩余 {len(todo)} 个")
    
    if not todo:
        print("\n🎉 所有人物都已抓取完毕！")
        conn.close()
        return
    
    # 3. 逐个抓取
    print(f"\n🎯 开始抓取 {len(todo)} 个人物详情...\n")
    
    for i, pid in enumerate(todo):
        person_name = persons.get(pid, {}).get("name", "???")
        roles = persons.get(pid, {}).get("roles", set())
        roles_str = "+".join(sorted(roles))
        
        print(f"[{i+1}/{len(todo)}] 🔍 {person_name} [{roles_str}]...")
        
        url = f"https://api.geekdo.com/api/geekitems?objectid={pid}&objecttype=person"
        data = fetch_json(url)
        
        if data and "item" in data:
            c.execute("INSERT OR REPLACE INTO persons_raw (person_id, json_data, roles) VALUES (?, ?, ?)",
                      (pid, json.dumps(data, ensure_ascii=False), roles_str))
            conn.commit()
            print(f"  ✅ OK")
        else:
            print(f"  ❌ 失败")
        
        time.sleep(SLEEP_BETWEEN)
        
        if (i + 1) % 100 == 0:
            print(f"\n  📈 进度: {i+1}/{len(todo)}\n")
    
    # 统计
    c.execute("SELECT COUNT(*) FROM persons_raw")
    total = c.fetchone()[0]
    print(f"\n🏆 人物采集完毕！共 {total} 人")
    
    conn.close()

if __name__ == "__main__":
    main()