"""
桌游星图 — 补查 primarylink（轻量模式）
只对 XML API 新增的、缺少 primarylink 数据的游戏，
用旧版 geekdo JSON API 补查一次详情。

用法：python3 crawlers/patch_primary.py
通常每周一运行一次即可。
"""

import os
import sys
import sqlite3
import json
import time
import cloudscraper

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'bgg_starmap.db')

SLEEP_BETWEEN = 2
SLEEP_ON_ERROR = 15
MAX_RETRIES = 3

scraper = cloudscraper.create_scraper(browser={
    'browser': 'chrome', 'platform': 'windows', 'desktop': True
})

def fetch_json(url):
    for attempt in range(MAX_RETRIES):
        try:
            r = scraper.get(url, timeout=20)
            if r.status_code == 200 and 'json' in r.headers.get('content-type', ''):
                return r.json()
            elif r.status_code == 429:
                print(f"  ⚠️ 429 限速，等待 {SLEEP_ON_ERROR}s...")
                time.sleep(SLEEP_ON_ERROR)
            elif r.status_code == 404:
                return None
            else:
                print(f"  ❌ HTTP {r.status_code}")
                time.sleep(5)
        except Exception as e:
            print(f"  🌐 网络错误: {e} (重试 {attempt+1}/{MAX_RETRIES})")
            time.sleep(5)
    return None


def find_games_missing_primary(conn):
    """
    找出 games_raw 中没有 primarylink 标记的游戏。
    旧版 geekdo JSON 的出版商有 "primarylink":1 字段，
    XML API 转换的数据没有这个字段。
    """
    c = conn.cursor()
    c.execute("SELECT game_id, json_data FROM games_raw")

    missing = []
    for game_id, json_text in c.fetchall():
        try:
            data = json.loads(json_text)
            publishers = data.get('item', {}).get('links', {}).get('boardgamepublisher', [])
            # 如果任何一个出版商有 primarylink 字段，说明是旧版数据，不需要补查
            has_primary = any('primarylink' in p for p in publishers)
            if not has_primary and publishers:
                missing.append(game_id)
        except:
            continue

    return missing


def main():
    print("🔧 桌游星图 — 补查 primarylink")

    conn = sqlite3.connect(DB_PATH)
    missing = find_games_missing_primary(conn)

    if not missing:
        print("✨ 所有游戏都已有 primarylink 数据，无需补查")
        conn.close()
        return

    print(f"📋 发现 {len(missing)} 个游戏缺少 primarylink，开始补查...\n")

    c = conn.cursor()
    patched = 0

    for i, game_id in enumerate(missing):
        print(f"[{i+1}/{len(missing)}] 🔍 补查 ID={game_id}...")

        # 用旧版 geekdo API 获取完整详情
        url = f"https://api.geekdo.com/api/geekitems?objectid={game_id}&objecttype=thing"
        data = fetch_json(url)

        if data and 'item' in data:
            name = data['item'].get('name', '???')
            # 用旧版数据完整替换 games_raw（因为旧版包含 primarylink）
            c.execute("INSERT OR REPLACE INTO games_raw (game_id, json_data) VALUES (?, ?)",
                      (game_id, json.dumps(data, ensure_ascii=False)))

            # 同时补查 dynamicinfo
            dyn_url = f"https://api.geekdo.com/api/dynamicinfo?objectid={game_id}&objecttype=thing"
            dyn = fetch_json(dyn_url)
            if dyn and 'item' in dyn:
                c.execute("INSERT OR REPLACE INTO games_dynamic (game_id, json_data) VALUES (?, ?)",
                          (game_id, json.dumps(dyn, ensure_ascii=False)))

            conn.commit()
            print(f"  ✅ {name}")
            patched += 1
        else:
            print(f"  ❌ 获取失败")

        time.sleep(SLEEP_BETWEEN)

    conn.close()
    print(f"\n🏁 补查完成：{patched}/{len(missing)} 个游戏已补充 primarylink")


if __name__ == "__main__":
    main()
