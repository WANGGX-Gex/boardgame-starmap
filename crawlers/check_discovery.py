#!/usr/bin/env python3
"""快速查询某个游戏是通过什么路径进入数据库的"""
import json, sqlite3, os, sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'bgg_starmap.db')

target = sys.argv[1] if len(sys.argv) > 1 else '147541'

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# 1. discovered_ids 记录
print(f"📋 discovered_ids 中关于 ID={target} 的记录:")
c.execute("SELECT source_game_id, relation_type FROM discovered_ids WHERE game_id=?", (target,))
rows = c.fetchall()
if rows:
    for src, rel in rows:
        c.execute("SELECT json_data FROM games_raw WHERE game_id=?", (src,))
        r = c.fetchone()
        src_name = json.loads(r[0]).get('item', {}).get('name', '?') if r else '?'
        print(f"   发现者: {src_name} (ID={src}), 关系: {rel}")
else:
    print(f"   （无记录 — 可能是 Top 1000 核心桌游，或手动添加的）")

# 2. 哪些游戏的 links 里通过非 contains 关系引用了它
print(f"\n🔍 谁通过 expansion/reimplements/integrates 引用了 ID={target}:")
NON_CONTAINS_KEYS = [
    'boardgameexpansion', 'expandsboardgame',
    'reimplements', 'reimplementation',
    'boardgameintegration',
]
c.execute("SELECT game_id, json_data FROM games_raw")
found = 0
for gid, jt in c.fetchall():
    gid = str(gid)
    try:
        data = json.loads(jt)
        links = data.get('item', {}).get('links', {}) or {}
        for rk in NON_CONTAINS_KEYS:
            for l in (links.get(rk, []) or []):
                lid = str(l.get('objectid', '')).strip()
                if lid == target:
                    gname = data.get('item', {}).get('name', '?')
                    print(f"   ← [{rk}] {gname} (ID={gid})")
                    found += 1
    except:
        continue
if found == 0:
    print(f"   （无 — 它不是通过这些关系被发现的）")

conn.close()
