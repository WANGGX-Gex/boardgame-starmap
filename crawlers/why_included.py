#!/usr/bin/env python3
"""
桌游星图 — 调试工具：查询某个游戏为什么在星图中
从数据库层面检查它的关联关系、被谁引用、怎么被发现的

用法：python3 crawlers/why_included.py <game_id>
示例：python3 crawlers/why_included.py 10        # 查 Elfenland
"""
import json, sys, os, sqlite3

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'bgg_starmap.db')

GAME_RELATION_KEYS = [
    'boardgameexpansion', 'expandsboardgame',
    'reimplements', 'reimplementation',
    'boardgameintegration',
    'contains', 'containedin',
]

def main():
    if len(sys.argv) < 2:
        print("用法: python3 crawlers/why_included.py <game_id>")
        sys.exit(1)

    target = sys.argv[1]
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 1. 基本信息
    c.execute("SELECT json_data FROM games_raw WHERE game_id=?", (target,))
    row = c.fetchone()
    if not row:
        print(f"❌ ID={target} 不在 games_raw 中")
        return

    data = json.loads(row[0])
    item = data.get('item', {})
    name = item.get('name', '?')
    links = item.get('links', {}) or {}

    # dynamic
    c.execute("SELECT json_data FROM games_dynamic WHERE game_id=?", (target,))
    drow = c.fetchone()
    rank, rated = 0, 0
    if drow:
        dyn = json.loads(drow[0])
        ri = dyn.get('item', {}).get('rankinfo', [])
        stats = dyn.get('item', {}).get('stats', {})
        rank = ri[0].get('rank', 0) if ri else 0
        rated = stats.get('usersrated', 0)

    # top 1000?
    ids_file = os.path.join(BASE_DIR, 'data', 'top_1000_ids.txt')
    top_ids = set()
    if os.path.exists(ids_file):
        with open(ids_file) as f:
            top_ids = set(line.strip() for line in f if line.strip())

    is_core = target in top_ids
    print(f"🎯 {name} (ID={target})")
    print(f"   排名: #{rank}  评价人数: {rated}  核心桌游: {'是 ⭐' if is_core else '否'}")

    # 2. 主动引用
    print(f"\n📤 它主动引用的游戏关联:")
    outgoing = 0
    for rel_key in GAME_RELATION_KEYS:
        for linked in (links.get(rel_key, []) or []):
            lid = str(linked.get('objectid', '')).strip()
            lname = linked.get('name', '?')
            if lid:
                # 检查目标是否在数据库中
                c.execute("SELECT 1 FROM games_raw WHERE game_id=?", (lid,))
                in_db = "✅" if c.fetchone() else "❌ 不在DB"
                print(f"   → [{rel_key}] {lname} (ID={lid}) {in_db}")
                outgoing += 1
    if outgoing == 0:
        print(f"   （无）")

    # 3. 被谁引用
    print(f"\n📥 引用了它的游戏:")
    incoming = 0
    c.execute("SELECT game_id, json_data FROM games_raw")
    for gid, json_text in c.fetchall():
        gid = str(gid)
        if gid == target:
            continue
        try:
            gdata = json.loads(json_text)
            glinks = gdata.get('item', {}).get('links', {}) or {}
            for rel_key in GAME_RELATION_KEYS:
                for linked in (glinks.get(rel_key, []) or []):
                    lid = str(linked.get('objectid', '')).strip()
                    if lid == target:
                        gname = gdata.get('item', {}).get('name', '?')
                        is_top = " ⭐TOP1000" if gid in top_ids else ""
                        print(f"   ← [{rel_key}] {gname} (ID={gid}){is_top}")
                        incoming += 1
        except:
            continue
    if incoming == 0:
        print(f"   （无）")

    # 4. discovered_ids
    c.execute("SELECT source_game_id, relation_type FROM discovered_ids WHERE game_id=?", (target,))
    disc = c.fetchall()
    if disc:
        print(f"\n📋 discovered_ids 记录:")
        for src, rel in disc:
            c.execute("SELECT json_data FROM games_raw WHERE game_id=?", (src,))
            r = c.fetchone()
            src_name = json.loads(r[0]).get('item', {}).get('name', '?') if r else '?'
            is_top = " ⭐TOP1000" if src in top_ids else ""
            print(f"   发现者: {src_name} (ID={src}){is_top}, 关系: {rel}")
    else:
        print(f"\n📋 discovered_ids 中无记录")

    # 5. 在 graph_data.json 中吗
    graph_path = os.path.join(BASE_DIR, 'output', 'graph_data.json')
    if not os.path.exists(graph_path):
        graph_path = os.path.join(BASE_DIR, 'graph_data.json')
    if os.path.exists(graph_path):
        with open(graph_path, 'r', encoding='utf-8') as f:
            gdata = json.load(f)
        nid = f"g_{target}"
        in_graph = any(n['id'] == nid for n in gdata['nodes'])
        game_links = [l for l in gdata['links']
                      if (l['source'] == nid or l['target'] == nid)
                      and l['source'].startswith('g_') and l['target'].startswith('g_')]
        print(f"\n🌐 在星图 graph_data.json 中: {'是' if in_graph else '否'}")
        if in_graph:
            print(f"   游戏间连线数: {len(game_links)}")
            if game_links == []:
                print(f"   ⚠️ 无游戏间连线（可能因为中间桥梁被评价人数过滤）")

    conn.close()

if __name__ == '__main__':
    main()