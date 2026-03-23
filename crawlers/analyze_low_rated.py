#!/usr/bin/env python3
"""
分析：有多少桌游在 BFS 可达范围内但因评价人数不足被过滤
"""
import json, sqlite3, os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'bgg_starmap.db')

GAME_RELATION_KEYS = [
    'boardgameexpansion', 'expandsboardgame',
    'reimplements', 'reimplementation',
    'boardgameintegration',
    'contains', 'containedin',
]
MIN_RATINGS = 50
MAX_DISTANCE = 2

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# 加载 dynamic
dynamic_map = {}
c.execute("SELECT game_id, json_data FROM games_dynamic")
for gid, jt in c.fetchall():
    try:
        d = json.loads(jt)
        stats = d.get('item', {}).get('stats', {})
        ri = d.get('item', {}).get('rankinfo', [])
        dynamic_map[str(gid)] = {
            'usersrated': int(stats.get('usersrated', 0)),
            'rank': int(ri[0].get('rank', 0)) if ri else 0,
        }
    except:
        continue

# 核心桌游
core_ids = set()
ranked = [(gid, d['rank']) for gid, d in dynamic_map.items() if d['rank'] > 0]
ranked.sort(key=lambda x: x[1])
core_ids = set(gid for gid, _ in ranked[:1000])

# 加载 games_raw + 邻接表
all_games = {}
adjacency = {}
c.execute("SELECT game_id, json_data FROM games_raw")
for gid, jt in c.fetchall():
    gid = str(gid)
    try:
        data = json.loads(jt)
    except:
        continue
    all_games[gid] = data
    links = data.get('item', {}).get('links', {}) or {}
    neighbors = set()
    for rk in GAME_RELATION_KEYS:
        for l in (links.get(rk, []) or []):
            lid = str(l.get('objectid', '')).strip()
            if lid and lid in all_games:
                neighbors.add(lid)
    adjacency[gid] = neighbors

# 双向
for gid, nb in list(adjacency.items()):
    for nid in nb:
        adjacency.setdefault(nid, set()).add(gid)

# BFS
from collections import deque
distance = {}
queue = deque()
for cid in core_ids:
    if cid in all_games:
        distance[cid] = 0
        queue.append(cid)
while queue:
    gid = queue.popleft()
    if distance[gid] >= MAX_DISTANCE:
        continue
    for nb in adjacency.get(gid, []):
        if nb not in distance and nb in all_games:
            distance[nb] = distance[gid] + 1
            queue.append(nb)

# 统计
reachable = set(distance.keys())
low_rated_filtered = 0
low_rated_bridge = 0  # 低评价但连接了两端都是高评价的

for gid in reachable:
    if gid in core_ids:
        continue
    rated = dynamic_map.get(gid, {}).get('usersrated', 0)
    if rated < MIN_RATINGS:
        low_rated_filtered += 1

# 分评价人数区间统计
buckets = {'0': 0, '1-9': 0, '10-49': 0, '50-99': 0, '100-499': 0, '500+': 0}
for gid in reachable:
    if gid in core_ids:
        continue
    rated = dynamic_map.get(gid, {}).get('usersrated', 0)
    if rated == 0: buckets['0'] += 1
    elif rated < 10: buckets['1-9'] += 1
    elif rated < 50: buckets['10-49'] += 1
    elif rated < 100: buckets['50-99'] += 1
    elif rated < 500: buckets['100-499'] += 1
    else: buckets['500+'] += 1

print(f"📊 BFS 可达的非核心桌游: {len(reachable) - len(core_ids)} 个")
print(f"   其中评价人数 < {MIN_RATINGS}: {low_rated_filtered} 个")
print(f"\n📈 评价人数分布:")
for bucket, count in buckets.items():
    print(f"   {bucket:>8s}: {count:>5d} 个")

print(f"\n💡 如果去掉评价人数过滤，星图会多 {low_rated_filtered} 个桌游节点")
print(f"   当前纳入: {len(reachable) - low_rated_filtered - len(core_ids)} + 1000 核心 = {len(reachable) - low_rated_filtered} 个")
print(f"   去掉过滤: {len(reachable) - len(core_ids)} + 1000 核心 = {len(reachable)} 个")

conn.close()
