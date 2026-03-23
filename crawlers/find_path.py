#!/usr/bin/env python3
"""
桌游星图 — 最短路径查询工具
从数据库构建完整邻接表，查询任意游戏到 Top 1000 的最短路径
（包含因评价人数不足而被星图过滤的中间节点）

用法：python3 crawlers/find_path.py <game_id>
示例：python3 crawlers/find_path.py 10        # 查 Elfenland 到 Top 1000 的最短路径
      python3 crawlers/find_path.py 10 229     # 查两个游戏之间的最短路径
"""
import json, sys, os, sqlite3
from collections import deque

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'bgg_starmap.db')

GAME_RELATION_KEYS = [
    'boardgameexpansion', 'expandsboardgame',
    'reimplements', 'reimplementation',
    'boardgameintegration',
    'contains', 'containedin',
]
MIN_RATINGS = 50

def load_db():
    """从数据库加载所有游戏数据、动态数据、邻接表"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # 加载 games_raw
    all_games = {}
    adjacency = {}
    link_info = {}  # (a, b) → rel_key

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
                if lid:
                    neighbors.add(lid)
                    link_info[(gid, lid)] = rk
        adjacency[gid] = neighbors

    # 双向补全（只保留双方都在数据库中的）
    for gid, nb in list(adjacency.items()):
        for nid in list(nb):
            if nid not in all_games:
                continue
            adjacency.setdefault(nid, set()).add(gid)
            if (nid, gid) not in link_info:
                link_info[(nid, gid)] = link_info.get((gid, nid), '?')

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
    ranked = [(gid, d['rank']) for gid, d in dynamic_map.items() if d['rank'] > 0]
    ranked.sort(key=lambda x: x[1])
    core_ids = set(gid for gid, _ in ranked[:1000])

    conn.close()
    return all_games, adjacency, link_info, dynamic_map, core_ids


def format_node(gid, all_games, dynamic_map, core_ids):
    """格式化节点信息"""
    data = all_games.get(gid, {})
    name = data.get('item', {}).get('name', '?')
    dyn = dynamic_map.get(gid, {})
    rank = dyn.get('rank', 0)
    rated = dyn.get('usersrated', 0)

    tags = []
    if gid in core_ids:
        tags.append('⭐ TOP1000')
    if rated < MIN_RATINGS:
        tags.append(f'⚠️ 低评价({rated}人)')
    
    rank_str = f" rank#{rank}" if rank > 0 else ""
    tags_str = f" {' '.join(tags)}" if tags else ""
    return f"{name} (ID={gid}{rank_str}, {rated}人评价){tags_str}"


def find_path_to_core(target, all_games, adjacency, link_info, dynamic_map, core_ids):
    """BFS 找到 target 到最近核心桌游的最短路径"""
    if target not in all_games:
        print(f"❌ ID={target} 不在数据库中")
        return

    print(f"🎯 {format_node(target, all_games, dynamic_map, core_ids)}")

    if target in core_ids:
        print(f"\n✅ 它本身就是核心桌游！")
        return

    dist = {target: 0}
    parent = {target: None}
    queue = deque([target])
    found = None

    while queue:
        gid = queue.popleft()
        if gid in core_ids and gid != target:
            found = gid
            break
        for nb in adjacency.get(gid, []):
            if nb not in dist and nb in all_games:
                dist[nb] = dist[gid] + 1
                parent[nb] = gid
                queue.append(nb)

    if not found:
        print(f"\n❌ 找不到到任何核心桌游的路径")
        neighbors = adjacency.get(target, set())
        if neighbors:
            print(f"   它有 {len(neighbors)} 个游戏邻居（都不在数据库中或都不可达核心桌游）")
        else:
            print(f"   它没有任何游戏间关联")
        return

    # 回溯路径
    path = []
    cur = found
    while cur is not None:
        path.append(cur)
        cur = parent[cur]
    path.reverse()

    print(f"\n🔍 最短路径（{len(path)-1} 步）:\n")
    print(f"  {format_node(path[0], all_games, dynamic_map, core_ids)}")
    for i in range(1, len(path)):
        prev, curr = path[i-1], path[i]
        rel = link_info.get((prev, curr)) or link_info.get((curr, prev)) or '?'
        print(f"  → [{rel}]")
        print(f"  {format_node(curr, all_games, dynamic_map, core_ids)}")


def find_path_between(id_a, id_b, all_games, adjacency, link_info, dynamic_map, core_ids):
    """BFS 找两个游戏之间的最短路径"""
    if id_a not in all_games:
        print(f"❌ ID={id_a} 不在数据库中"); return
    if id_b not in all_games:
        print(f"❌ ID={id_b} 不在数据库中"); return

    print(f"🎯 起点: {format_node(id_a, all_games, dynamic_map, core_ids)}")
    print(f"🎯 终点: {format_node(id_b, all_games, dynamic_map, core_ids)}")

    dist = {id_a: 0}
    parent = {id_a: None}
    queue = deque([id_a])

    while queue:
        gid = queue.popleft()
        if gid == id_b:
            break
        for nb in adjacency.get(gid, []):
            if nb not in dist and nb in all_games:
                dist[nb] = dist[gid] + 1
                parent[nb] = gid
                queue.append(nb)

    if id_b not in dist:
        print(f"\n❌ 两者之间不存在路径")
        return

    path = []
    cur = id_b
    while cur is not None:
        path.append(cur)
        cur = parent[cur]
    path.reverse()

    print(f"\n🔍 最短路径（{len(path)-1} 步）:\n")
    print(f"  {format_node(path[0], all_games, dynamic_map, core_ids)}")
    for i in range(1, len(path)):
        prev, curr = path[i-1], path[i]
        rel = link_info.get((prev, curr)) or link_info.get((curr, prev)) or '?'
        print(f"  → [{rel}]")
        print(f"  {format_node(curr, all_games, dynamic_map, core_ids)}")


def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  python3 crawlers/find_path.py <game_id>          # 到 Top 1000 最短路径")
        print("  python3 crawlers/find_path.py <id_a> <id_b>      # 两个游戏间最短路径")
        print("示例:")
        print("  python3 crawlers/find_path.py 10                 # Elfenland → Top 1000")
        print("  python3 crawlers/find_path.py 10 229             # Elfenland → King of the Elves")
        sys.exit(1)

    print(f"📦 加载数据库 {DB_PATH}...")
    all_games, adjacency, link_info, dynamic_map, core_ids = load_db()
    print(f"   {len(all_games)} 个游戏, {len(core_ids)} 个核心桌游\n")

    if len(sys.argv) == 2:
        find_path_to_core(sys.argv[1], all_games, adjacency, link_info, dynamic_map, core_ids)
    else:
        find_path_between(sys.argv[1], sys.argv[2], all_games, adjacency, link_info, dynamic_map, core_ids)


if __name__ == '__main__':
    main()