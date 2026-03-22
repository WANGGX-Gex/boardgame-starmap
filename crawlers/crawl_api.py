"""
桌游星图 — BGG XML API2 爬虫（主力）
支持批量查询（每批最多 20 个 ID），效率约为旧版 10 倍

用法：python3 crawl_api.py
输入：data/top_1000_ids.txt + data/bgg_starmap.db（已有数据）
输出：data/bgg_starmap.db（更新 games_raw, games_dynamic, discovered_ids）
"""

import os
import sys
import sqlite3
import json
import time
import xml.etree.ElementTree as ET
import requests
import yaml

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

# ============================================================
# 加载配置
# ============================================================
def load_config():
    cfg_path = os.path.join(BASE_DIR, 'config', 'config.yaml')
    if not os.path.exists(cfg_path):
        print("❌ 找不到 config/config.yaml，请从 config.example.yaml 复制并填入 Token")
        sys.exit(1)
    with open(cfg_path, 'r') as f:
        return yaml.safe_load(f)

CFG = load_config()
BGG = CFG['bgg']
TOKEN = BGG['xml_api_token']
API_BASE = BGG['xml_api_base']
SLEEP = BGG.get('sleep_between', 1.5)
SLEEP_ERR = BGG.get('sleep_on_error', 15)
MAX_RETRIES = BGG.get('max_retries', 3)
BATCH_SIZE = BGG.get('batch_size', 20)

DB_PATH = os.path.join(BASE_DIR, 'data', 'bgg_starmap.db')
IDS_FILE = os.path.join(BASE_DIR, 'data', 'top_1000_ids.txt')

HEADERS = {
    'Authorization': f'Bearer {TOKEN}',
    'User-Agent': 'BoardGame_StarMap_v1.0',
}

# ============================================================
# 数据库
# ============================================================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS games_raw (
        game_id TEXT PRIMARY KEY, json_data TEXT,
        fetch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS games_dynamic (
        game_id TEXT PRIMARY KEY, json_data TEXT,
        fetch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    c.execute('''CREATE TABLE IF NOT EXISTS discovered_ids (
        game_id TEXT PRIMARY KEY, source_game_id TEXT, relation_type TEXT,
        discovered_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    return conn

def get_fetched_ids(conn, table):
    c = conn.cursor()
    c.execute(f"SELECT game_id FROM {table}")
    return set(row[0] for row in c.fetchall())

def save_game(conn, game_id, raw_json, dynamic_json):
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO games_raw (game_id, json_data) VALUES (?, ?)",
              (game_id, json.dumps(raw_json, ensure_ascii=False)))
    c.execute("INSERT OR REPLACE INTO games_dynamic (game_id, json_data) VALUES (?, ?)",
              (game_id, json.dumps(dynamic_json, ensure_ascii=False)))
    conn.commit()

def save_discovered(conn, game_id, source_id, relation_type):
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO discovered_ids (game_id, source_game_id, relation_type) VALUES (?, ?, ?)",
              (game_id, source_id, relation_type))

# ============================================================
# XML API 请求 + 解析
# ============================================================
def fetch_xml(ids_list):
    """批量获取桌游详情，返回 XML ElementTree root 或 None"""
    ids_str = ','.join(ids_list)
    url = f"{API_BASE}/thing?id={ids_str}&stats=1&type=boardgame,boardgameexpansion"

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return ET.fromstring(r.content)
            elif r.status_code in (401, 403):
                print(f"  ❌ 认证失败 (HTTP {r.status_code})，Token 可能已过期")
                return None
            elif r.status_code == 429:
                print(f"  ⚠️ 限速 429，等待 {SLEEP_ERR}s...")
                time.sleep(SLEEP_ERR)
            elif r.status_code == 202:
                # BGG 返回 202 表示请求已排队，需要稍后重试
                print(f"  ⏳ 202 排队中，等待 5s...")
                time.sleep(5)
            else:
                print(f"  ❌ HTTP {r.status_code}")
                time.sleep(5)
        except Exception as e:
            print(f"  🌐 网络错误: {e} (重试 {attempt+1}/{MAX_RETRIES})")
            time.sleep(5)

    return None

def xml_to_geekdo_format(item_elem):
    """
    将 XML API 的 <item> 元素转换为与旧版 geekdo JSON API 兼容的格式
    这样 clean_data_v3.py 不需要修改解析逻辑
    """
    game_id = item_elem.get('id')
    item_type = item_elem.get('type', 'boardgame')

    # 基本信息
    primary_name = ''
    alternatenames = []
    for name_elem in item_elem.findall('name'):
        if name_elem.get('type') == 'primary':
            primary_name = name_elem.get('value', '')
        else:
            alternatenames.append({'name': name_elem.get('value', '')})

    def get_val(tag, default=''):
        e = item_elem.find(tag)
        return e.get('value', default) if e is not None else default

    # 图片
    image = ''
    img_elem = item_elem.find('image')
    if img_elem is not None and img_elem.text:
        image = img_elem.text.strip()

    # 描述
    desc = ''
    desc_elem = item_elem.find('description')
    if desc_elem is not None and desc_elem.text:
        desc = desc_elem.text.strip()

    # 关联关系（links）
    links = {}
    link_type_map = {
        'boardgamecategory': 'boardgamecategory',
        'boardgamemechanic': 'boardgamemechanic',
        'boardgamedesigner': 'boardgamedesigner',
        'boardgameartist': 'boardgameartist',
        'boardgamepublisher': 'boardgamepublisher',
        'boardgameexpansion': None,  # 特殊处理
        'boardgameimplementation': None,  # 特殊处理
        'boardgameintegration': 'boardgameintegration',
        'boardgamecompilation': None,  # 特殊处理
    }

    for link_elem in item_elem.findall('link'):
        lt = link_elem.get('type', '')
        lid = link_elem.get('id', '')
        lname = link_elem.get('value', '')
        inbound = link_elem.get('inbound', 'false') == 'true'

        entry = {'objectid': lid, 'name': lname, 'objecttype': 'thing'}

        if lt == 'boardgamepublisher':
            # XML API 没有 primarylink 标记，默认都设为非 primary
            # 后续会用旧版数据或出版商详情补充
            links.setdefault('boardgamepublisher', []).append(entry)

        elif lt == 'boardgameexpansion':
            if inbound:
                links.setdefault('expandsboardgame', []).append(entry)
            else:
                links.setdefault('boardgameexpansion', []).append(entry)

        elif lt == 'boardgameimplementation':
            if inbound:
                links.setdefault('reimplementation', []).append(entry)
            else:
                links.setdefault('reimplements', []).append(entry)

        elif lt == 'boardgamecompilation':
            if inbound:
                links.setdefault('containedin', []).append(entry)
            else:
                links.setdefault('contains', []).append(entry)

        elif lt in link_type_map and link_type_map[lt]:
            links.setdefault(link_type_map[lt], []).append(entry)

    # 构建与旧版 geekdo JSON 兼容的格式
    raw_data = {
        'item': {
            'name': primary_name,
            'alternatenames': alternatenames,
            'yearpublished': get_val('yearpublished'),
            'minplayers': get_val('minplayers'),
            'maxplayers': get_val('maxplayers'),
            'minplaytime': get_val('minplaytime'),
            'maxplaytime': get_val('maxplaytime'),
            'subtype': item_type.replace('boardgame', 'boardgame'),
            'imageurl': image,
            'short_description': desc[:500] if desc else '',
            'description': desc,
            'links': links,
        }
    }

    # 统计数据 → 构建与旧版 dynamicinfo 兼容的格式
    stats_elem = item_elem.find('.//statistics/ratings')
    dynamic_data = {'item': {'rankinfo': [], 'stats': {}, 'polls': {}}}

    if stats_elem is not None:
        def stat_val(tag, as_type=float):
            e = stats_elem.find(tag)
            if e is not None:
                v = e.get('value', '0')
                try:
                    return as_type(v)
                except (ValueError, TypeError):
                    return 0
            return 0

        dynamic_data['item']['stats'] = {
            'average': stat_val('average'),
            'usersrated': stat_val('usersrated', int),
            'avgweight': stat_val('averageweight'),
            'numowned': stat_val('owned', int),
        }

        # 排名
        ranks_elem = stats_elem.find('ranks')
        if ranks_elem is not None:
            for rank_elem in ranks_elem.findall('rank'):
                if rank_elem.get('type') == 'subtype':
                    rank_val = rank_elem.get('value', '0')
                    bavg = rank_elem.get('bayesaverage', '0')
                    dynamic_data['item']['rankinfo'].append({
                        'rank': int(rank_val) if rank_val.isdigit() else 0,
                        'baverage': float(bavg) if bavg.replace('.','',1).isdigit() else 0,
                    })

    # 最佳人数投票
    for poll in item_elem.findall('poll'):
        if poll.get('name') == 'suggested_numplayers':
            best_list = []
            for results in poll.findall('results'):
                numplayers = results.get('numplayers', '')
                best_votes = 0
                for result in results.findall('result'):
                    if result.get('value') == 'Best':
                        best_votes = int(result.get('numvotes', '0'))
                if best_votes > 0:
                    try:
                        np = numplayers.replace('+', '')
                        best_list.append({'min': int(np), 'max': int(np), 'votes': best_votes})
                    except ValueError:
                        pass
            if best_list:
                best_list.sort(key=lambda x: x['votes'], reverse=True)
                dynamic_data['item']['polls'] = {
                    'userplayers': {'best': [{'min': b['min'], 'max': b['max']} for b in best_list[:3]]}
                }

    return game_id, raw_data, dynamic_data

def extract_related_ids(raw_data, source_id):
    """从转换后的 JSON 中提取关联桌游 ID"""
    related = []
    links = raw_data.get('item', {}).get('links', {})

    relation_map = {
        'boardgameexpansion': 'expansion',
        'expandsboardgame': 'expands',
        'reimplementation': 'reimplemented_by',
        'reimplements': 'reimplements',
        'boardgameintegration': 'integrates',
        'contains': 'contains',
        'containedin': 'contained_in',
    }

    for link_type, rel_name in relation_map.items():
        for item in links.get(link_type, []):
            rid = str(item.get('objectid', ''))
            if rid:
                related.append({'id': rid, 'relation': rel_name, 'source_id': source_id})

    return related

# ============================================================
# 主流程
# ============================================================
def load_target_ids():
    ids = []
    if not os.path.exists(IDS_FILE):
        print(f"❌ 找不到 {IDS_FILE}")
        sys.exit(1)
    with open(IDS_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                ids.append(line.split(',')[0].strip())
    print(f"📦 加载了 {len(ids)} 个目标 ID")
    return ids

def crawl_batch(conn, target_ids, batch_name="桌游"):
    """批量采集一组桌游"""
    already = get_fetched_ids(conn, 'games_raw')
    todo = [gid for gid in target_ids if gid not in already]
    skipped = len(target_ids) - len(todo)

    if skipped > 0:
        print(f"⏭️  跳过已有 {skipped} 个，剩余 {len(todo)} 个")
    if not todo:
        print(f"🎉 {batch_name}全部已完成！")
        return []

    print(f"🎯 开始采集 {batch_name}：{len(todo)} 个\n")
    all_discovered = []

    for i in range(0, len(todo), BATCH_SIZE):
        batch = todo[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(todo) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"[批次 {batch_num}/{total_batches}] 🔍 获取 {len(batch)} 个游戏...")

        root = fetch_xml(batch)
        if root is None:
            print("  ❌ 批次获取失败，Token 可能已过期")
            raise ConnectionError("XML API 认证失败")

        items = root.findall('item')
        for item_elem in items:
            try:
                game_id, raw_data, dynamic_data = xml_to_geekdo_format(item_elem)
                save_game(conn, game_id, raw_data, dynamic_data)
                name = raw_data['item']['name']
                print(f"  ✅ {name} (ID={game_id})")

                related = extract_related_ids(raw_data, game_id)
                for r in related:
                    save_discovered(conn, r['id'], r['source_id'], r['relation'])
                all_discovered.extend(related)
            except Exception as e:
                print(f"  ⚠️ 解析异常: {e}")

        conn.commit()
        if i + BATCH_SIZE < len(todo):
            time.sleep(SLEEP)

    print(f"\n🏁 {batch_name}采集完毕，发现 {len(all_discovered)} 个关联桌游")
    return all_discovered

def main():
    print("🚀 桌游星图 - XML API2 数据采集")
    print(f"   Token: {TOKEN[:8]}...{TOKEN[-4:]}")
    print(f"   批量大小: {BATCH_SIZE}\n")

    conn = init_db()
    core_ids = load_target_ids()

    # 第 1 轮：核心桌游
    print("=" * 60)
    print("📡 第 1 轮：Top 1000 核心桌游")
    print("=" * 60)
    crawl_batch(conn, core_ids, "Top 1000 核心桌游")

    # 第 2 轮：关联桌游
    print("\n" + "=" * 60)
    print("📡 第 2 轮：关联桌游")
    print("=" * 60)
    already = get_fetched_ids(conn, 'games_raw')
    c = conn.cursor()
    c.execute("SELECT DISTINCT game_id FROM discovered_ids")
    discovered = set(row[0] for row in c.fetchall())
    todo_2 = list(discovered - already)

    if todo_2:
        print(f"🔍 发现 {len(todo_2)} 个待补充的关联桌游")
        crawl_batch(conn, todo_2, "关联桌游")
    else:
        print("✨ 无需补充")

    # 统计
    c = conn.cursor()
    for t in ['games_raw', 'games_dynamic', 'discovered_ids']:
        c.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"  📦 {t}: {c.fetchone()[0]} 条")

    conn.close()
    print("\n✅ XML API 采集完成")

if __name__ == "__main__":
    main()
