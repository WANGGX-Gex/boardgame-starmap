"""
桌游星图 — 从 BGG 官方 CSV 数据转储获取 Top 1000

数据来源：https://boardgamegeek.com/data_dumps/bg_ranks
  → 需要 BGG 账号密码登录后下载（Bearer Token 不可用于 data dumps）
  → 脚本自动登录 → 解析页面 → 下载 zip → 解压 CSV → 提取 Top 1000

CSV 列：id, name, yearpublished, rank, bayesaverage, average,
        usersrated, is_expansion, abstracts_rank, cgs_rank, ...

用法：
  python3 crawlers/get_top_ids.py            # 自动下载并提取
  python3 crawlers/get_top_ids.py --local    # 仅从本地 CSV 提取（不下载）

配置（config/config.yaml）：
  bgg:
    username: "你的BGG用户名"
    password: "你的BGG密码"
"""
import os
import sys
import csv
import re
import io
import time
import zipfile
import requests
import yaml

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_FILE  = os.path.join(BASE_DIR, 'data', 'boardgames_ranks.csv')
OUTPUT    = os.path.join(BASE_DIR, 'data', 'top_1000_ids.txt')
TOP_N     = 1000

DUMPS_URL = "https://boardgamegeek.com/data_dumps/bg_ranks"
LOGIN_URL = "https://boardgamegeek.com/login/api/v1"

# ============================================================
# 加载配置
# ============================================================
def load_config():
    cfg_path = os.path.join(BASE_DIR, 'config', 'config.yaml')
    if not os.path.exists(cfg_path):
        print("❌ 找不到 config/config.yaml")
        return {}
    with open(cfg_path, 'r') as f:
        return yaml.safe_load(f) or {}

# ============================================================
# BGG 登录（cookie 认证）
# ============================================================
def bgg_login(session, username, password):
    """
    用 BGG 账号密码登录，获取 session cookie。
    BGG 登录 API：POST /login/api/v1
    返回 Set-Cookie: SessionID, bgg_username, bgg_password
    注意：cookie 会被设置两次（一次有效、一次 deleted），
    requests.Session 会自动保留最新值。
    """
    print(f"  🔑 登录 BGG（用户: {username}）...")

    payload = {
        "credentials": {
            "username": username,
            "password": password,
        }
    }
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "BoardGame_StarMap_v1.0",
    }

    try:
        r = session.post(LOGIN_URL, json=payload, headers=headers, timeout=30)

        if r.status_code == 200 or r.status_code == 204:
            # 验证是否真的拿到了有效 cookie
            cookies = session.cookies.get_dict()
            # BGG 返回的 cookie 名是 bggusername（无下划线）
            uname = cookies.get('bggusername') or cookies.get('bgg_username') or ''
            if uname and uname != 'deleted':
                print(f"  ✅ 登录成功")
                return True
            else:
                # 只要有 SessionID 也算成功（cookie 名可能再变）
                if cookies.get('SessionID'):
                    print(f"  ✅ 登录成功（有 SessionID）")
                    return True
                print(f"  ❌ 登录返回了 {r.status_code}，但 cookie 无效")
                print(f"     cookies: {list(cookies.keys())}")
                return False
        elif r.status_code == 401:
            print(f"  ❌ 用户名或密码错误 (HTTP 401)")
            return False
        else:
            print(f"  ❌ 登录失败: HTTP {r.status_code}")
            try:
                print(f"     响应: {r.text[:200]}")
            except:
                pass
            return False
    except Exception as e:
        print(f"  ❌ 登录异常: {e}")
        return False

# ============================================================
# 下载 CSV
# ============================================================
def download_csv(session):
    """
    访问 data_dumps 页面，解析出最新 zip/csv 链接，下载并解压。
    """
    print(f"\n📥 访问 BGG data dumps 页面...")
    headers = {"User-Agent": "BoardGame_StarMap_v1.0"}

    try:
        r = session.get(DUMPS_URL, headers=headers, timeout=30)
        if r.status_code != 200:
            print(f"  ❌ 访问失败: HTTP {r.status_code}")
            if r.status_code in (401, 403):
                print(f"     可能未登录或 session 过期")
            return False

        # 检查是否真的拿到了页面（而非跳转到登录）
        if "Error" in r.text[:200] and "access" in r.text[:200].lower():
            print(f"  ❌ 无权限访问（可能未登录）")
            return False

        # 从页面中提取下载链接
        # 常见格式：href="/data_dumps/bg_ranks?download=boardgames_ranks_2025-03-23.csv.zip"
        #          或：href="...boardgames_ranks...zip"
        #          或：href="...boardgames_ranks...csv"
        zip_links = re.findall(
            r'href="([^"]*boardgames_ranks[^"]*\.zip[^"]*)"', r.text, re.IGNORECASE
        )
        csv_links = re.findall(
            r'href="([^"]*boardgames_ranks[^"]*\.csv[^"]*)"', r.text, re.IGNORECASE
        )
        # 也检查 download 参数式链接
        dl_links = re.findall(
            r'href="([^"]*data_dumps/bg_ranks\?download=[^"]*)"', r.text, re.IGNORECASE
        )

        all_links = zip_links + csv_links + dl_links

        if not all_links:
            # 最后尝试找任何看起来像下载链接的东西
            all_links = re.findall(
                r'href="([^"]*(?:download|ranks)[^"]*(?:\.zip|\.csv)[^"]*)"',
                r.text, re.IGNORECASE
            )

        if not all_links:
            print(f"  ❌ 未找到下载链接")
            print(f"     页面前 500 字符: {r.text[:500]}")
            return False

        # 去重，取最新的（通常日期最大的排最后）
        unique_links = list(dict.fromkeys(all_links))
        # 按日期排序，取最新
        unique_links.sort()
        dl_url = unique_links[-1]

        # HTML 实体解码（页面 href 中 & 会被编码为 &amp;）
        import html
        dl_url = html.unescape(dl_url)

        # 补全 URL
        if dl_url.startswith('/'):
            dl_url = 'https://boardgamegeek.com' + dl_url
        elif not dl_url.startswith('http'):
            dl_url = 'https://boardgamegeek.com/' + dl_url

        print(f"  📦 下载: {dl_url}")

        r2 = session.get(dl_url, headers=headers, timeout=120)
        if r2.status_code != 200:
            print(f"  ❌ 下载失败: HTTP {r2.status_code}")
            return False

        content = r2.content

        # 判断是 zip 还是直接 CSV
        if content[:2] == b'PK':
            # ZIP 文件
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    csv_names = [n for n in zf.namelist() if n.endswith('.csv')]
                    if not csv_names:
                        print(f"  ❌ zip 中未找到 CSV 文件")
                        print(f"     zip 内容: {zf.namelist()}")
                        return False
                    csv_content = zf.read(csv_names[0]).decode('utf-8')
                    print(f"  📄 解压: {csv_names[0]} ({len(csv_content)//1024} KB)")
            except zipfile.BadZipFile:
                print(f"  ❌ 下载的文件不是有效 zip")
                return False
        else:
            # 直接是 CSV 内容
            csv_content = content.decode('utf-8')
            print(f"  📄 直接下载 CSV ({len(csv_content)//1024} KB)")

        # 基本验证：检查是否包含期望的列头
        first_line = csv_content.split('\n')[0].lower()
        if 'id' not in first_line or 'rank' not in first_line:
            print(f"  ❌ CSV 格式不对，列头: {csv_content[:200]}")
            return False

        # 保存到本地
        with open(CSV_FILE, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        print(f"  ✅ 已保存: {CSV_FILE}")
        return True

    except requests.Timeout:
        print(f"  ❌ 请求超时")
        return False
    except Exception as e:
        print(f"  ❌ 下载异常: {e}")
        return False

# ============================================================
# 解析 CSV
# ============================================================
def parse_csv(filepath):
    """解析 BGG ranks CSV，返回 [(rank, id, name), ...] 排序列表"""
    games = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        # 验证必要列
        fields = set(reader.fieldnames or [])
        required = {'id', 'rank'}
        if not required.issubset(fields):
            print(f"  ❌ CSV 缺少必要列，当前列: {reader.fieldnames}")
            return []

        for row in reader:
            try:
                game_id  = row['id'].strip()
                rank_str = row['rank'].strip()
                name     = row.get('name', '').strip()
                is_exp   = row.get('is_expansion', '0').strip()

                if not rank_str.isdigit():
                    continue
                rank = int(rank_str)
                if rank <= 0 or not game_id:
                    continue
                # 跳过扩展包
                if is_exp == '1':
                    continue

                games.append((rank, game_id, name))
            except Exception:
                continue

    games.sort(key=lambda x: x[0])
    return games

# ============================================================
# 主函数
# ============================================================
def get_top_ids():
    local_only = '--local' in sys.argv
    print(f"📋 桌游星图 — BGG Top {TOP_N} 提取")
    print(f"   模式: {'仅本地' if local_only else '自动下载'}")

    # ── 下载阶段 ──
    if not local_only:
        cfg = load_config()
        bgg_cfg = cfg.get('bgg', {})
        username = bgg_cfg.get('username', '')
        password = bgg_cfg.get('password', '')

        if not username or not password:
            print("\n⚠️ config.yaml 中未配置 bgg.username / bgg.password")
            print("   无法自动下载，尝试使用本地 CSV...")
        else:
            session = requests.Session()

            if bgg_login(session, username, password):
                if download_csv(session):
                    print("  🎉 CSV 下载更新完成")
                else:
                    print("  ⚠️ 下载失败，尝试使用本地 CSV...")
            else:
                print("  ⚠️ 登录失败，尝试使用本地 CSV...")

    # ── 解析阶段 ──
    print(f"\n📋 从本地 CSV 提取 Top {TOP_N}...")

    if not os.path.exists(CSV_FILE):
        print(f"  ❌ 未找到 {CSV_FILE}")
        print(f"     请从 https://boardgamegeek.com/data_dumps/bg_ranks 手动下载")
        print(f"     解压后放到: {CSV_FILE}")
        return False

    # 文件信息
    mtime = os.path.getmtime(CSV_FILE)
    age_days = (time.time() - mtime) / 86400
    size_mb = os.path.getsize(CSV_FILE) / (1024 * 1024)
    print(f"  📄 文件: boardgames_ranks.csv ({size_mb:.1f} MB)")
    print(f"  📅 更新: {time.strftime('%Y-%m-%d %H:%M', time.localtime(mtime))}"
          f"{'  ⚠️ ' + str(int(age_days)) + ' 天前' if age_days > 14 else ''}")

    # 解析
    games = parse_csv(CSV_FILE)
    if not games:
        print("  ❌ CSV 解析失败或无有效数据")
        return False

    top_games = games[:TOP_N]

    # 写入 top_1000_ids.txt
    with open(OUTPUT, 'w', encoding='utf-8') as f:
        for rank, gid, name in top_games:
            f.write(f"{gid}\n")

    print(f"\n📋 完成！Top {len(top_games)} 个 ID → top_1000_ids.txt")
    print(f"   CSV 总排名游戏数: {len(games)}")
    if top_games:
        print(f"   #1:    {top_games[0][2]} (ID={top_games[0][1]})")
        print(f"   #{len(top_games)}: {top_games[-1][2]} (ID={top_games[-1][1]})")

    return True


if __name__ == '__main__':
    if not get_top_ids():
        print("\n⚠️ 提取失败，保留现有 top_1000_ids.txt")
        sys.exit(1)
