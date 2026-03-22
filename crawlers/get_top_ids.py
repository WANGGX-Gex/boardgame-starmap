import cloudscraper
from bs4 import BeautifulSoup
import time
import os

def get_top_1000():
    all_ids = []
    print("🕵️‍♂️ 装备 Cloudflare 隐身盾，开始强行潜入 BGG...")
    
    # 【核心升级】创建一个能骗过防火墙的超级访问器
    scraper = cloudscraper.create_scraper(browser={
        'browser': 'chrome',
        'platform': 'windows',
        'desktop': True
    })
    
    file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "top_1000_ids.txt")
    with open(file_path, "w", encoding="utf-8") as f:
        
        for page in range(1, 11):
            url = f"https://boardgamegeek.com/browse/boardgame/page/{page}"
            print(f"\n📄 正在扫描第 {page} 页 ({url})...")
            
            # 【核心升级】用 scraper 替代原来的 requests
            response = scraper.get(url)
            
            if response.status_code != 200:
                print(f"❌ 突防失败，状态码: {response.status_code}。请考虑增加延时。")
                time.sleep(10)
                continue
                
            soup = BeautifulSoup(response.text, 'html.parser')
            games = soup.select('.collection_objectname a.primary')
            
            page_count = 0
            for game in games:
                href = game.get('href')
                if href:
                    game_id = href.split('/')[2]
                    game_name = game.text.strip()
                    all_ids.append(game_id)
                    f.write(f"{game_id},{game_name}\n")
                    page_count += 1
            
            print(f"✅ 第 {page} 页抓取完成，本页找到 {page_count} 个游戏。")
            print("💤 保持低调，原地潜伏 5 秒...")
            time.sleep(5) # 稍微延长一点休息时间，更加稳妥
            
    print(f"\n🎉 突防成功！共抓取 {len(all_ids)} 个 ID。")
    print(f"📂 名单已安全保存在 {os.path.abspath(file_path)}。")

if __name__ == "__main__":
    get_top_1000()
