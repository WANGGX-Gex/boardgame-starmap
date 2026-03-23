"""
桌游星图 - 数据清洗与星图 JSON 生成（v3）

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
工作流范式（每次拿到新数据后重复以下步骤）：

  第一步：运行本脚本
      python3 clean_data_v3.py

  第二步：检查输出
      ✅ graph_data.json          ← 星图主数据（前端直接读取）
      📋 publisher_to_fill.csv    ← 未能自动匹配国家的出版商（需人工补充）
      📊 运行报告（控制台输出）

  第三步：人工补充国家（可选，用 LLM 辅助）
      打开 publisher_to_fill.csv，在 country_code 列填入国家代码
      填完后 另存为 publisher_country_manual.csv（保持同目录）

  第四步：重新运行
      python3 clean_data_v3.py
      脚本会自动读取 publisher_country_manual.csv 并合并

  之后每次更新数据库，只需重复 第一步→第四步
  publisher_country_manual.csv 是累积的，不会丢失之前的标注
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

国家代码参考（常用）：
  US=美国 DE=德国 FR=法国 UK=英国 JP=日本 KR=韩国
  CN=中国 TW=中国台湾 HK=中国香港 MO=中国澳门
  IT=意大利 ES=西班牙 NL=荷兰 PL=波兰 CZ=捷克
  CA=加拿大 AU=澳大利亚 BR=巴西 RU=俄罗斯 SE=瑞典
  DK=丹麦 NO=挪威 FI=芬兰 BE=比利时 CH=瑞士
  AT=奥地利 HU=匈牙利 GR=希腊 PT=葡萄牙 IL=以色列
  TH=泰国 SG=新加坡 IN=印度 TR=土耳其 UA=乌克兰
  RS=塞尔维亚 RO=罗马尼亚 BG_=保加利亚 LV=拉脱维亚
  IE=爱尔兰 MX=墨西哥 AR=阿根廷 CO=哥伦比亚 KZ=哈萨克斯坦

依赖：
    pip install opencc-python-reimplemented

输入：
    bgg_starmap.db                    （SQLite 数据库，必须）
    top_1000_ids.txt                  （核心桌游 ID 列表，必须）
    publisher_country_manual.csv      （人工标注，可选，累积）

输出：
    graph_data.json                   （星图主数据）
    publisher_to_fill.csv             （待人工补充的出版商）
"""

import os
import sqlite3
import json
import re
import csv
from collections import defaultdict, Counter

# ============================================================
# OpenCC（简繁转换）
# ============================================================
try:
    from opencc import OpenCC
    _t2s = OpenCC('t2s')
    _s2t = OpenCC('s2t')
    HAS_OPENCC = True
except ImportError:
    HAS_OPENCC = False

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'bgg_starmap.db')
OUTPUT_PATH = os.path.join(BASE_DIR, 'output', 'graph_data.json')
MANUAL_COUNTRY_CSV = os.path.join(BASE_DIR, 'data', 'publisher_country_manual.csv')
TO_FILL_CSV = os.path.join(BASE_DIR, 'data', 'publisher_to_fill.csv')
GAME_NAME_OVERRIDE_CSV = os.path.join(BASE_DIR, 'data', 'game_name_override.csv')

# ============================================================
# 配置
# ============================================================
MIN_RATINGS_FOR_RELATED = 50
MAX_DESC_LENGTH = 300
MIN_GAMES_FOR_FILL = 5  # publisher_to_fill.csv 只输出 >= 这个数的


# ############################################################
#
#   第一部分：中文名工具
#
# ############################################################

_JAPANESE_KANA_RE = re.compile(
    r'[\u3040-\u309F\u30A0-\u30FF\u31F0-\u31FF\uFF65-\uFF9F]'
)
_JAPANESE_PARTICLES = re.compile(
    r'の|を|は|が|に|で|と|も|へ|から|まで|より|だ|です|ます|した|する|られ|ない|ある|いる|おり|ください'
)

_SIMPLIFIED_ONLY = set("国关动东书会产亲亿从众优传伤体华单历发变号问团场处头学实对导层帮广应张归录总战报择拥挥损换携摇数时显来条极构标样检权欢浅测济温游满热爱牵独现环码确种积类经绝统联艺节获营虑装观规认论证识设语说请调质资赵转过运进远选邮释量开问际随难电韩项须领风馆马验鱼鸡黄齿龙")
_TRADITIONAL_ONLY = set("國關動東書會產親億從眾優傳傷體華單歷發變號問團場處頭學實對導層幫廣應張歸錄總戰報擇擁揮損換攜搖數時顯來條極構標樣檢權歡淺測濟溫遊滿熱愛牽獨現環碼確種積類經絕統聯藝節獲營慮裝觀規認論證識設語說請調質資趙轉過運進遠選郵釋量開問際隨難電韓項須領風館馬驗魚雞黃齒龍")


def contains_japanese(text):
    if not text: return False
    return bool(_JAPANESE_KANA_RE.search(text)) or bool(_JAPANESE_PARTICLES.search(text))

def contains_cjk(text):
    return any('\u4e00' <= ch <= '\u9fff' for ch in (text or ''))

def contains_korean(text):
    return any('\uac00' <= ch <= '\ud7af' or '\u1100' <= ch <= '\u11ff' for ch in (text or ''))

def classify_chinese(text):
    if not text: return 'ambiguous'
    cjk = ''.join(ch for ch in text if '\u4e00' <= ch <= '\u9fff')
    if not cjk: return 'ambiguous'
    if HAS_OPENCC:
        simp = _t2s.convert(cjk)
        trad = _s2t.convert(cjk)
        if simp == cjk and trad != cjk: return 'simplified'
        elif simp != cjk: return 'traditional'
        return 'ambiguous'
    else:
        s = sum(1 for ch in cjk if ch in _SIMPLIFIED_ONLY)
        t = sum(1 for ch in cjk if ch in _TRADITIONAL_ONLY)
        if s > t: return 'simplified'
        elif t > s: return 'traditional'
        return 'ambiguous'

def to_simplified(text):
    if not text: return ''
    return _t2s.convert(text) if HAS_OPENCC else text

def clean_label(text):
    if not text: return ""
    return re.sub(r'\s+', ' ', str(text).strip())

def strip_suffix_notes(text):
    if not text: return ""
    text = clean_label(text)
    original = text
    text = re.sub(r'\s*[\(\[【（《].*?[\)\]】）》]\s*', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip(' ：:;；-—')
    return text if text else original

def pick_best_chinese_names(alternatenames):
    simp, trad, ambi = [], [], []
    seen = set()
    for entry in (alternatenames or []):
        raw = clean_label(entry.get('name', '') if isinstance(entry, dict) else str(entry))
        if not raw or contains_japanese(raw) or contains_korean(raw) or not contains_cjk(raw):
            continue
        cleaned = strip_suffix_notes(raw)
        if not cleaned or not contains_cjk(cleaned): cleaned = raw
        if cleaned in seen: continue
        seen.add(cleaned)
        s = classify_chinese(cleaned)
        (simp if s == 'simplified' else trad if s == 'traditional' else ambi).append(cleaned)

    key = lambda n: (abs(len(n) - 5), len(n), n)
    simp.sort(key=key); trad.sort(key=key); ambi.sort(key=key)

    cn = simp[0] if simp else None
    hant = trad[0] if trad else None
    display = cn or (to_simplified(hant) if hant else (ambi[0] if ambi else None))

    return {
        'name_zh_cn': cn, 'name_zh_hant': hant,
        'name_zh_display': display, 'name_zh_candidates': simp + trad + ambi,
    }

def extract_chinese_name(alternatenames):
    return pick_best_chinese_names(alternatenames).get('name_zh_display')


# ############################################################
#
#   第二部分：出版商国家/地区
#
# ############################################################

COUNTRY_DB = {
    'US': ('United States','🇺🇸','美国'), 'DE': ('Germany','🇩🇪','德国'),
    'UK': ('United Kingdom','🇬🇧','英国'), 'FR': ('France','🇫🇷','法国'),
    'CN': ('China','🇨🇳','中国'), 'JP': ('Japan','🇯🇵','日本'),
    'KR': ('South Korea','🇰🇷','韩国'), 'IT': ('Italy','🇮🇹','意大利'),
    'ES': ('Spain','🇪🇸','西班牙'), 'NL': ('Netherlands','🇳🇱','荷兰'),
    'PL': ('Poland','🇵🇱','波兰'), 'CZ': ('Czech Republic','🇨🇿','捷克'),
    'CA': ('Canada','🇨🇦','加拿大'), 'AU': ('Australia','🇦🇺','澳大利亚'),
    'BR': ('Brazil','🇧🇷','巴西'), 'RU': ('Russia','🇷🇺','俄罗斯'),
    'SE': ('Sweden','🇸🇪','瑞典'), 'DK': ('Denmark','🇩🇰','丹麦'),
    'NO': ('Norway','🇳🇴','挪威'), 'FI': ('Finland','🇫🇮','芬兰'),
    'BE': ('Belgium','🇧🇪','比利时'), 'CH': ('Switzerland','🇨🇭','瑞士'),
    'AT': ('Austria','🇦🇹','奥地利'), 'PT': ('Portugal','🇵🇹','葡萄牙'),
    'GR': ('Greece','🇬🇷','希腊'), 'IN': ('India','🇮🇳','印度'),
    'TH': ('Thailand','🇹🇭','泰国'), 'SG': ('Singapore','🇸🇬','新加坡'),
    'MY': ('Malaysia','🇲🇾','马来西亚'), 'IL': ('Israel','🇮🇱','以色列'),
    'TR': ('Turkey','🇹🇷','土耳其'), 'MX': ('Mexico','🇲🇽','墨西哥'),
    'AR': ('Argentina','🇦🇷','阿根廷'), 'CL': ('Chile','🇨🇱','智利'),
    'CO': ('Colombia','🇨🇴','哥伦比亚'), 'NZ': ('New Zealand','🇳🇿','新西兰'),
    'HU': ('Hungary','🇭🇺','匈牙利'), 'RO': ('Romania','🇷🇴','罗马尼亚'),
    'BG_': ('Bulgaria','🇧🇬','保加利亚'), 'HR': ('Croatia','🇭🇷','克罗地亚'),
    'UA': ('Ukraine','🇺🇦','乌克兰'), 'PH': ('Philippines','🇵🇭','菲律宾'),
    'ID': ('Indonesia','🇮🇩','印度尼西亚'), 'ZA': ('South Africa','🇿🇦','南非'),
    'EE': ('Estonia','🇪🇪','爱沙尼亚'), 'LV': ('Latvia','🇱🇻','拉脱维亚'),
    'LT': ('Lithuania','🇱🇹','立陶宛'), 'SK': ('Slovakia','🇸🇰','斯洛伐克'),
    'SI': ('Slovenia','🇸🇮','斯洛文尼亚'), 'RS': ('Serbia','🇷🇸','塞尔维亚'),
    'IE': ('Ireland','🇮🇪','爱尔兰'), 'PE': ('Peru','🇵🇪','秘鲁'),
    'VN': ('Vietnam','🇻🇳','越南'), 'KZ': ('Kazakhstan','🇰🇿','哈萨克斯坦'),
    'HK': ('Hong Kong','🇭🇰','中国香港'), 'MO': ('Macau','🇲🇴','中国澳门'),
    'TW': ('China Taiwan','','中国台湾'),  # 不显示 Emoji
}

# ---- 出版商名称 → 国家代码（不区分大小写匹配）----
_PUBLISHER_NAMES = {
    # US
    'Fantasy Flight Games':'US','Z-Man Games':'US','Rio Grande Games':'US',
    'Stonemaier Games':'US','Greater Than Games':'US','Greater Than Games, LLC':'US',
    'Renegade Game Studios':'US','Restoration Games':'US','Leder Games':'US',
    'Plaid Hat Games':'US','Dire Wolf':'US','Dire Wolf Digital':'US',
    'Pandasaurus Games':'US','Floodgate Games':'US','Chip Theory Games':'US',
    'Thunderworks Games':'US','Bezier Games':'US','Bézier Games':'US',
    'Indie Boards & Cards':'US','Indie Boards and Cards':'US',
    'Cryptozoic Entertainment':'US','Hasbro':'US','Mattel':'US',
    'Steve Jackson Games':'US','Atlas Games':'US','Alderac Entertainment Group':'US',
    'AEG':'US','Upper Deck Entertainment':'US','WizKids':'US','WizKids (I)':'US',
    'Mayfair Games':'US','Looney Labs':'US','North Star Games':'US',
    'Wizards of the Coast':'US','Avalon Hill':'US','The Avalon Hill Game Co':'US',
    'GMT Games':'US','Decision Games (I)':'US','Columbia Games':'US',
    'Victory Point Games':'US','Brotherwise Games':'US','Tasty Minstrel Games':'US',
    'Daily Magic Games':'US','Keymaster Games':'US','Bitewing Games':'US',
    'Button Shy':'US','Smirk & Dagger Games':'US','Grey Fox Games':'US',
    'Arcane Wonders':'US','CMON Global Limited':'US','CMON Limited':'US',
    'Cool Mini Or Not':'US','CMON':'US','Spin Master Ltd.':'US','Spin Master':'US',
    'Capstone Games':'US','Red Raven Games':'US','Compass Games':'US',
    'Multi-Man Publishing':'US','Academy Games, Inc.':'US','Eagle-Gryphon Games':'US',
    'Funko Games':'US','Skybound Games':'US','Flatout Games':'US',
    'Lucky Duck Games':'US','Mondo Games':'US','Plan B Games':'US',
    'Next Move Games':'US','Asmodee North America':'US','Stronghold Games':'US',
    'Calliope Games':'US','Pencil First Games, LLC':'US','Pencil First Games':'US',
    'Inside Up Games':'US','Gamewright':'US','USAopoly':'US','The Op':'US',
    'The Op Games':'US','Ravensburger North America, Inc.':'US',
    'Van Ryder Games':'US','Wise Wizard Games':'US','Flying Frog Productions':'US',
    'Stone Blade Entertainment':'US','Petersen Games':'US','Level 99 Games':'US',
    'Tycoon Games':'US','Gale Force Nine, LLC':'US','Gale Force Nine':'US',
    'Spaghetti Western Games':'US','CrowD Games':'US','Milton Bradley':'US',
    'Parker Brothers':'US','Fabled Nexus':'US','Fireside Games':'US',
    'Catan Studio':'US','R&R Games':'US','Game Salute':'US','CMYK':'US',
    'Starling Games (II)':'US','Paizo Publishing':'US',
    # DE
    'Kosmos':'DE','Hans im Glück':'DE','Ravensburger':'DE',
    'Ravensburger Spieleverlag GmbH':'DE','alea':'DE','Queen Games':'DE',
    'Pegasus Spiele':'DE','Amigo':'DE','AMIGO':'DE',
    'Amigo Spiel + Freizeit GmbH':'DE','HABA':'DE','HABA - Habermaaß GmbH':'DE',
    'Lookout Games':'DE','Feuerland Spiele':'DE','Schmidt Spiele':'DE',
    'Eggertspiele':'DE','eggertspiele':'DE','dlp games':'DE','Spielworxx':'DE',
    'Frosted Games':'DE','Edition Spielwiese':'DE',
    'Nürnberger-Spielkarten-Verlag':'DE','NSV':'DE','Deep Print Games':'DE',
    'Möller Design':'DE','Skellig Games':'DE','Heidelberger Spieleverlag':'DE',
    'Zoch Verlag':'DE','2F-Spiele':'DE','Schwerkraft-Verlag':'DE',
    'Board Game Circus':'DE','Strohmann Games':'DE','ABACUSSPIELE':'DE',
    'Giant Roc':'DE','Corax Games':'DE','Grimspire':'DE','HeidelBÄR Games':'DE',
    'ASS Altenburger Spielkarten':'DE','W. Nostheide Verlag GmbH':'DE',
    'HUCH!':'DE','Franckh-Kosmos Verlags-GmbH & Co. KG':'DE',
    'Spiel direkt eG':'DE','PD-Verlag':'DE',
    # FR
    'Asmodee':'FR','asmodee':'FR','Repos Production':'FR','Days of Wonder':'FR',
    'Libellud':'FR','Matagot':'FR','Iello':'FR','IELLO':'FR',
    'Ludonaute':'FR','Bombyx':'FR','Ankama':'FR','Pearl Games':'FR',
    'Lumberjacks Studio':'FR','Catch Up Games':'FR','Blue Cocker Games':'FR',
    'Holy Grail Games':'FR','Gigamic':'FR','Blue Orange (EU)':'FR',
    'Blue Orange Games':'FR','Sorry We Are French':'FR','Super Meeple':'FR',
    'Space Cowboys':'FR','Hachette Boardgames':'FR','Funnyfox':'FR',
    'Studio H':'FR','Ystari Games':'FR','Funforge':'FR','Pixie Games':'FR',
    'Origames':'FR','Descartes Editeur':'FR',"Don't Panic Games":'FR',
    'Monolith Board Games':'FR','Guillotine Games':'FR','Ludically':'FR',
    'Synapses Games':'FR','La Boîte de Jeu':'FR','Blackrock Games':'FR',
    'Ferti':'FR',
    # UK
    'Games Workshop':'UK','Games Workshop Ltd.':'UK','Osprey Games':'UK',
    'Alley Cat Games':'UK','Garphill Games':'UK','Hub Games':'UK',
    'Steamforged Games Ltd.':'UK','Steamforged Games':'UK',
    'Modiphius Entertainment':'UK','Chaosium':'UK','Sophisticated Games':'UK',
    # IT
    'Cranio Creations':'IT','Giochi Uniti':'IT','dV Giochi':'IT','DV Games':'IT',
    'Horrible Guild':'IT','Ares Games':'IT','Pendragon Game Studio':'IT',
    'Ghenos Games':'IT','Asterion Press':'IT','Stratelibri':'IT','MS Edizioni':'IT',
    'uplay.it edizioni':'IT','Mancalamaro':'IT','Stupor Mundi':'IT',
    'Giochix.it':'IT','Nexus Editrice':'IT','Raven Distribution':'IT',
    # ES
    'Devir':'ES','Ludonova':'ES','Tranjis Games':'ES','Maldito Games':'ES',
    'Arrakis Games':'ES','Salt & Pepper Games':'ES','TCG Factory':'ES',
    'Gen X Games':'ES','Gen-X Games':'ES','Edge Entertainment':'ES',
    'MasQueOca Games':'ES','Ediciones MasQueOca':'ES','HomoLudicus':'ES',
    '2Tomatoes Games':'ES','SD Games':'ES','Last Level':'ES',
    # NL
    '999 Games':'NL','Splotter Spellen':'NL','White Goblin Games':'NL',
    'Jumbo':'NL','The Game Master BV':'NL','PHALANX':'NL',
    # BE
    'Geek Attitude Games':'BE','Intrafin Games':'BE',
    # PL
    'Portal Games':'PL','Rebel Sp. z o.o.':'PL','Rebel':'PL','Board&Dice':'PL',
    'Galakta':'PL','Games Factory Publishing':'PL','Granna':'PL','Lucrum Games':'PL',
    'Lacerta':'PL','Bard Centrum Gier':'PL','G3':'PL','IUVI Games':'PL',
    'Ogry Games':'PL','FoxGames':'PL','Egmont Polska':'PL','Czacha Games':'PL',
    'Awaken Realms':'PL',
    # CZ
    'Czech Games Edition':'CZ','Delicious Games':'CZ','MINDOK':'CZ',
    'ADC Blackfire Entertainment':'CZ','REXhry':'CZ','Albi':'CZ',
    'TLAMA games':'CZ','Fox in the Box':'CZ',
    # CA
    'Roxley':'CA','Kolossal Games':'CA','FoxMind':'CA',
    'Le Scorpion Masqué':'CA','Filosofia Éditions':'CA',
    # HU
    'Mindclash Games':'HU','Gém Klub Kft.':'HU','Delta Vision Publishing':'HU',
    'Reflexshop':'HU','Compaya.hu: Gamer Café Kft.':'HU',
    # SE
    'Fryxgames':'SE','Free League Publishing':'SE','Enigma (Bergsala Enigma)':'SE',
    'Alga':'SE',
    # DK
    'Spilbræt.dk':'DK',
    # FI
    'Lautapelit.fi':'FI',
    # CH
    'Helvetiq':'CH',
    # AT
    'Piatnik':'AT','Piatnik Distribution':'AT',
    # JP
    'Arclight':'JP','Arclight Games':'JP','JAPON BRAND':'JP','Oink Games':'JP',
    'Hobby Japan':'JP','Hobby Japan Co., Ltd.':'JP','Group SNE':'JP',
    'One Draw':'JP','itten':'JP','Saashi & Saashi':'JP','BakaFire Party':'JP',
    'JELLY JELLY GAMES':'JP','テンデイズゲームズ (TendaysGames)':'JP',
    'Engames':'JP','New Games Order, LLC':'JP','Colon Arc':'JP',
    'asobition':'JP','Grounding':'JP','HJ Holdings Inc':'JP','Sugorokuya':'JP',
    # KR
    'Mandoo Games':'KR','Board M Factory':'KR','BoardM Factory':'KR',
    'Korea Boardgames Co., Ltd.':'KR','Korea Boardgames':'KR',
    'Happy Baobab':'KR','DIVE DICE Inc.':'KR','DiveDice':'KR',
    'Boardpick':'KR','Piece Craft':'KR','DiceTree Games':'KR','MTS Games':'KR',
    'Popcorn Games':'KR','Angry Lion Games':'KR','HIT Games':'KR',
    'sternenschimmermeer':'KR',
    # CN
    'YOKA Games':'CN',"Surfin' Meeple China":'CN','CMON Asia Limited':'CN',
    'Game Harbor':'CN','Banana Games':'CN','MYBG Co., Ltd.':'CN',
    'One Moment Games':'CN','Rawstone':'CN','Board Game Rookie':'CN',
    '游人码头':'CN','大世界桌游':'CN','Gameland 游戏大陆':'CN',
    # TW
    'Swan Panasia Co., Ltd.':'TW','新天鹅堡':'TW','Moaideas Game Design':'TW',
    'EmperorS4 Games':'TW','Big Fun Games':'TW','Mizo Games':'TW',
    'Homosapiens Lab':'TW','Good Game Studio':'TW','GoKids 玩樂小子':'TW',
    'Giga Mech Games':'TW','骰子人桌遊':'TW','桌遊愛樂事':'TW',
    # HK
    'Jolly Thinkers':'HK','Broadway Toys LTD':'HK','Wargames Club Publishing':'HK',
    'Capstone HK Ltd.':'HK',
    # BR
    'Galápagos Jogos':'BR','PaperGames (III)':'BR','PaperGames':'BR',
    'Conclave Editora':'BR','Meeple BR Jogos':'BR','MeepleBR':'BR',
    'Grok Games':'BR','Ludofy Creative':'BR','Mosaico Jogos':'BR',
    'Grow Jogos e Brinquedos':'BR','L. P. Septímio':'BR',
    # RU
    'Hobby World':'RU','Crowd Games':'RU','Cosmodrome Games':'RU',
    'Lavka Games':'RU','GaGa Games':'RU','Lifestyle Boardgames Ltd':'RU',
    'Zvezda':'RU','Smart Ltd':'RU','Evrikus':'RU','Choo Choo Games':'RU',
    # UA
    'Geekach Games':'UA','Geekach LLC':'UA','Lord of Boards':'UA',
    'Feelindigo':'UA','Ігромаг':'UA','Games7Days':'UA','IGames':'UA',
    # RS
    'CoolPlay':'RS','MIPL':'RS',
    # GR
    'Kaissa Chess & Games':'GR','Cube Factory of Ideas':'GR',
    # TH
    'Siam Board Games':'TH','Lanlalen':'TH','Tower Tactic Games':'TH',
    # LV
    'Brain Games':'LV',
    # TR
    'NeoTroy Games':'TR','Bosphorus Board Games':'TR',
    # BG_
    'Fantasmagoria':'BG_',
    # KZ
    'Tabletop KZ':'KZ',
    # PT
    'MESAboardgames':'PT','PYTHAGORAS':'PT','MEBO Games':'PT',
    # 特殊占位符
    '(Web published)':None,'(Self-Published)':None,
    '(Public Domain)':None,'(Unknown)':None,
}
# 转 lower key
PUBLISHER_NAME_MAP = {k.lower(): v for k, v in _PUBLISHER_NAMES.items()}

# ---- 描述关键词 → 国家代码 ----
_PLACE_KEYWORDS = {
    'US': ['united states','usa','american','california','texas','new york','minnesota','seattle','chicago','boston','los angeles','atlanta','portland','austin','georgia','wisconsin','indiana','ohio','virginia','pennsylvania','connecticut','massachusetts','maryland','oregon','florida','colorado','michigan','north carolina','tennessee','missouri','arizona','new jersey','san francisco','denver','philadelphia','nashville','tallahassee','hanford','springfield','renton','pawtucket','plymouth, minnesota'],
    'DE': ['germany','german ','deutschland','berlin','hamburg','münchen','munich','nürnberg','nuremberg','stuttgart','essen','düsseldorf','frankfurt','cologne','köln','freiburg','leipzig','dresden','heidelberg','hannover','dreieich','bremen','bonn'],
    'FR': ['france','french ','paris','lyon','toulouse','française','marseille','bordeaux','strasbourg','nantes','nancy'],
    'UK': ['united kingdom','england','british','london','nottingham','bristol','manchester','scotland','edinburgh','wales','leeds','liverpool','glasgow','brighton','oxford','cambridge'],
    'JP': ['japan','japanese','tokyo','osaka','yokohama','kyoto'],
    'KR': ['korea','korean','seoul','busan','south korea'],
    'CN': ['china','chinese','beijing','shanghai','shenzhen','guangzhou','ningbo','chengdu','wuhan','hangzhou'],
    'TW': ['taiwan','taipei'],
    'HK': ['hong kong'],
    'IT': ['italy','italian','milano','milan','roma','rome','torino','modena','cascina','firenze','bologna'],
    'ES': ['spain','spanish','españa','madrid','barcelona','valencia','sevilla','española'],
    'NL': ['netherlands','dutch','holland','amsterdam','rotterdam','utrecht'],
    'PL': ['poland','polish','polska','warsaw','warszawa','kraków','krakow','wrocław','poznań'],
    'CZ': ['czech','prague','praha','brno'],
    'CA': ['canada','canadian','calgary','toronto','vancouver','montreal','montréal','ottawa','québec'],
    'AU': ['australia','australian','melbourne','sydney','brisbane'],
    'NZ': ['new zealand','auckland','wellington'],
    'BR': ['brazil','brazilian','brasil','são paulo','rio de janeiro'],
    'RU': ['russia','russian','moscow'],
    'SE': ['sweden','swedish','stockholm','gothenburg','malmö','nordic'],
    'DK': ['denmark','danish','copenhagen'],
    'NO': ['norway','norwegian','oslo'],
    'FI': ['finland','finnish','helsinki'],
    'BE': ['belgium','belgian','brussels','bruxelles','antwerp','gent'],
    'CH': ['switzerland','swiss','zürich','zurich','bern','basel','geneva'],
    'AT': ['austria','austrian','wien','vienna','viennese','salzburg'],
    'HU': ['hungary','hungarian','budapest'],
    'IL': ['israel','israeli','tel aviv'],
    'TH': ['thailand','thai','bangkok'],
    'SG': ['singapore'],
    'IN': ['india','indian','mumbai','delhi','bangalore'],
    'GR': ['greece','greek','athens'],
    'PT': ['portugal','portuguese','lisbon'],
    'UA': ['ukraine','ukrainian','kyiv','kiev'],
    'RO': ['romania','romanian','bucharest'],
    'RS': ['serbia','serbian','belgrade'],
    'HR': ['croatia','croatian','zagreb'],
    'BG_': ['bulgaria','bulgarian','sofia'],
    'SK': ['slovakia','slovak','bratislava'],
    'TR': ['turkey','turkish','istanbul','ankara'],
    'MX': ['mexico','mexican','méxico'],
    'AR': ['argentina','argentine','buenos aires'],
    'CO': ['colombia','colombian','bogotá'],
    'LV': ['latvia','latvian','riga','baltic'],
    'LT': ['lithuania','lithuanian','vilnius'],
    'EE': ['estonia','estonian','tallinn'],
    'IE': ['ireland','irish','dublin'],
    'KZ': ['kazakhstan','almaty','astana'],
}
PLACE_TO_COUNTRY = {}
for code, kws in _PLACE_KEYWORDS.items():
    for kw in kws:
        PLACE_TO_COUNTRY[kw] = code


def format_country(code):
    if not code or code not in COUNTRY_DB:
        return None
    name_en, emoji, name_zh = COUNTRY_DB[code]
    if code == 'TW':
        display = 'China Taiwan'
    elif emoji:
        display = f'{emoji} {name_en}'
    else:
        display = name_en
    return {'code': code, 'name_en': name_en, 'emoji': emoji, 'display': display, 'name_zh': name_zh}


def match_publisher_country(name, description=''):
    # 1. 名称精确匹配（不区分大小写）
    code = PUBLISHER_NAME_MAP.get((name or '').lower())
    if code is not None:
        return code  # 可能返回 None（占位符）

    # 2. 描述关键词匹配
    if description:
        text = re.sub(r'<[^>]+>', ' ', description).lower()
        text = re.sub(r'&[a-z]+;', ' ', text)
        scores = {}
        for place, c in PLACE_TO_COUNTRY.items():
            if place in text:
                scores[c] = scores.get(c, 0) + 1
        if scores:
            return max(scores, key=scores.get)

    return None  # 真的匹配不到


def load_manual_countries():
    """加载人工补充的 CSV（支持两种文件名，累积合并）"""
    manual = {}
    for path in [MANUAL_COUNTRY_CSV]:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    pid = (row.get('publisher_id') or '').strip()
                    code = (row.get('country_code') or '').strip()
                    if pid and code:
                        manual[pid] = code
    return manual


def load_game_name_overrides():
    """加载中文译名手动修正表"""
    overrides = {}
    if os.path.exists(GAME_NAME_OVERRIDE_CSV):
        with open(GAME_NAME_OVERRIDE_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                bgg_id = (row.get('bgg_id') or '').strip()
                name_zh = (row.get('name_zh_override') or '').strip()
                if bgg_id and name_zh:
                    overrides[bgg_id] = name_zh
    return overrides


# ############################################################
#
#   第三部分：通用工具
#
# ############################################################

def shorten_text(text, limit=MAX_DESC_LENGTH):
    text = clean_label(text or '')
    if not text: return ''
    return text if len(text) <= limit else text[:limit].rstrip() + '…'

def parse_int(v, d=0):
    try: return int(v)
    except: return d

def parse_float(v, d=0.0):
    try: return float(v)
    except: return d

def is_primary_link(v):
    return str(v).strip() in {'1','true','True'} or v == 1 or v is True

def is_promo(name):
    if not name: return False
    lower = name.lower()
    return any(kw in lower for kw in ['promo','mini expansion','bonus card','insert','sleeve','coin set','metal coins','upgrade','playmat','scorepad'])

def get_or_create_relation_bucket(bucket, gid):
    if gid not in bucket:
        bucket[gid] = {k: set() for k in ['expansions','base_games','reimplements','reimplemented_by','integrates','contains','contained_in','designers','artists','publishers_primary','publishers_co']}
    return bucket[gid]

def as_sorted_name_list(ids_set, nodes):
    rows = []
    for nid in ids_set:
        node = nodes.get(nid)
        if not node: continue
        rows.append({'id': nid, 'bgg_id': node.get('bgg_id'), 'class': node.get('class'),
                     'name_zh': node.get('name_zh_display') or node.get('name_zh'),
                     'name_en': node.get('name_en')})
    rows.sort(key=lambda x: (x.get('name_zh') or '', x.get('name_en') or ''))
    return rows


# ############################################################
#
#   第四部分：主流程
#
# ############################################################

def main():
    print("🚀 桌游星图 - 数据清洗 v3")
    print(f"   OpenCC: {'✅' if HAS_OPENCC else '❌ 回退模式 (pip install opencc-python-reimplemented)'}")

    # 确保输出目录存在
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    tables = {}
    for t in ['games_raw','games_dynamic','publishers_raw','persons_raw']:
        c.execute(f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{t}'")
        exists = c.fetchone()[0] > 0
        count = 0
        if exists:
            c.execute(f"SELECT COUNT(*) FROM {t}")
            count = c.fetchone()[0]
        tables[t] = count
        print(f"  {t}: {count} 条" + ("" if exists else " ⚠️ 表不存在"))

    # 1. 动态数据
    print("\n📦 加载动态数据...")
    dynamic_map = {}
    if tables['games_dynamic'] > 0:
        c.execute("SELECT game_id, json_data FROM games_dynamic")
        for game_id, json_text in c.fetchall():
            try:
                dyn = json.loads(json_text)
                item = dyn.get('item',{})
                stats = item.get('stats',{})
                ri = item.get('rankinfo',[]) or []
                polls = item.get('polls',{}) or {}
                dynamic_map[str(game_id)] = {
                    'rank': parse_int(ri[0].get('rank',0),0) if ri else 0,
                    'baverage': parse_float(ri[0].get('baverage',0),0) if ri else 0,
                    'average': parse_float(stats.get('average',0),0),
                    'usersrated': parse_int(stats.get('usersrated',0),0),
                    'avgweight': parse_float(stats.get('avgweight',0),0),
                    'numowned': parse_int(stats.get('numowned',0),0),
                    'best_players': (polls.get('userplayers',{}) or {}).get('best',[]),
                }
            except: continue
    print(f"  排名数据: {len(dynamic_map)} 条")

    # 核心桌游判定：优先从数据库 rank 动态计算 Top 1000
    # top_1000_ids.txt 仅作为初始引导 / fallback
    core_ids = set()

    # 方式 1：从 dynamic_map 中取 rank 1~1000 的游戏（实时数据）
    ranked_games = [(gid, d['rank']) for gid, d in dynamic_map.items()
                    if d.get('rank', 0) > 0]
    ranked_games.sort(key=lambda x: x[1])
    dynamic_core = set(gid for gid, rank in ranked_games[:1000])

    if len(dynamic_core) >= 500:
        # 数据库中有足够的排名数据，直接使用
        core_ids = dynamic_core
        print(f"  核心桌游: {len(core_ids)} 个（从数据库 rank 动态计算）")
    else:
        # 排名数据不足（新数据库或数据缺失），回退到 txt 文件
        ids_file = os.path.join(BASE_DIR, 'data', 'top_1000_ids.txt')
        if os.path.exists(ids_file):
            with open(ids_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip(): core_ids.add(line.split(',')[0].strip())
            print(f"  核心桌游: {len(core_ids)} 个（从 top_1000_ids.txt 回退）")
        else:
            print("  ⚠️ 无法确定核心桌游：数据库 rank 不足且无 top_1000_ids.txt")

    # 同时输出一份最新的 top_1000_ids.txt（方便其他脚本使用）
    if ranked_games:
        ids_file_out = os.path.join(BASE_DIR, 'data', 'top_1000_ids.txt')
        with open(ids_file_out, 'w', encoding='utf-8') as f:
            for gid, rank in ranked_games[:1000]:
                f.write(f"{gid}\n")
        print(f"  📋 已更新 top_1000_ids.txt（{min(len(ranked_games), 1000)} 个）")

    # 加载中文译名手动修正表
    name_overrides = load_game_name_overrides()
    if name_overrides:
        print(f"  📝 已加载中文译名修正: {len(name_overrides)} 条 (game_name_override.csv)")

    # 2. 过滤桌游（含距离限制）
    print("\n🧮 过滤桌游...")

    # 2a. 预建游戏间邻接表，用于 BFS 距离计算
    #     只看桌游之间的关联关系（expansion, reimplements, integrates, contains 等）
    #     不含人物/出版商/机制/类别
    GAME_RELATION_KEYS = [
        'boardgameexpansion', 'expandsboardgame',
        'reimplements', 'reimplementation',
        'boardgameintegration',
        'contains', 'containedin',
    ]
    MAX_DISTANCE = 2  # 只保留离核心桌游 ≤ 2 步的游戏

    print(f"  📐 计算到核心桌游的距离（最大 {MAX_DISTANCE} 步）...")
    all_games_raw = {}
    adjacency = {}  # game_id → set of neighbor game_ids

    c.execute("SELECT game_id, json_data FROM games_raw")
    for game_id, json_text in c.fetchall():
        game_id = str(game_id)
        try:
            data = json.loads(json_text)
        except:
            continue
        all_games_raw[game_id] = data
        item_links = data.get('item', {}).get('links', {}) or {}
        neighbors = set()
        for rel_key in GAME_RELATION_KEYS:
            for linked in (item_links.get(rel_key, []) or []):
                lid = str(linked.get('objectid', '')).strip()
                if lid:
                    neighbors.add(lid)
        adjacency[game_id] = neighbors

    # 确保邻接关系是双向的（A→B 意味着 B→A）
    for gid, neighbors in list(adjacency.items()):
        for nid in neighbors:
            if nid not in adjacency:
                adjacency[nid] = set()
            adjacency[nid].add(gid)

    # BFS：从核心桌游出发，计算每个游戏的最短距离
    from collections import deque
    distance = {}
    queue = deque()
    for cid in core_ids:
        if cid in all_games_raw:
            distance[cid] = 0
            queue.append(cid)

    while queue:
        gid = queue.popleft()
        cur_dist = distance[gid]
        if cur_dist >= MAX_DISTANCE:
            continue
        for neighbor in adjacency.get(gid, []):
            if neighbor not in distance and neighbor in all_games_raw:
                distance[neighbor] = cur_dist + 1
                queue.append(neighbor)

    reachable_ids = set(distance.keys())
    dist_stats = {}
    for d in distance.values():
        dist_stats[d] = dist_stats.get(d, 0) + 1
    for d in sorted(dist_stats):
        label = "核心桌游" if d == 0 else f"距离 {d}"
        print(f"    {label}: {dist_stats[d]} 个")
    unreachable = len(all_games_raw) - len(reachable_ids)
    if unreachable > 0:
        print(f"    距离 > {MAX_DISTANCE}（排除）: {unreachable} 个")

    # 2b. 过滤：核心桌游直接纳入，非核心须距离 ≤ MAX_DISTANCE 且满足评分门槛
    included_games = {}
    excluded = 0
    excluded_distance = 0
    for game_id, data in all_games_raw.items():
        item = data.get('item', {})
        dyn = dynamic_map.get(game_id, {})
        is_core = game_id in core_ids
        if is_core:
            included_games[game_id] = data
        else:
            # 距离检查
            if game_id not in reachable_ids:
                excluded_distance += 1
                excluded += 1
                continue
            # 原有过滤条件
            if is_promo(item.get('name', '')) or dyn.get('usersrated', 0) < MIN_RATINGS_FOR_RELATED:
                excluded += 1
                continue
            included_games[game_id] = data
    print(f"  纳入: {len(included_games)}, 过滤: {excluded}（其中距离过远: {excluded_distance}）")

    # 3. 构建节点与连线
    print("\n🔧 构建节点...")
    nodes, links, relations = {}, [], {}
    all_person_ids, all_publisher_ids = {}, {}
    jp_filtered = 0

    for game_id, data in included_games.items():
        item = data.get('item',{})
        item_links = item.get('links',{}) or {}
        dyn = dynamic_map.get(game_id,{})
        is_core = game_id in core_ids
        name_en = clean_label(item.get('name',''))
        zh = pick_best_chinese_names(item.get('alternatenames',[]))

        # 统计日文过滤
        for alt in (item.get('alternatenames',[]) or []):
            an = alt.get('name','') if isinstance(alt,dict) else str(alt)
            if contains_japanese(an) and contains_cjk(an): jp_filtered += 1

        nid = f'g_{game_id}'
        rb = get_or_create_relation_bucket(relations, nid)
        node = {
            'id':nid,'bgg_id':game_id,'class':'game','name_en':name_en,
            'name_zh':zh['name_zh_display'],'name_zh_cn':zh['name_zh_cn'],
            'name_zh_hant':zh['name_zh_hant'],'name_zh_display':zh['name_zh_display'],
            'name_zh_candidates':zh['name_zh_candidates'],
            'year':item.get('yearpublished',''),'subtype':item.get('subtype','boardgame'),
            'is_core':is_core,'rank':dyn.get('rank',0),'baverage':dyn.get('baverage',0),
            'average':dyn.get('average',0),'usersrated':dyn.get('usersrated',0),
            'weight':dyn.get('avgweight',0),'numowned':dyn.get('numowned',0),
            'best_players':dyn.get('best_players',[]),
            'min_players':item.get('minplayers',''),'max_players':item.get('maxplayers',''),
            'playing_time':item.get('maxplaytime',''),'image_url':item.get('imageurl',''),
            'categories':[x.get('name') for x in item_links.get('boardgamecategory',[]) if x.get('name')],
            'mechanics':[x.get('name') for x in item_links.get('boardgamemechanic',[]) if x.get('name')],
            'description':shorten_text(item.get('short_description') or item.get('description','')),
            'publishers_primary':[],'publishers_co':[],'designers':[],'artists':[],
            'related_games':{'expansions':[],'base_games':[],'reimplements':[],'reimplemented_by':[],'integrates':[],'contains':[],'contained_in':[]},
        }
        # 应用中文译名手动修正（优先级最高）
        if game_id in name_overrides:
            override_name = name_overrides[game_id]
            node['name_zh'] = override_name
            node['name_zh_display'] = override_name
        nodes[nid] = node

        # 桌游关系连线
        for exp in item_links.get('boardgameexpansion',[]) or []:
            eid = str(exp.get('objectid','')).strip()
            if eid: tid=f'g_{eid}'; links.append({'source':tid,'target':nid,'type':'expansion'}); rb['expansions'].add(tid)
        for exp in item_links.get('expandsboardgame',[]) or []:
            bid = str(exp.get('objectid','')).strip()
            if bid: tid=f'g_{bid}'; links.append({'source':nid,'target':tid,'type':'expansion'}); rb['base_games'].add(tid)
        for r in item_links.get('reimplements',[]) or []:
            rid = str(r.get('objectid','')).strip()
            if rid: tid=f'g_{rid}'; links.append({'source':nid,'target':tid,'type':'reimplements'}); rb['reimplements'].add(tid)
        for r in item_links.get('reimplementation',[]) or []:
            rid = str(r.get('objectid','')).strip()
            if rid: sid=f'g_{rid}'; links.append({'source':sid,'target':nid,'type':'reimplements'}); rb['reimplemented_by'].add(sid)
        for r in item_links.get('boardgameintegration',[]) or []:
            rid = str(r.get('objectid','')).strip()
            if rid: tid=f'g_{rid}'; links.append({'source':nid,'target':tid,'type':'integrates'}); rb['integrates'].add(tid)
        for r in item_links.get('contains',[]) or []:
            rid = str(r.get('objectid','')).strip()
            if rid: tid=f'g_{rid}'; links.append({'source':nid,'target':tid,'type':'contains'}); rb['contains'].add(tid)
        for r in item_links.get('containedin',[]) or []:
            rid = str(r.get('objectid','')).strip()
            if rid: sid=f'g_{rid}'; links.append({'source':sid,'target':nid,'type':'contains'}); rb['contained_in'].add(sid)

        # 人物
        for d in item_links.get('boardgamedesigner',[]) or []:
            pid = str(d.get('objectid','')).strip()
            if not pid: continue
            pnid = f'p_{pid}'; links.append({'source':nid,'target':pnid,'type':'designed_by'}); rb['designers'].add(pnid)
            if pid not in all_person_ids: all_person_ids[pid] = {'name':d.get('name',''),'roles':set(),'game_count':0}
            all_person_ids[pid]['roles'].add('designer'); all_person_ids[pid]['game_count'] += 1
        for a in item_links.get('boardgameartist',[]) or []:
            pid = str(a.get('objectid','')).strip()
            if not pid: continue
            pnid = f'p_{pid}'; links.append({'source':nid,'target':pnid,'type':'art_by'}); rb['artists'].add(pnid)
            if pid not in all_person_ids: all_person_ids[pid] = {'name':a.get('name',''),'roles':set(),'game_count':0}
            all_person_ids[pid]['roles'].add('artist'); all_person_ids[pid]['game_count'] += 1

        # 出版商
        for pub in item_links.get('boardgamepublisher',[]) or []:
            pubid = str(pub.get('objectid','')).strip()
            if not pubid: continue
            pubnid = f'pub_{pubid}'; primary = is_primary_link(pub.get('primarylink'))
            if pubid not in all_publisher_ids: all_publisher_ids[pubid] = {'name':pub.get('name',''),'primary_count':0,'co_count':0}
            if primary:
                all_publisher_ids[pubid]['primary_count'] += 1
                links.append({'source':nid,'target':pubnid,'type':'published_by'})
                links.append({'source':nid,'target':pubnid,'type':'published_by_primary'})
                rb['publishers_primary'].add(pubnid)
            else:
                all_publisher_ids[pubid]['co_count'] += 1
                links.append({'source':nid,'target':pubnid,'type':'published_by_co'})
                rb['publishers_co'].add(pubnid)

    print(f"  桌游: {sum(1 for n in nodes.values() if n['class']=='game')}, 人物: {len(all_person_ids)}, 出版商: {len(all_publisher_ids)}")
    print(f"  🚫 日文名过滤: {jp_filtered} 条")

    # 4. 人物节点
    print(f"\n👤 人物节点...")
    pd_detail = {}
    if tables.get('persons_raw',0) > 0:
        c.execute("SELECT person_id, json_data FROM persons_raw")
        for pid, jt in c.fetchall():
            try: pd_detail[str(pid)] = json.loads(jt)
            except: continue

    for pid, info in all_person_ids.items():
        node = {'id':f'p_{pid}','bgg_id':pid,'class':'person','name_en':clean_label(info['name']),
                'name_zh':None,'name_zh_cn':None,'name_zh_hant':None,'name_zh_display':None,'name_zh_candidates':[],
                'roles':sorted(info['roles']),'image_url':'','description':'','game_count':info['game_count']}
        detail = pd_detail.get(pid,{}).get('item',{}) if pid in pd_detail else {}
        if detail:
            zh = pick_best_chinese_names(detail.get('alternatenames',[]))
            node['name_en'] = clean_label(detail.get('name',node['name_en']))
            node.update({'name_zh':zh['name_zh_display'],'name_zh_cn':zh['name_zh_cn'],'name_zh_hant':zh['name_zh_hant'],
                        'name_zh_display':zh['name_zh_display'],'name_zh_candidates':zh['name_zh_candidates']})
            node['image_url'] = detail.get('imageurl','')
            node['description'] = shorten_text(detail.get('short_description') or detail.get('description',''))
        nodes[node['id']] = node

    # 5. 出版商节点 + 国家
    print(f"\n🏢 出版商节点...")
    pub_detail = {}
    if tables.get('publishers_raw',0) > 0:
        c.execute("SELECT publisher_id, json_data FROM publishers_raw")
        for pubid, jt in c.fetchall():
            try: pub_detail[str(pubid)] = json.loads(jt)
            except: continue

    manual_countries = load_manual_countries()
    if manual_countries:
        print(f"  📋 已加载人工标注: {len(manual_countries)} 条 ({os.path.basename(MANUAL_COUNTRY_CSV)})")

    country_stats = {'matched': 0, 'manual': 0, 'placeholder': 0, 'unknown': 0}
    unknown_pubs = []

    for pubid, info in all_publisher_ids.items():
        nid = f'pub_{pubid}'
        node = {'id':nid,'bgg_id':pubid,'class':'publisher','name_en':clean_label(info['name']),
                'name_zh':None,'name_zh_cn':None,'name_zh_hant':None,'name_zh_display':None,'name_zh_candidates':[],
                'image_url':'','description':'',
                'game_count':info['primary_count']+info['co_count'],
                'primary_game_count':info['primary_count'],'co_game_count':info['co_count'],
                'country':None}
        detail = pub_detail.get(pubid,{}).get('item',{}) if pubid in pub_detail else {}
        if detail:
            zh = pick_best_chinese_names(detail.get('alternatenames',[]))
            node['name_en'] = clean_label(detail.get('name',node['name_en']))
            node.update({'name_zh':zh['name_zh_display'],'name_zh_cn':zh['name_zh_cn'],'name_zh_hant':zh['name_zh_hant'],
                        'name_zh_display':zh['name_zh_display'],'name_zh_candidates':zh['name_zh_candidates']})
            node['image_url'] = detail.get('imageurl','')
            node['description'] = shorten_text(detail.get('short_description') or detail.get('description',''))

        # 国家匹配：人工 > 名称 > 描述
        code = manual_countries.get(nid)
        source = 'manual' if code else None

        if not code:
            code = match_publisher_country(node['name_en'], node['description'])
            if code is not None:
                source = 'auto'

        if code:
            node['country'] = format_country(code)
            country_stats['matched' if source == 'auto' else 'manual'] += 1
        elif source is None and node['name_en'].startswith('('):
            country_stats['placeholder'] += 1
        else:
            country_stats['unknown'] += 1
            unknown_pubs.append(node)

        nodes[nid] = node

    print(f"  自动匹配: {country_stats['matched']}, 人工标注: {country_stats['manual']}, "
          f"占位符: {country_stats['placeholder']}, 未匹配: {country_stats['unknown']}")

    # 输出待填写 CSV
    unknown_pubs.sort(key=lambda p: p.get('game_count',0), reverse=True)
    to_fill = [p for p in unknown_pubs if p.get('game_count',0) >= MIN_GAMES_FOR_FILL and not p['name_en'].startswith('(')]
    with open(TO_FILL_CSV, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['publisher_id','bgg_id','name_en','game_count','description','country_code'])
        for p in to_fill:
            desc = re.sub(r'<[^>]+>',' ',p.get('description','')).strip()[:200]
            writer.writerow([p['id'], p['bgg_id'], p['name_en'], p['game_count'], desc, ''])
    print(f"  📋 待填写: {TO_FILL_CSV} ({len(to_fill)} 家, >= {MIN_GAMES_FOR_FILL} games)")

    # 6. 清理连线
    print("\n🧹 清理连线...")
    valid_ids = set(nodes.keys())
    seen, clean_links = set(), []
    for link in links:
        s,t,tp = link['source'],link['target'],link['type']
        if s not in valid_ids or t not in valid_ids or s==t: continue
        key = (s,t,tp)
        if key in seen: continue
        seen.add(key); clean_links.append(link)
    print(f"  {len(links)} → {len(clean_links)} 条")

    # 7. 回填预计算字段
    for nid, bucket in relations.items():
        gn = nodes.get(nid)
        if not gn: continue
        gn['publishers_primary'] = as_sorted_name_list(bucket['publishers_primary'], nodes)
        gn['publishers_co'] = as_sorted_name_list(bucket['publishers_co'], nodes)
        gn['designers'] = as_sorted_name_list(bucket['designers'], nodes)
        gn['artists'] = as_sorted_name_list(bucket['artists'], nodes)
        gn['related_games'] = {k: as_sorted_name_list(bucket[k], nodes) for k in ['expansions','base_games','reimplements','reimplemented_by','integrates','contains','contained_in']}

    # 8. 输出
    lt_counts = Counter(l['type'] for l in clean_links)
    pub_nodes = [n for n in nodes.values() if n['class']=='publisher' and n.get('country')]
    cd = Counter(n['country']['code'] for n in pub_nodes)

    graph_data = {
        'nodes': list(nodes.values()),
        'links': clean_links,
        'meta': {
            'version':'v3','total_nodes':len(nodes),'total_links':len(clean_links),
            'game_nodes':sum(1 for n in nodes.values() if n['class']=='game'),
            'person_nodes':sum(1 for n in nodes.values() if n['class']=='person'),
            'publisher_nodes':sum(1 for n in nodes.values() if n['class']=='publisher'),
            'link_type_counts':dict(lt_counts),
            'name_policy':'v3: OpenCC简繁 + 日文排除; display永远简体; game_name_override优先',
            'core_policy':'rank动态计算Top1000; 非核心桌游须距离核心≤2步; top_1000_ids.txt作为fallback',
            'publisher_policy':'published_by_primary / published_by_co / published_by(兼容)',
            'country_policy':'国家和地区; TW→中国台湾(无emoji); HK🇭🇰; MO🇲🇴',
            'min_ratings_for_related':MIN_RATINGS_FOR_RELATED,
            'jp_names_filtered':jp_filtered,
        }
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(graph_data, f, ensure_ascii=False, indent=1)

    sz = os.path.getsize(OUTPUT_PATH) / (1024*1024)
    m = graph_data['meta']
    print(f"""
{'='*60}
🎉 完成！

📄 {OUTPUT_PATH} ({sz:.1f} MB)
   节点: {m['total_nodes']} (桌游 {m['game_nodes']} + 人物 {m['person_nodes']} + 出版商 {m['publisher_nodes']})
   连线: {m['total_links']}
   日文过滤: {jp_filtered} 条

🌍 出版商国家: {len(pub_nodes)} 已标注 / {country_stats['unknown']} 未标注
   {'  '.join(f'{c}:{n}' for c,n in cd.most_common(10))}

📋 下一步:
   {'无需操作' if not to_fill else f'填写 {TO_FILL_CSV} ({len(to_fill)}家) → 另存为 publisher_country_manual.csv → 重新运行'}
{'='*60}""")

    conn.close()


if __name__ == '__main__':
    main()
