import requests
import random
from bs4 import BeautifulSoup
import psycopg
from datetime import datetime, timezone
import time
import random


keyword = "BTC"
page = 0
max_pages = 111
all_data = []
stop = False


def crawl_utoday_page(keyword="BTC", page=0):

    url = f"https://u.today/search/node?keys={keyword}&_wrapper_format=html&page={page}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        print(f"⚠️ 第 {page} 页请求失败")
        return [] 

    soup = BeautifulSoup(res.text, "html.parser")

    search_result= soup.find("div", class_="search-result")
    if not search_result:
        print(f"页没有数据")
        return []
    
    items = search_result.find_all("div", class_="news__item")
    if not items:
        print(f'没有数据"')
        return []

    page_data = []

    for item in items:
        # 链接
        body = item.find("a", class_="news__item-body")
        link = body.get("href") if body else None

        # 标题
        title_tag = item.find("div", class_="news__item-title")
        title = title_tag.get_text(strip=True) if title_tag else None

        # 日期
        datetag = item.find("div", class_="humble")
        if datetag:
            raw_date = datetag.get_text(strip=True)
            try:
                dt = datetime.strptime(raw_date, "%b %d, %Y - %H:%M")
                dt = dt.replace(tzinfo=ZoneInfo("Europe/Berlin"))
                dt = dt.astimezone(timezone.utc)

            except Exception:
                dt = raw_date
        else:
            dt = None

        # 作者
        author_tag = item.find("a", class_="humble humble--author")
        author = author_tag.get_text(strip=True) if author_tag else None

        page_data.append({
            "title": title,
            "link": link,
            "author": author,
            "date": dt
        })

    print(f"✅ 第 {page} 页获取 {len(page_data)} 条新闻。")
    return page_data     


def get_last_update_date():
    """
    从 PostgreSQL 数据库中读取最近一条新闻的日期
    """
    dbconn = os.getenv("DBCONN")  # Lambda 环境变量中保存连接字符串
    conn = psycopg.connect(dbconn)
    cur = conn.cursor()
    cur.execute("SELECT MAX(date) FROM news;")   # ⚠️ 根据你的表名字段改
    last_date = cur.fetchone()[0]
    print(f"📌 数据库中最新的新闻日期: {last_date}")
    cur.close()
    conn.close()

    # 没数据时默认抓取最近 3 天
    if not last_date:
        last_date = datetime.now(timezone.utc).date() - timedelta(days=3)
    else:
        last_date = last_date.date()  # 转成纯日期

    return last_date






def lambda_handler(event, context):
    keyword = event.get("keyword", "BTC")
    max_pages = int(event.get("max_pages", 111))
    last_date = get_last_update_date()
    print(f"📌 从 {last_date} 之后的新闻开始爬取。")

    # today= datetime.now(timezone.utc).date()
    page = 0
    all_data = []
    stop = False


    while not stop and page < max_pages:

        page_data = crawl_utoday_page(keyword, page)

        # 如果没数据就停止
        if not page_data:
            print("✅ 没有更多数据，爬虫结束。")
            break


            # 按日期过滤，只要今天的
        for item in page_data:
            if isinstance(item["date"], str):
                try:
                    item["date"] = datetime.fromisoformat(item["date"].replace("Z", "+00:00"))
                except Exception:
                    continue  # 格式错误跳过
            news_date = item["date"].astimezone(timezone.utc).date()

            if news_date > last_date:
                all_data.append(item)
            elif news_date <= last_date:
                stop = True  # 一旦出现昨天的新闻就停
                print(f"⏹ 遇到 {news_date} (≤ {last_date})，停止爬取。")
                break

        # 合并数据
        # all_data.extend(page_data)

        # 翻页
        page += 1

        # 随机延迟，防止被封
        time.sleep(random.uniform(1, 3))

    print(f"共爬取 {len(all_data)} 条新闻。")
    return all_data

    # for i, item in enumerate(all_data, start=1):
    #     print(f"\n📰 第 {i} 条")
    #     print(f"标题: {item['title']}")
    #     print(f"作者: {item['author']}")
    #     print(f"日期: {item['date']}")
    #     print(f"链接: {item['link']}")
