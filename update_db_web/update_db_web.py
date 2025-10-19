import os
import psycopg


def save_to_postgres(event,context):
    news=event['']
    inserted = 0  # 👈 计数器
    conn = psycopg.connect(dbconn)
    cur = conn.cursor()
    for item in news:
        cur.execute("""
            INSERT INTO news (title, link, author, date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (link) DO NOTHING;
        """, (
            item.get("title"),
            item.get("link"),
            item.get("author"),
            item.get("date")
        ))
        if cur.rowcount > 0:
            inserted += 1  # 👈 成功插入时计数器加一
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ 已保存 {inserted} 条记录到 PostgreSQL。")

    return {
        "statusCode": 200,
        "body": f"Successfully inserted {inserted} news."
    }