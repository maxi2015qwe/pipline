import os
import psycopg
from datetime import datetime, timezone


def save_BTC_price_to_postgres(event, context):
    price=event
    dbconn = os.getenv("DBCONN")
    conn = psycopg.connect(dbconn)
    cur = conn.cursor()

    # ✅ 创建表（如果不存在）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS BTC_price (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL UNIQUE,
            open_price DOUBLE PRECISION,
            high_price DOUBLE PRECISION,
            low_price DOUBLE PRECISION,
            close_price DOUBLE PRECISION,
            volume BIGINT
        );
    """)

    cur.execute("SELECT COALESCE(MAX(timestamp), '1970-01-01') FROM BTC_price;")
    last_timestamp = cur.fetchone()[0]
    last_date = last_timestamp.date() if last_timestamp else datetime(1970, 1, 1).date()
    print(f"📌 数据库里已有的最新日期: {last_date}")

    records_to_insert = []
    for item in price:
        # ✅ 只保留今天的数据
        ts = datetime.fromisoformat(item["timestamp"])
        if ts.date <= last_date:
            continue
        # ✅ 插入数据并避免重复
        cur.executemany("""
            INSERT INTO BTC_price (timestamp, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING;
        """, [
            (
                item["timestamp"],
                item["open_price"],
                item["high_price"],
                item["low_price"],
                item["close_price"],
                item["volume_price"]
            )

        ])

    inserted_count = 0

    if records_to_insert:
        cur.executemany("""
            INSERT INTO BTC_price (timestamp, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING;
        """, records_to_insert)
        inserted_count = cur.rowcount

    conn.commit()
    cur.close()
    conn.close()


    print(f"✅ 已保存 {inserted_count} 条 BTC 新价格记录到 PostgreSQL。")
    return {
        "statusCode": 200,
        "body": f"Successfully inserted {inserted_count} new BTC price records."
    }


