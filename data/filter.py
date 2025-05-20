import pymysql
import requests
from bs4 import BeautifulSoup
import time
import yaml

ACTIVITY_MAP = {
    '108배':         0b000001,
    '스님과의 차담': 0b000010,
    '새벽 예불':     0b000100,
    '염주 만들기':   0b001000,
    '연등 만들기':   0b010000,
    '명상':          0b100000,
}

REGION_MAP = {
    '강원': 1 << 0, '경기': 1 << 1, '경상남도': 1 << 2, '경상북도': 1 << 3,
    '광주': 1 << 4, '대구': 1 << 5, '대전': 1 << 6, '부산': 1 << 7,
    '서울': 1 << 8, '인천': 1 << 9, '전라남도': 1 << 10, '전라북도': 1 << 11,
    '제주': 1 << 12, '충청남도': 1 << 13, '충청북도': 1 << 14,
    '울산': 1 << 15, '세종': 1 << 16,
}

BATCH_SIZE = 100

def load_db_config(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
        return config.get("database")

def get_connection(config):
    return pymysql.connect(
        host=config["host"],
        user=config["user"],
        password=config["password"],
        db=config["database"],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )

def extract_price(soup):
    table = soup.select_one('div.table table')
    if not table:
        return 0
    rows = table.select('tr')
    for i, tr in enumerate(rows):
        th = tr.find('th')
        if th and '성인' in th.get_text():
            if i + 1 < len(rows):
                td = rows[i + 1].find('td')
                if td:
                    text = td.get_text(strip=True).replace(',', '').replace('원', '')
                    try:
                        return int(text)
                    except:
                        return 0
    return 0

def extract_activity(schedule):
    if not schedule:
        return 0
    bit = 0
    for k, v in ACTIVITY_MAP.items():
        if k in schedule:
            bit |= v
    return bit

def extract_region(address):
    if not address:
        return 0
    for region, bit in REGION_MAP.items():
        if region in address.split()[0]:
            return bit
    return 0

def batch_update_filter(config):
    conn = get_connection(config)
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT t.id, t.url, t.schedule, t.address, f.price AS old_price, f.activity AS old_activity, f.region AS old_region
                FROM templestay t
                JOIN filter f ON t.id = f.templestay_id
                WHERE f.price IS NULL OR f.activity IS NULL OR f.region IS NULL
                ORDER BY t.id ASC
            """)
            rows = cursor.fetchall()

        if not rows:
            print("업데이트할 대상이 없습니다.")
            return

        batch_data = []
        total_count = 0

        for idx, row in enumerate(rows, start=1):
            tid = row['id']
            url = row['url']
            schedule = row['schedule']
            address = row['address']
            old_price = row['old_price']
            old_activity = row['old_activity']
            old_region = row['old_region']

            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                detail_soup = BeautifulSoup(response.text, 'html.parser')
            except Exception as e:
                print(f"ID {tid} 크롤링 실패: {e}")
                continue

            new_price = extract_price(detail_soup)
            new_activity = extract_activity(schedule)
            new_region = extract_region(address)

            if (
                new_price != old_price or
                new_activity != old_activity or
                new_region != old_region
            ):
                batch_data.append((new_price, new_activity, new_region, tid))
                print(f"[{idx}] ID:{tid} 변경")
            else:
                print(f"[{idx}] ID:{tid} 변화 없음")

            if len(batch_data) >= BATCH_SIZE:
                with conn.cursor() as cursor:
                    cursor.executemany("""
                        UPDATE filter
                        SET price = %s,
                            activity = %s,
                            region = %s
                        WHERE templestay_id = %s
                    """, batch_data)
                conn.commit()
                print(f"{len(batch_data)}건 배치 업데이트 완료")
                total_count += len(batch_data)
                batch_data.clear()

            time.sleep(0.2)

        if batch_data:
            with conn.cursor() as cursor:
                cursor.executemany("""
                    UPDATE filter
                    SET price = %s,
                        activity = %s,
                        region = %s
                    WHERE templestay_id = %s
                """, batch_data)
            conn.commit()
            total_count += len(batch_data)
            print(f"{len(batch_data)}건 배치 업데이트 완료")

        print(f"\n총 {total_count}건 업데이트 완료")

    except Exception as e:
        print(f"에러 발생: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    db_config_path = "C:\\jeolloga-crawling\\data\\db_config.yaml"
    db_config = load_db_config(db_config_path)
    batch_update_filter(db_config)