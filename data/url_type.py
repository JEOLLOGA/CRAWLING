import requests
from bs4 import BeautifulSoup
import mysql.connector
import re
import time
import pickle
import os
import yaml

TYPE_BIT_MAP = {
    "당일형": 0b001,
    "휴식형": 0b010,
    "체험형": 0b100,
}

def load_db_config(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
        return config.get("database")

def get_connection(config):
    try:
        return mysql.connector.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"]
        )
    except mysql.connector.Error as e:
        print(f"DB 연결 오류: {e}")
        return None

def build_reserve_url(seq, bookmark_id):
    return (
        "https://www.templestay.com/fe/MI000000000000000062/reserve/view.do"
        f"?pageIndex=1&areaCd=&templestaySeq={seq}&templeBookMarkId={bookmark_id}"
        "&templeIdTmp=&areaSelect=&templeId=&templePrgType=&searchCnt=&searchStaDate=&searchEndDate=&searchKeyword="
    )

def type_to_binary(type_text):
    return TYPE_BIT_MAP.get(type_text.strip(), 0)

def extract_url_and_type(li):
    strong_tag = li.select_one('div.txt > strong')
    href = strong_tag.get("onclick", "") if strong_tag else ""
    match = re.search(r"fncReserve\('(\d+)',\s*'([\w_]+)'\)", href)
    if not match:
        return None, None, None
    seq, bookmark_id = match.groups()
    full_url = build_reserve_url(seq, bookmark_id)

    spans = li.select('span[class^="cate"]')
    type_bits = 0
    for span in spans:
        bit = type_to_binary(span.get_text(strip=True))
        if bit:
            type_bits |= bit

    program_id = seq
    return full_url, type_bits, program_id

def load_url_cache():
    if os.path.exists('url_cache.pkl'):
        with open('url_cache.pkl', 'rb') as f:
            return pickle.load(f)
    return set()

def save_url_cache(url_set):
    with open('url_cache.pkl', 'wb') as f:
        pickle.dump(url_set, f)

def batch_insert_and_upsert(conn, url_type_list):
    if not url_type_list:
        return

    cursor = conn.cursor(dictionary=True)

    try:
        valid_url_type_list = [(url, type_bits, program_id) for url, type_bits, program_id in url_type_list if type_bits > 0]
        if not valid_url_type_list:
            return

        url_params = [(url,) for url, _, _ in valid_url_type_list]
        cursor.executemany("INSERT IGNORE INTO templestay (url) VALUES (%s)", url_params)

        url_to_type = {url: type_bits for url, type_bits, _ in valid_url_type_list}
        url_keys_tuple = tuple(url_to_type.keys())

        if url_keys_tuple:
            placeholders = ','.join(['%s'] * len(url_keys_tuple))
            query = f"SELECT id, url FROM templestay WHERE url IN ({placeholders})"
            cursor.execute(query, url_keys_tuple)
            rows = cursor.fetchall()
        else:
            rows = []

        filter_data = [(row["id"], url_to_type[row["url"]]) for row in rows]
        cursor.executemany("""
            INSERT INTO filter (templestay_id, type)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE type=VALUES(type)
        """, filter_data)

        conn.commit()
    except Exception as e:
        print(f"DB 작업 중 오류 발생: {e}")
        conn.rollback()
    finally:
        cursor.close()

def crawl_and_process(config, start_page=1, end_page=50, batch_size=100):
    base_url = "https://www.templestay.com/fe/MI000000000000000062/templestay/prgList.do?pageIndex="
    conn = get_connection(config)
    if not conn:
        print("DB 연결 실패로 크롤링 중단")
        return

    url_cache = load_url_cache()
    batch = []

    try:
        for page in range(start_page, end_page + 1):
            try:
                print(f"{page} 페이지 처리 중")
                res = requests.get(base_url + str(page), timeout=10)
                res.raise_for_status()

                soup = BeautifulSoup(res.text, 'html.parser')
                list_items = soup.select('div.myplace_list > ul > li')
                if not list_items:
                    continue

                for li in list_items:
                    url, type_bits, program_id = extract_url_and_type(li)
                    if not url or url in url_cache or type_bits == 0:
                        continue
                    batch.append((url, type_bits, program_id))
                    url_cache.add(url)

                    if len(batch) >= batch_size:
                        batch_insert_and_upsert(conn, batch)
                        print(f">> {len(batch)}건 DB 저장 완료")
                        batch.clear()
                        save_url_cache(url_cache)

                time.sleep(1)

            except Exception as e:
                print(f"{page} 페이지 에러: {e}")

        if batch:
            batch_insert_and_upsert(conn, batch)
            print(f">> 마지막 배치 {len(batch)}건 DB 저장 완료")
            save_url_cache(url_cache)

    finally:
        conn.close()

if __name__ == "__main__":
    db_config_path = "C:\\jeolloga-crawling\\data\\db_config.yaml"
    db_config = load_db_config(db_config_path)
    crawl_and_process(db_config, start_page=1, end_page=50)