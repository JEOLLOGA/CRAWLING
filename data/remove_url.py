import pickle
import os
import mysql.connector
import yaml

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

def load_url_cache():
    if os.path.exists('url_cache.pkl'):
        with open('url_cache.pkl', 'rb') as f:
            return pickle.load(f)
    return set()

def delete_removed_urls(conn, current_urls):
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT id, url FROM templestay")
        db_rows = cursor.fetchall()

        removed_ids = [
            row["id"] for row in db_rows
            if row["url"] not in current_urls
        ]

        if removed_ids:
            format_ids = ",".join(map(str, removed_ids))
            print(f">> 삭제된 항목 {len(removed_ids)}건 templestay, filter에서 삭제 중...")

            cursor.execute(f"DELETE FROM filter WHERE templestay_id IN ({format_ids})")
            cursor.execute(f"DELETE FROM templestay WHERE id IN ({format_ids})")

            conn.commit()
        else:
            print(">> 삭제할 URL이 없습니다.")

    except Exception as e:
        print(f"삭제 작업 중 오류: {e}")
        conn.rollback()
    finally:
        cursor.close()

if __name__ == "__main__":
    db_config_path = "C:\\jeolloga-crawling\\data\\db_config.yaml"
    db_config = load_db_config(db_config_path)

    conn = get_connection(db_config)
    if not conn:
        print("DB 연결 실패")
        exit()

    url_cache = load_url_cache()
    delete_removed_urls(conn, url_cache)
    conn.close()