import pandas as pd
import pymysql
import yaml
import re

ETC_MAP = {
    '주차 가능': 0b001,
    '1인실': 0b010,
    '단체 가능': 0b100,
}

def load_db_config(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file).get("database")

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

def normalize(name):
    if not isinstance(name, str):
        return ""
    name = name.strip()
    name = re.sub(r'\s+', '', name)
    name = name.replace('（', '(').replace('）', ')')
    name = name.replace('\xa0', '')
    return name

def load_temple_name_to_ids(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT id, temple_name FROM templestay WHERE temple_name IS NOT NULL")
        rows = cursor.fetchall()
        mapping = {}
        for row in rows:
            key = normalize(row['temple_name'])
            mapping.setdefault(key, []).append(row['id'])
        return mapping

def calculate_etc_bit(df):
    df['temple_name'] = df['temple_name'].fillna('').apply(normalize)
    df['etc'] = df['etc'].fillna('')
    return (
        df.groupby('temple_name')['etc']
        .apply(lambda items: sum(
            ETC_MAP.get(bit.strip(), 0)
            for line in items
            for bit in line.split(',')
            if bit.strip()
        ))
        .reset_index(name='etc_bit')
    )

def generate_case_update_sql(templestay_id_bit_pairs):
    case_lines = []
    ids = []
    for tid, bit in templestay_id_bit_pairs:
        case_lines.append(f"WHEN f.templestay_id = {tid} THEN {bit}")
        ids.append(str(tid))
    case_sql = "SET f.etc = CASE\n" + "\n".join(case_lines) + "\nELSE f.etc END"
    where_in = ", ".join(ids)
    return f"""
    UPDATE filter f
    {case_sql}
    WHERE f.templestay_id IN ({where_in});
    """

def main():
    csv_path = "C:\\jeolloga-crawling\\data\\etc.csv"
    db_config_path = "C:\\jeolloga-crawling\\data\\db_config.yaml"
    df = pd.read_csv(csv_path, encoding='cp949')
    config = load_db_config(db_config_path)
    conn = get_connection(config)

    try:
        name_to_ids = load_temple_name_to_ids(conn)
        etc_df = calculate_etc_bit(df)

        templestay_id_bit_pairs = []
        for _, row in etc_df.iterrows():
            temple_name = row['temple_name']
            bit = row['etc_bit']
            ids = name_to_ids.get(temple_name, [])
            for tid in ids:
                templestay_id_bit_pairs.append((tid, bit))

        if not templestay_id_bit_pairs:
            print("일치하는 temple_name이 없습니다. 업데이트 생략.")
            return

        sql = generate_case_update_sql(templestay_id_bit_pairs)
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()
        print("etc 비트 업데이트 완료")

    except Exception as e:
        conn.rollback()
        print(f"에러 발생: {e}")
    finally:
        conn.close()

main()