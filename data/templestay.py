import re
import json
import time
import logging
import yaml
import os

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache

from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

CONFIG_PATH = 'C:\\jeolloga-crawling\\data\\db_config.yaml'

def load_config(path=CONFIG_PATH):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)

config = load_config()
DB_CONFIG = config['database']

connection_pool = None

def init_connection_pool():
    global connection_pool
    try:
        connection_pool = MySQLConnectionPool(**DB_CONFIG)
        print("데이터베이스 연결 풀 생성 완료")
    except mysql.connector.Error as err:
        print(f"상세 오류: {type(err)} - {str(err)}")
        raise

def get_connection():
    global connection_pool
    if connection_pool is None:
        init_connection_pool()
    try:
        return connection_pool.get_connection()
    except mysql.connector.Error as err:
        print(f"연결 풀에서 연결 가져오기 실패: {err}")
        print("직접 데이터베이스 연결 시도 중...")
        return mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database']
        )

def create_driver(headless=True):
    options = Options()
    if headless:
        options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--blink-settings=imagesEnabled=false')
    options.add_argument('--disk-cache-size=52428800')
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_window_size(1024, 768)
    driver.set_page_load_timeout(20)
    return driver

@lru_cache(maxsize=100)
def extract_phone_number(phone_text):
    phone_pattern = re.compile(r'[\d\- /]+')
    match = phone_pattern.search(phone_text)
    if not match:
        return None
    phone = match.group().strip()
    if '/' in phone:
        phone = phone.split('/')[-1].strip()
    return phone

def extract_image_urls(soup):
    """이미지 URL들을 추출하는 함수"""
    image_urls = []
    
    # swiper-slide 내의 이미지들 추출
    swiper_slides = soup.find_all('div', class_='swiper-slide')
    for slide in swiper_slides:
        img = slide.find('img')
        if img and img.get('src'):
            src = img.get('src')
            # 상대 경로를 절대 경로로 변환
            if src.startswith('/'):
                src = 'https://www.templestay.com' + src
            image_urls.append(src)
    
    img_tags = soup.find_all('img')
    for img in img_tags:
        src = img.get('src')
        if src and 'templePrg' in src:
            if src.startswith('/'):
                src = 'https://www.templestay.com' + src
            if src not in image_urls:
                image_urls.append(src)
    
    return image_urls

def parse_program_schedule(html): 
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    if not table:
        return None

    schedule_dict = OrderedDict()
    current_day = None

    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')

        if len(cols) == 3:
            day_cell = cols[0].get_text(strip=True)
            time_cell = cols[1].get_text(strip=True)
            activity_cell = cols[2].get_text(strip=True)

            if day_cell:
                current_day = day_cell.replace(" ", "")
                if current_day not in schedule_dict:
                    schedule_dict[current_day] = OrderedDict()

            if current_day:
                schedule_dict[current_day][time_cell] = activity_cell

        elif len(cols) == 2 and current_day:
            time_cell = cols[0].get_text(strip=True)
            activity_cell = cols[1].get_text(strip=True)
            schedule_dict[current_day][time_cell] = activity_cell

    return json.dumps(schedule_dict, ensure_ascii=False, separators=(',', ':'))

def extract_introduction_text(soup):
    program_sections = []
    for section in soup.find_all('div', class_='section'):
        h4 = section.find('h4')
        if h4 and '프로그램 소개' in h4.get_text():
            txt_div = section.find('div', class_='txt')
            if txt_div and txt_div.find('p'):
                program_sections.append(txt_div)

    if not program_sections:
        for div in soup.find_all('div'):
            img = div.find('img', alt='프로그램 소개')
            if img:
                parent_section = div.parent
                txt_div = parent_section.find('div', class_='txt')
                if txt_div and txt_div.find('p'):
                    program_sections.append(txt_div)

    all_text = []
    for section in program_sections:
        p_tags = section.find_all('p')
        for p in p_tags:
            text = p.get_text()
            clean_text = '\n'.join([line.strip() for line in text.splitlines() if line.strip()])
            all_text.append(clean_text)

    return '\n\n'.join(all_text) if all_text else None

def crawl_templestay_details(url, driver=None):
    close_driver = False
    if driver is None:
        driver = create_driver()
        close_driver = True
    
    try:
        logger.info(f"크롤링 시작: {url}")
        driver.get(url)
        time.sleep(0.8)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        place_div = soup.find('div', class_='place')
        if not place_div:
            logger.warning(f"place div 없음: {url}")
            return (None, None, None, None, None, None, [])

        templestay_name = place_div.find('h3').get_text(strip=True) if place_div.find('h3') else None

        temple_name = None
        address = None
        phone = None

        info_div = place_div.find('div', class_='info')
        if info_div:
            lis = info_div.find_all('li')
            for li in lis:
                img = li.find('img')
                text_label = img['alt'] if img and 'alt' in img.attrs else ''
                text_nodes = li.find_all(text=True, recursive=False)
                text_value = ''.join(t.strip() for t in text_nodes if t.strip())

                if '주소' in text_label:
                    parts = [p.strip() for p in text_value.split(',', 1)]
                    if len(parts) == 2:
                        temple_name = parts[0]
                        address = parts[1]
                    else:
                        address = text_value
                elif '연락처' in text_label or re.search(r'\d{2,3}[-\s]?\d{3,4}[-\s]?\d{4}', text_value):
                    phone = extract_phone_number(text_value) or phone

        introduction = extract_introduction_text(soup)

        schedule_json = None
        for section in soup.find_all("div", class_="section"):
            h4 = section.find("h4")
            if h4 and "프로그램 일정" in h4.get_text():
                schedule_div = section.find("div", class_="table")
                if schedule_div:
                    table = schedule_div.find("table")
                    if table:
                        schedule_json = parse_program_schedule(str(table))
                break

        # 이미지 URL 추출
        image_urls = extract_image_urls(soup)

        logger.info(f"크롤링 완료: {templestay_name} ({temple_name}), 이미지 {len(image_urls)}개")

        return (templestay_name, temple_name, address, phone, introduction, schedule_json, image_urls)

    except Exception as e:
        logger.error(f"크롤링 실패 ({url}): {e}")
        return (None, None, None, None, None, None, [])
    finally:
        if close_driver:
            driver.quit()

def fetch_urls_from_db():
    conn = get_connection()
    cursor = conn.cursor(buffered=True)
    try:
        cursor.execute("SELECT id, url FROM templestay WHERE templestay_name IS NULL ORDER BY id")
        results = cursor.fetchall()
        logger.info(f"templestay_name이 NULL인 {len(results)}개의 URL 불러옴")
        return results
    finally:
        cursor.close()
        conn.close()

def update_templestay_batch(batch_data):
    if not batch_data:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    success_count = 0

    try:
        query = """
            UPDATE templestay 
            SET templestay_name = %s,
                temple_name = %s,
                address = %s,
                phone = %s,
                introduction = %s, 
                schedule = %s,
                updated_at = NOW()
            WHERE id = %s
        """
        cursor.executemany(query, batch_data)
        conn.commit()
        success_count = cursor.rowcount
        logger.info(f"배치 업데이트 완료: {success_count}건")

    except Exception as e:
        logger.error(f"배치 업데이트 실패: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    return success_count

def insert_images_batch(image_data):
    """이미지 데이터를 배치로 삽입하는 함수"""
    if not image_data:
        return 0

    conn = get_connection()
    cursor = conn.cursor()
    success_count = 0

    try:
        query = """
            INSERT INTO image (templestay_id, img_url, created_at) 
            VALUES (%s, %s, NOW())
        """
        cursor.executemany(query, image_data)
        conn.commit()
        success_count = cursor.rowcount
        logger.info(f"이미지 배치 삽입 완료: {success_count}건")

    except Exception as e:
        logger.error(f"이미지 배치 삽입 실패: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    return success_count

def process_url_batch(urls_batch):
    driver = create_driver()
    batch_data = []
    image_data = []
    
    try:
        for templestay_id, url in urls_batch:
            templestay_name, temple_name, address, phone, introduction, schedule, image_urls = crawl_templestay_details(url, driver)
            
            if templestay_name or temple_name or address or phone or introduction or schedule:
                batch_data.append((
                    templestay_name,
                    temple_name,
                    address,
                    phone,
                    introduction,
                    schedule,
                    templestay_id
                ))
            
            # 이미지 URL이 있으면 이미지 데이터에 추가
            for img_url in image_urls:
                image_data.append((templestay_id, img_url))
            
            time.sleep(0.2)
    finally:
        driver.quit()
    
    return batch_data, image_data

def main(batch_size=20, max_workers=3):
    try:
        try:
            init_connection_pool()
        except Exception as e:
            logger.warning(f"연결 풀 초기화 실패, 단일 연결 모드로 전환: {e}")

        url_data = fetch_urls_from_db()
        logger.info(f"전체 처리 대상: {len(url_data)}개")

        successful_updates = 0
        successful_images = 0
        batches = [url_data[i:i+batch_size] for i in range(0, len(url_data), batch_size)]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {executor.submit(process_url_batch, batch): batch for batch in batches}
            all_batch_data = []
            all_image_data = []

            for future in as_completed(future_to_batch):
                try:
                    batch_data, image_data = future.result()
                    all_batch_data.extend(batch_data)
                    all_image_data.extend(image_data)
                except Exception as e:
                    logger.error(f"배치 처리 중 오류 발생: {e}")

            # templestay 데이터 업데이트
            if all_batch_data:
                bulk_batch_size = 100
                for i in range(0, len(all_batch_data), bulk_batch_size):
                    bulk_batch = all_batch_data[i:i + bulk_batch_size]
                    success_count = update_templestay_batch(bulk_batch)
                    successful_updates += success_count

            # 이미지 데이터 삽입
            if all_image_data:
                bulk_batch_size = 200  # 이미지는 더 많은 수로 배치 처리
                for i in range(0, len(all_image_data), bulk_batch_size):
                    bulk_batch = all_image_data[i:i + bulk_batch_size]
                    success_count = insert_images_batch(bulk_batch)
                    successful_images += success_count

        logger.info(f"작업 완료: templestay {successful_updates}건, 이미지 {successful_images}건 처리 성공")

    except Exception as e:
        logger.error(f"프로그램 실행 중 오류 발생: {e}")
        import traceback
        logger.error(f"상세 오류 내용: {traceback.format_exc()}")

if __name__ == "__main__":
    main(batch_size=10, max_workers=1)