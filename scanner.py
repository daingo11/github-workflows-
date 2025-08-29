import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
import csv
import time
import os
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ================= CẤU HÌNH =================
ADDRESS_FILE = "addresses.txt"
OUTPUT_FILE = "matches.csv"
CHECKPOINT_FILE = "checkpoint.txt"
BASE_URL = "https://lbc.cryptoguru.org/dio/"
THREADS = 5
MAX_REQUESTS_PER_SECOND = 2

# ================= TELEGRAM CONFIG =================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHAT_ID = os.environ.get("CHAT_ID")

def send_telegram_message(message):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        print("Chưa cấu hình Telegram token/chat ID. Bỏ qua gửi tin nhắn.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": message})
    except Exception as e:
        print(f"Lỗi khi gửi Telegram: {e}")

# ================= RATE LIMITER =================
class RateLimiter:
    def __init__(self, rate_per_sec):
        self.lock = threading.Lock()
        self.interval = 1 / rate_per_sec
        self.last_time = 0

    def wait(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_time
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.last_time = time.time()

rate_limiter = RateLimiter(MAX_REQUESTS_PER_SECOND)

# ================= ĐỌC DANH SÁCH ĐỊA CHỈ =================
with open(ADDRESS_FILE, "r") as f:
    my_addresses = set(line.strip() for line in f if line.strip())

# ================= CHECKPOINT =================
if os.path.exists(CHECKPOINT_FILE):
    with open(CHECKPOINT_FILE, "r") as f:
        start_page = int(f.read().strip())
    print(f"Tiếp tục từ trang {start_page} theo checkpoint.")
else:
    start_page = 1

# ================= ĐỌC CSV CŨ =================
previous_addresses = set()
file_exists = os.path.exists(OUTPUT_FILE)
if file_exists:
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            previous_addresses.add(row[1])

# ================= CẤU HÌNH REQUEST =================
session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

# ================= MỞ CSV =================
csvfile = open(OUTPUT_FILE, "a", newline="", encoding="utf-8")
writer = csv.writer(csvfile)
if not file_exists:
    writer.writerow(["Trang", "Địa chỉ trùng khớp"])
csv_lock = threading.Lock()

# ================= HÀM QUÉT 1 TRANG =================
def process_page(page):
    print(f"[{datetime.datetime.now()}] Đang kiểm tra trang {page} ...")
    try:
        rate_limiter.wait()
        response = session.get(f"{BASE_URL}{page}", timeout=10)
        if response.status_code != 200:
            print(f"Trang {page} không truy cập được (status {response.status_code}).")
            return None
    except Exception as e:
        print(f"Lỗi khi request trang {page}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    addresses_on_page = set()
    for a in soup.find_all("a", href=True):
        if "blockchain.info/address" in a['href']:
            addresses_on_page.add(a.text.strip())

    if not addresses_on_page:
        print(f"Trang {page} không có địa chỉ mới.")
        return None

    new_addresses = addresses_on_page - previous_addresses
    if not new_addresses:
        print(f"Không còn địa chỉ mới trên trang {page}.")
        return None

    matches = my_addresses.intersection(new_addresses)
    return (page, matches, addresses_on_page)

# ================= VÒNG LẶP MULTITHREAD =================
page = start_page
empty_pages_count = 0  # đếm số trang trống liên tiếp

while True:
    pages_to_check = [page + i for i in range(THREADS)]
    results = []

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        future_to_page = {executor.submit(process_page, p): p for p in pages_to_check}
        for future in as_completed(future_to_page):
            result = future.result()
            if result:
                results.append(result)

    if not results:
        empty_pages_count += 1
        if empty_pages_count >= 5:
            print("Không còn dữ liệu mới sau nhiều trang. Dừng.")
            break
        else:
            page += THREADS
            continue
    else:
        empty_pages_count = 0

    # Ghi CSV, cập nhật địa chỉ và gửi Telegram
    for page_num, matches, addresses_on_page in results:
        previous_addresses.update(addresses_on_page)
        if matches:
            print(f"Trang {page_num} có {len(matches)} địa chỉ trùng khớp.")
            with csv_lock:
                for addr in matches:
                    writer.writerow([page_num, addr])
                csvfile.flush()
            # Gửi Telegram
            message = f"Trang {page_num} có {len(matches)} địa chỉ trùng khớp:\n" + "\n".join(matches)
            send_telegram_message(message)

    # Cập nhật checkpoint
    max_page_checked = max(p[0] for p in results)
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(max_page_checked + 1))

    page = max_page_checked + 1

csvfile.close()
print("Hoàn tất quét.")
