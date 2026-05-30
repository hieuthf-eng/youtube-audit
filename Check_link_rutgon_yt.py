import os
import re
import csv
import io
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from googleapiclient.discovery import build

# --- CẤU HÌNH ---
API_KEY = os.environ.get('YOUTUBE_API_KEY')
EMAIL_USER = os.environ.get('EMAIL_USER')
EMAIL_PASS = os.environ.get('EMAIL_PASS')
EMAIL_TO = os.environ.get('EMAIL_TO')

youtube = build('youtube', 'v3', developerKey=API_KEY)

# --- DANH SÁCH URL INPUT ---
# Bạn có thể dán 300 URL của bạn vào mảng này, hoặc viết hàm đọc từ file txt/csv tùy ý
YOUTUBE_URLS = [
    "https://www.youtube.com/watch?v=qbq__igQSR0",
"https://www.youtube.com/watch?v=7wiriULLN8g",
"https://www.youtube.com/watch?v=w0N-LhDlV0I",
"https://www.youtube.com/watch?v=kVKVzIxdSNU",
"https://www.youtube.com/watch?v=B259qUCluS0",
"https://www.youtube.com/watch?v=OFSGDi81oOs",
"https://www.youtube.com/watch?v=jqoTmzFHbbo",
"https://www.youtube.com/watch?v=pQNCwIvFiCw",
"https://www.youtube.com/watch?v=d7nLoMWwwKA&t=57s",
"https://www.youtube.com/watch?v=a9ClO2kNNFU",
"https://www.youtube.com/watch?v=qeMqsVQcv44",
"https://www.youtube.com/watch?v=b_xr0eUQCkA",
"https://www.youtube.com/watch?v=CI2iMeBkZHE",
"https://www.youtube.com/watch?v=Qck5FkeQoR4",
"https://www.youtube.com/watch?v=J5tifH8zp4w",
"https://www.youtube.com/watch?v=73NVnQ5nhQQ&list=PLucM008t8hmLdbjkshFtIXGDIJ4LkfAYy",
"https://www.youtube.com/watch?v=b7NQz7efXgA&list=PLucM008t8hmIjAedJrVMQEOl87CWoTPTj",
"https://www.youtube.com/watch?v=gYs-9AO6gjY&list=PLucM008t8hmIgLD12xUow3ZKLhi3vSmXb",
"https://www.youtube.com/watch?v=--IGSqAcDwk&list=PLucM008t8hmKCBP0zSlUnte1Vd2y6tBdO",
"https://www.youtube.com/watch?v=cgkAtpgDjlg&list=PLucM008t8hmJ6E8f9EMATQLSTcDVGlzSL",
"https://www.youtube.com/watch?v=uiJaaCODIUE&list=PLucM008t8hmLRV9CNJOAR0J_BuQdUgTBV",
"https://www.youtube.com/watch?v=LJ05aQcRPOI&list=PLucM008t8hmL242QwLkL2BF6IB2RMRq26",
"https://www.youtube.com/watch?v=aUN-uS7mHRY&list=PLucM008t8hmKH5yzsPv3gwATwJnLj0FO8",
"https://www.youtube.com/watch?v=Cw9bOQxHYbA",
"https://www.youtube.com/watch?v=dJ-6TZtGC_Y",
"https://www.youtube.com/watch?v=bW7PzoCebpU",
"https://www.youtube.com/watch?v=2MA21fHX7gw",
"https://www.youtube.com/watch?v=SQqA95BzSHo",
"https://www.youtube.com/watch?v=8p-Tl9OkTWo",
"https://www.youtube.com/watch?v=bM0pR6B9GJ4",
"https://www.youtube.com/watch?v=idofi6By3dE",
"https://www.youtube.com/watch?v=MLCDE-SvfvM",
"https://www.youtube.com/watch?v=JnXlmYSl4Fg",
"https://www.youtube.com/watch?v=wLWGMByrVxY",
"https://www.youtube.com/watch?v=CLrcXq3dojg",
"https://www.youtube.com/watch?v=RU5AxhAIHl0",
"https://www.youtube.com/watch?v=dM32v940h6Q",
"https://www.youtube.com/watch?v=6w4Jt9DY0Kw",
"https://www.youtube.com/watch?v=MydBKulUKcc",
"https://www.youtube.com/watch?v=owipLY6_pyU",
"https://www.youtube.com/watch?v=ekoAi4rg5OU",
"https://www.youtube.com/watch?v=072HBCGjC5E",
"https://www.youtube.com/watch?v=WQOP9NtjTDg",
"https://www.youtube.com/watch?v=_nmYzBZrqqw",
"https://www.youtube.com/watch?v=4QeMGU1iqbk",
"https://www.youtube.com/watch?v=kUzlmANbNow",
"https://www.youtube.com/watch?v=zge9WwauLSc",
"https://www.youtube.com/watch?v=BbBzyhTBNZI",
"https://www.youtube.com/watch?v=f_UvFO2x-RE",
"https://www.youtube.com/watch?v=xl9bUsvXlRM",
"https://www.youtube.com/watch?v=EdcbU6G-QZM",
"https://www.youtube.com/watch?v=4_EMJN_gs2Y",
"https://www.youtube.com/watch?v=KX68BvY8MAw",
"https://www.youtube.com/watch?v=xKR_j8tC700",
"https://www.youtube.com/watch?v=2OxgMdHi05c",
"https://www.youtube.com/watch?v=sbo6CLtfqu8",
"https://www.youtube.com/watch?v=K7pY16bVVrE",
"https://www.youtube.com/watch?v=lt8Q0zUqvNs",
"https://www.youtube.com/watch?v=Jd1N6tgz6cw",
"https://www.youtube.com/watch?v=mQ17hIfZ8Xo",
"https://www.youtube.com/watch?v=PZFjPxlADbQ",
"https://www.youtube.com/watch?v=NTnf3TPWTUY",
"https://www.youtube.com/watch?v=U8xUtIHhIlU",
"https://www.youtube.com/watch?v=grU4U2SUuyc",
"https://www.youtube.com/watch?v=B-uwqMmVKAQ",
"https://www.youtube.com/watch?v=Ebk05S4MPNs",
"https://www.youtube.com/watch?v=nv_bo5FqsZ8",
"https://www.youtube.com/watch?v=FHaApFuyfV4",
"https://www.youtube.com/watch?v=-Y5lfYrM5k8",
"https://www.youtube.com/watch?v=2OMwMsNgZ8s",
"https://www.youtube.com/watch?v=frLBPor0Xf4",
"https://www.youtube.com/watch?v=BBPDHkDvVGs",
"https://www.youtube.com/watch?v=hgX4UyyCezQ",
"https://www.youtube.com/watch?v=bSr3rIOINCI",
"https://www.youtube.com/watch?v=k_m0ERuH7jA",
"https://www.youtube.com/watch?v=7Km2tKbJlzk",
"https://www.youtube.com/watch?v=7Km2tKbJlzk",
"https://www.youtube.com/watch?v=0tFzix3DUKQ",
"https://www.youtube.com/watch?v=cf7ZvP4oorg",
"https://www.youtube.com/watch?v=SXGfyWCvVK0",
"https://www.youtube.com/watch?v=nkCv6sRabqA",
"https://www.youtube.com/watch?v=o8ljnrnfyDQ",
"https://www.youtube.com/watch?v=lv4qP8HoQFM",
"https://www.youtube.com/watch?v=_IvxP1JEZoI",
"https://www.youtube.com/watch?v=DXZ9FJ9hgIU",
"https://www.youtube.com/watch?v=f-K3uiFRWMk",
"https://www.youtube.com/watch?v=VBfK_nu76AM",
"https://www.youtube.com/watch?v=ThKyknRnHkI",
"https://www.youtube.com/watch?v=9WTUdt_6pFo",
"https://www.youtube.com/watch?v=Eg-uh-3Y-e0",
"https://www.youtube.com/watch?v=bjG9ZqTMXp8",
"https://www.youtube.com/watch?v=ouW5T-aY2ZY",
"https://www.youtube.com/watch?v=63tY7-HOBWA",
"https://www.youtube.com/watch?v=2Ams66-n750",
"https://www.youtube.com/watch?v=rzGFoc30BrQ",
"https://www.youtube.com/watch?v=WTJ08yNPtLc",
"https://www.youtube.com/watch?v=9yM9ZEc3cfg",
"https://www.youtube.com/watch?v=dlrgpRVIm68",
"https://www.youtube.com/watch?v=xnFYcLnqkJs",
"https://www.youtube.com/watch?v=UBwiQJOAgI0",
"https://www.youtube.com/watch?v=pYzgYxxEwiY",
"https://www.youtube.com/watch?v=fpMGQGWVjRs",
"https://www.youtube.com/watch?v=tJmbTyWbBkM",
"https://www.youtube.com/watch?v=9nXtIipBzEA",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://dib.vn/mo-tai-khoan-exness-extrading",
"https://www.youtube.com/watch?v=r5Om2lMaFTI",
"https://www.youtube.com/watch?v=UX-pfwf0OVM",
"https://www.youtube.com/watch?v=gYs-9AO6gjY",
"https://www.youtube.com/watch?v=--IGSqAcDwk",
"https://www.youtube.com/watch?v=eOEAWd9z2aE",
"https://www.youtube.com/watch?v=wFL2mjUTnNg",
"https://www.youtube.com/watch?v=GEzK65u_hiw",
"https://www.youtube.com/watch?v=ZQhbalU2T_A",
"https://www.youtube.com/watch?v=O9uVX7s-H_E",
"https://www.youtube.com/watch?v=d17_2KEewd0",
"https://www.youtube.com/watch?v=gzvSfkHxECQ",
"https://www.youtube.com/watch?v=_BlDH0LNOZ8",
"https://www.youtube.com/watch?v=_M_2zoJTVF4",
"https://www.youtube.com/watch?v=a9ClO2kNNFU",
"https://www.youtube.com/watch?v=moknGAeD1dE",
"https://www.youtube.com/watch?v=cgkAtpgDjlg",
"https://www.youtube.com/watch?v=H9dM0uKPvY4",
"https://www.youtube.com/watch?v=hdhTryAnfkM",
"https://www.youtube.com/watch?v=hOnBOxo7Gn4",
"https://www.youtube.com/watch?v=LJ05aQcRPOI",
"https://www.youtube.com/watch?v=--IGSqAcDwk",
"https://www.youtube.com/watch?v=ESGWO9BIff4",
"https://www.youtube.com/watch?v=73NVnQ5nhQQ",
"https://www.youtube.com/watch?v=b7NQz7efXgA",
"https://www.youtube.com/watch?v=rlMtLf8pIWw",
"https://www.youtube.com/watch?v=IRNedjhQZ-0",
"https://www.youtube.com/watch?v=SoXTfsPBheM",
"https://www.youtube.com/watch?v=9R7OsXcMd_k",
"https://www.youtube.com/watch?v=FeE_HDct6sQ",
"https://www.youtube.com/watch?v=XlHWoX_Iwn8",
"https://www.youtube.com/watch?v=HQ3bbW5cmO4",
"https://www.youtube.com/watch?v=uiJaaCODIUE",
"https://www.youtube.com/watch?v=S6QEAe6Gi8k",
"https://www.youtube.com/watch?v=8W0KN4EWT0o",
"https://www.youtube.com/watch?v=kGSVfXNHOlk",
"https://www.youtube.com/watch?v=4K6rPQqmZq4",
"https://www.youtube.com/watch?v=3JqCjGnuFbY",
"https://www.youtube.com/watch?v=u5yZZUPuBE0",
"https://www.youtube.com/watch?v=07ZOB0V4mfk",
"https://www.youtube.com/watch?v=s-X9M0eQ6CE",
"https://www.youtube.com/watch?v=K6_FtZ5ZMuI",
"https://www.youtube.com/watch?v=ohks5jpLI20",
"https://www.youtube.com/watch?v=yp4aHRfC5jo",
"https://www.youtube.com/watch?v=Ct1clov0CBM",
"https://www.youtube.com/watch?v=XOfstXju_Zc",
"https://www.youtube.com/watch?v=rSketQw-X44",
"https://www.youtube.com/watch?v=UWdd0BenftY",
"https://www.youtube.com/watch?v=-id7HszWX8o",
"https://www.youtube.com/watch?v=rX5YsZtGyA0",
"https://www.youtube.com/watch?v=fWZM31YNlgs",
"https://www.youtube.com/watch?v=pbQmLUNRWQs",
"https://www.youtube.com/watch?v=UsM7Hm1jzD0",
"https://www.youtube.com/watch?v=TyGLnRV7ciU",
"https://www.youtube.com/watch?v=HvYNyOvTe2A",
"https://www.youtube.com/watch?v=Y1Tj4wq9uiw",
"https://www.youtube.com/watch?v=zkmmyO3jsvk",
"https://www.youtube.com/watch?v=wbt0TxW1IP4",
"https://www.youtube.com/watch?v=fSxxuFp3tcc",
"https://www.youtube.com/watch?v=bjZx899WlBE",
"https://www.youtube.com/watch?v=eCu1fi6b0Fo",
"https://www.youtube.com/watch?v=zcEuEerfaqM",
"https://www.youtube.com/watch?v=3JqCjGnuFbY",
"https://www.youtube.com/watch?v=v1M5kkuhbzQ",
"https://www.youtube.com/watch?v=g_iwjmsEBlo",
"https://www.youtube.com/watch?v=YnR3uVJLAu0",
"https://www.youtube.com/watch?v=Tga7aExAJMQ",
# ... Dán toàn bộ danh sách 300 URL của bạn vào đây ...
]

# --- BIẾN LƯU KẾT QUẢ ---
CSV_DATA = []
CSV_HEADER = ['STT', 'URL Video', 'Trạng thái', 'Tiêu đề (Nếu có)', 'Chi tiết trạng thái']

def log(message):
    print(message, flush=True)

def extract_video_id(url):
    """Trích xuất Video ID từ URL YouTube công thức chung"""
    parsed_id = re.search(r'(?:v=|\/v\/|embed\/|youtu\.be\/|\/shorts\/|^)([a-zA-Z0-9_-]{11})', url)
    return parsed_id.group(1) if parsed_id else None

def check_videos_status(urls):
    """Kiểm tra trạng thái hàng loạt video bằng cách gom nhóm 50 ID/request"""
    video_map = {}
    id_to_url = {}
    
    # Bước 1: Trích xuất ID và lọc các URL hợp lệ
    for url in urls:
        v_id = extract_video_id(url)
        if v_id:
            id_to_url[v_id] = url
            # Mặc định ban đầu giả định là bị xóa hoặc sai ID, sẽ cập nhật lại khi API trả về dữ liệu
            video_map[v_id] = {
                'url': url,
                'status': 'Đã xóa / Không tồn tại',
                'title': 'N/A',
                'privacy': 'N/A'
            }
        else:
            CSV_DATA.append([len(CSV_DATA) + 1, url, 'URL Không hợp lệ', 'N/A', 'Không parse được ID'])

    video_ids = list(id_to_url.keys())
    log(f"Tổng số ID hợp lệ cần check: {len(video_ids)}")

    # Bước 2: Gom nhóm bốc đầu 50 ID mỗi lượt gửi lên API để tiết kiệm quota
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        chunk_str = ','.join(chunk)
        
        try:
            # Gọi part='status,snippet' để check cả quyền riêng tư lẫn tiêu đề
            request = youtube.videos().list(id=chunk_str, part='status,snippet')
            response = request.execute()
            
            # Cập nhật thông tin cho các video tìm thấy (đang tồn tại công khai hoặc hạn chế công khai)
            found_ids = []
            for item in response.get('items', []):
                v_id = item['id']
                found_ids.append(v_id)
                
                title = item['snippet']['title']
                privacy_status = item['status']['privacyStatus'] # public, unlisted, hoặc private
                
                if privacy_status == 'public':
                    status_text = 'Đang hoạt động'
                    detail_text = 'Công khai (Public)'
                elif privacy_status == 'unlisted':
                    status_text = 'Hạn chế'
                    detail_text = 'Không công khai (Unlisted)'
                elif privacy_status == 'private':
                    status_text = 'Bị ẩn'
                    detail_text = 'Riêng tư (Private)'
                else:
                    status_text = 'Không xác định'
                    detail_text = privacy_status
                    
                video_map[v_id] = {
                    'url': id_to_url[v_id],
                    'status': status_text,
                    'title': title,
                    'privacy': detail_text
                }
                
        except Exception as e:
            log(f"Lỗi khi check nhóm {i}-{i+50}: {e}")

    # Bước 3: Đổ dữ liệu vào bảng CSV_DATA theo đúng thứ tự
    for v_id, info in video_map.items():
        CSV_DATA.append([
            len(CSV_DATA) + 1,
            info['url'],
            info['status'],
            info['title'],
            info['privacy']
        ])

def send_email_with_csv(total_count):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    msg['Subject'] = f"[Hệ thống] Kết quả kiểm tra trạng thái {total_count} URL Video"

    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(CSV_HEADER)
    csv_writer.writerows(CSV_DATA)
    csv_bytes = csv_buffer.getvalue().encode('utf-8-sig')
    
    filename = "Ket_qua_kiem_tra_trang_thai_video.csv"
    attachment = MIMEApplication(csv_bytes, Name=filename)
    attachment['Content-Disposition'] = f'attachment; filename="{filename}"'
    msg.attach(attachment)

    body = (f"Chào bạn,\n\nHệ thống đã hoàn tất quét danh sách {total_count} đường link YouTube của bạn.\n"
            f"Kết quả phân loại trạng thái chi tiết (Hoạt động, Riêng tư, Đã xóa) đã được đính kèm trong file CSV bên dưới.\n"
            f"File đã xử lý chống lỗi font, bạn có thể mở trực tiếp bằng Excel để filter.")
    
    msg.attach(MIMEText(body, 'plain', 'utf-8'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
        server.quit()
        log(">> Đã gửi email báo cáo trạng thái link thành công!")
    except Exception as e:
        log(f"Lỗi gửi email: {e}")

def main():
    log("=== KHỞI CHẠY KIỂM TRA TRẠNG THÁI VIDEO HÀNG LOẠT ===")
    start_time = time.time()
    
    if not YOUTUBE_URLS:
        log("Danh sách URL rỗng.")
        return

    check_videos_status(YOUTUBE_URLS)
    
    if CSV_DATA:
        send_email_with_csv(len(YOUTUBE_URLS))
    else:
        log("Không có dữ liệu kết quả.")

    elapsed = round(time.time() - start_time, 2)
    log(f"=== HOÀN TẤT KIỂM TRA TRONG {elapsed} GIÂY ===")

if __name__ == "__main__":
    main()
