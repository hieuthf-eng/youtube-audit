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
    "https://www.youtube.com/watch?v=gU5Coo1TeIk",
    "https://www.youtube.com/watch?v=slpr4PgCgcg",
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
