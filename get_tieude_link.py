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
CHANNEL_ID = "UCH7t29p04-lX8_wawKlZsfA"
EMAIL_USER = os.environ.get('EMAIL_USER')
EMAIL_PASS = os.environ.get('EMAIL_PASS')
EMAIL_TO = os.environ.get('EMAIL_TO')

youtube = build('youtube', 'v3', developerKey=API_KEY)

# --- CẤU TRÚC CSV CẬP NHẬT MỚI (ĐÃ TÁCH NGÀY/GIỜ & ĐỔI PHÚT GIÂY) ---
CSV_DATA = []
CSV_HEADER = [
    'STT', 
    'Tiêu đề Video', 
    'URL Video', 
    'Thời lượng',       # Sẽ hiển thị dạng "X phút Y giây"
    'Ngày đăng',        # Tách riêng biệt
    'Giờ đăng',         # Tách riêng biệt
    'Thẻ Tags', 
    'Lượt xem', 
    'Lượt thích', 
    'Lượt bình luận'
]

def log(message):
    print(message, flush=True)

def parse_duration(duration_str):
    """Đổi định dạng ISO 8601 của YouTube sang tổng số giây"""
    match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration_str)
    if not match: return 0
    hours = int(match.group(1)[:-1]) if match.group(1) else 0
    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
    seconds = int(match.group(3)[:-1]) if match.group(3) else 0
    return (hours * 3600) + (minutes * 60) + seconds

def format_duration_vietnamese(total_seconds):
    """Chuyển đổi tổng số giây sang định dạng 'X phút Y giây'"""
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    if minutes > 0:
        return f"{minutes} phút {seconds} giây"
    return f"{seconds} giây"

def parse_datetime(iso_date_str):
    """
    Tách chuỗi định dạng '2026-05-19T10:19:07Z' thành (Ngày, Giờ).
    Nếu lỗi hoặc trống, trả về giá trị mặc định.
    """
    if not iso_date_str:
        return "N/A", "N/A"
    try:
        # Sử dụng Regex tách đoạn trước và sau ký tự 'T'
        match = re.match(r'(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})', iso_date_str)
        if match:
            return match.group(1), match.group(2)
    except Exception:
        pass
    return iso_date_str, ""

def get_all_long_videos(channel_id):
    videos_list = []
    try:
        ch_response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
        if not ch_response['items']: 
            log("Không tìm thấy thông tin kênh.")
            return []
        uploads_playlist_id = ch_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        next_page_token = None
        log("Đang quét danh sách video và định dạng lại cấu trúc dữ liệu...")
        
        while True:
            pl_request = youtube.playlistItems().list(
                playlistId=uploads_playlist_id, 
                part='contentDetails', 
                maxResults=50, 
                pageToken=next_page_token
            )
            pl_response = pl_request.execute()
            video_ids = [item['contentDetails']['videoId'] for item in pl_response['items']]
            
            if video_ids:
                vid_request = youtube.videos().list(
                    id=','.join(video_ids), 
                    part='snippet,contentDetails,statistics'
                )
                vid_response = vid_request.execute()
                
                for item in vid_response['items']:
                    raw_duration = parse_duration(item['contentDetails']['duration'])
                    
                    # LỌC VIDEO DÀI (> 125 giây)
                    if raw_duration > 125:
                        snippet = item['snippet']
                        stats = item.get('statistics', {})
                        
                        # 1. Đổi thời lượng sang: X phút Y giây
                        friendly_duration = format_duration_vietnamese(raw_duration)
                        
                        # 2. Tách chuỗi Ngày xuất bản ra làm 2 phần
                        published_date, published_time = parse_datetime(snippet.get('publishedAt', ''))
                        
                        tags_list = snippet.get('tags', [])
                        tags_str = ", ".join(tags_list) if tags_list else ""
                        
                        videos_list.append({
                            'title': snippet['title'],
                            'url': f"https://www.youtube.com/watch?v={item['id']}",
                            'duration': friendly_duration,
                            'date': published_date,
                            'time': published_time,
                            'tags': tags_str,
                            'views': int(stats.get('viewCount', 0)),
                            'likes': int(stats.get('likeCount', 0)),
                            'comments': int(stats.get('commentCount', 0))
                        })
            
            next_page_token = pl_response.get('nextPageToken')
            if not next_page_token: 
                break
            
        return videos_list
    except Exception as e:
        log(f"Lỗi khi lấy dữ liệu: {e}")
        return []

def send_email_with_csv(channel_name, video_count):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    msg['Subject'] = f"[{channel_name}] Báo cáo tối ưu {video_count} video công khai"

    # Lưu dữ liệu với cấu trúc CSV mới
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(CSV_HEADER)
    csv_writer.writerows(CSV_DATA)
    csv_bytes = csv_buffer.getvalue().encode('utf-8-sig')
    
    filename = f"Du_lieu_video_toi_uu_{channel_name.replace(' ', '_')}.csv"
    attachment = MIMEApplication(csv_bytes, Name=filename)
    attachment['Content-Disposition'] = f'attachment; filename="{filename}"'
    msg.attach(attachment)

    body = (f"Chào bạn,\n\nGửi bạn file báo cáo đã được tối ưu hiển thị theo yêu cầu:\n"
            f"- Cột Thời lượng đã được chuyển thành định dạng dễ đọc (X phút Y giây).\n"
            f"- Khung thời gian xuất bản đã được tách biệt hẳn thành 2 cột: 'Ngày đăng' và 'Giờ đăng' để tiện phân tích.\n\n"
            f"Tổng số lượng video lọc được: {video_count} video.")
    
    msg.attach(MIMEText(body, 'plain', 'utf-8'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
        server.quit()
        log(">> Đã gửi email báo cáo định dạng mới thành công!")
    except Exception as e:
        log(f"Lỗi gửi email: {e}")

def main():
    log("=== KHỞI CHẠY QUÉT VÀ ĐỊNH DẠNG LẠI DỮ LIỆU ===")
    start_time = time.time()
    
    channel_name = "Unknown Channel"
    try:
        ch_info = youtube.channels().list(id=CHANNEL_ID, part='snippet').execute()
        channel_name = ch_info['items'][0]['snippet']['title']
    except: 
        pass

    videos = get_all_long_videos(CHANNEL_ID)
    
    for index, v in enumerate(videos):
        CSV_DATA.append([
            index + 1, 
            v['title'], 
            v['url'], 
            v['duration'],
            v['date'],
            v['time'],
            v['tags'],
            v['views'],
            v['likes'],
            v['comments']
        ])

    if CSV_DATA:
        send_email_with_csv(channel_name, len(videos))
    else:
        log("Không tìm thấy dữ liệu video phù hợp.")

    elapsed = round(time.time() - start_time, 2)
    log(f"=== HOÀN TẤT TRONG {elapsed} GIÂY ===")

if __name__ == "__main__":
    main()
