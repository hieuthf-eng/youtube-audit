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
CHANNEL_ID = os.environ.get('CHANNEL_ID')
EMAIL_USER = os.environ.get('EMAIL_USER')
EMAIL_PASS = os.environ.get('EMAIL_PASS')
EMAIL_TO = os.environ.get('EMAIL_TO')

# Khởi tạo YouTube Data API v3 bằng API_KEY (Hoàn toàn công khai, không lo lỗi OAuth2)
youtube = build('youtube', 'v3', developerKey=API_KEY)

# --- KHAI BÁO BIẾN DỮ LIỆU ---
CSV_DATA = []
CSV_HEADER = [
    'STT', 
    'Tiêu đề Video', 
    'URL Video', 
    'Thời lượng (giây)', 
    'Ngày xuất bản', 
    'Thẻ Tags', 
    'Lượt xem', 
    'Lượt thích', 
    'Lượt bình luận'
]

def log(message):
    print(message, flush=True)

def parse_duration(duration_str):
    match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration_str)
    if not match: return 0
    hours = int(match.group(1)[:-1]) if match.group(1) else 0
    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
    seconds = int(match.group(3)[:-1]) if match.group(3) else 0
    return (hours * 3600) + (minutes * 60) + seconds

def get_all_long_videos(channel_id):
    videos_list = []
    try:
        # Lấy Playlist ID của các video đã tải lên từ contentDetails
        ch_response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
        if not ch_response['items']: 
            log("Không tìm thấy thông tin kênh với ID đã cung cấp.")
            return []
        uploads_playlist_id = ch_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        next_page_token = None
        log("Đang quét danh sách video và thu thập dữ liệu công khai từ YouTube...")
        
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
                # Gọi API videos().list để lấy chi tiết snippet, contentDetails và statistics công khai
                vid_request = youtube.videos().list(
                    id=','.join(video_ids), 
                    part='snippet,contentDetails,statistics'
                )
                vid_response = vid_request.execute()
                
                for item in vid_response['items']:
                    duration = parse_duration(item['contentDetails']['duration'])
                    
                    # LỌC VIDEO DÀI (> 125 giây)
                    if duration > 125:
                        snippet = item['snippet']
                        stats = item.get('statistics', {})
                        
                        # Chuyển mảng tags thành chuỗi text phân cách bằng dấu phẩy
                        tags_list = snippet.get('tags', [])
                        tags_str = ", ".join(tags_list) if tags_list else ""
                        
                        videos_list.append({
                            'title': snippet['title'],
                            'url': f"https://www.youtube.com/watch?v={item['id']}",
                            'duration': duration,
                            'published_at': snippet.get('publishedAt', ''),
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
        log(f"Lỗi khi lấy dữ liệu từ YouTube: {e}")
        return []

def send_email_with_csv(channel_name, video_count):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    msg['Subject'] = f"[{channel_name}] Báo cáo dữ liệu {video_count} video công khai"

    # Tạo file CSV với mã hóa utf-8-sig để tránh lỗi font tiếng Việt khi mở trực tiếp trên Excel
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(CSV_HEADER)
    csv_writer.writerows(CSV_DATA)
    csv_bytes = csv_buffer.getvalue().encode('utf-8-sig')
    
    filename = f"Du_lieu_video_cong_khai_{channel_name.replace(' ', '_')}.csv"
    attachment = MIMEApplication(csv_bytes, Name=filename)
    attachment['Content-Disposition'] = f'attachment; filename="{filename}"'
    msg.attach(attachment)

    body = (f"Chào bạn,\n\nGửi bạn tệp báo cáo tổng hợp dữ liệu công khai của các video dài (>125s) "
            f"trên kênh {channel_name}.\n"
            f"Tổng cộng tìm thấy: {video_count} video.\n\n"
            f"Báo cáo bao gồm: Tiêu đề, URL, Thời lượng, Ngày đăng, Thẻ Tags, Lượt xem, Lượt thích và Bình luận.")
    
    msg.attach(MIMEText(body, 'plain', 'utf-8'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
        server.quit()
        log(">> Đã gửi email báo cáo thành công!")
    except Exception as e:
        log(f"Lỗi gửi email: {e}")

def main():
    log("=== KHỞI CHẠY QUÉT DỮ LIỆU VIDEO CÔNG KHAI ===")
    start_time = time.time()
    
    # Lấy tên chính xác của kênh
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
            v['published_at'],
            v['tags'],
            v['views'],
            v['likes'],
            v['comments']
        ])

    if CSV_DATA:
        send_email_with_csv(channel_name, len(videos))
    else:
        log("Không tìm thấy video nào thỏa mãn điều kiện.")

    elapsed = round(time.time() - start_time, 2)
    log(f"=== HOÀN TẤT TRONG {elapsed} GIÂY ===")

if __name__ == "__main__":
    main()
