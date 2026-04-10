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

youtube = build('youtube', 'v3', developerKey=API_KEY)

# --- KHAI BÁO BIẾN DỮ LIỆU ---
CSV_DATA = []
CSV_HEADER = ['STT', 'Tiêu đề Video', 'URL Video', 'Thời lượng (giây)']

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
        # Lấy Playlist ID của các video đã tải lên
        ch_response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
        if not ch_response['items']: return []
        uploads_playlist_id = ch_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        next_page_token = None
        log("Đang quét danh sách video từ YouTube...")
        
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
                vid_request = youtube.videos().list(id=','.join(video_ids), part='snippet,contentDetails')
                vid_response = vid_request.execute()
                for item in vid_response['items']:
                    duration = parse_duration(item['contentDetails']['duration'])
                    # LỌC VIDEO DÀI (> 125 giây)
                    if duration > 125:
                        videos_list.append({
                            'title': item['snippet']['title'],
                            'url': f"https://www.youtube.com/watch?v={item['id']}",
                            'duration': duration
                        })
            
            next_page_token = pl_response.get('nextPageToken')
            if not next_page_token: break
            
        return videos_list
    except Exception as e:
        log(f"Lỗi: {e}")
        return []

def send_email_with_csv(channel_name, video_count):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    msg['Subject'] = f"[{channel_name}] Danh sách {video_count} video dài"

    # Tạo file CSV trong bộ nhớ
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(CSV_HEADER)
    csv_writer.writerows(CSV_DATA)
    csv_bytes = csv_buffer.getvalue().encode('utf-8-sig')
    
    filename = f"Danh_sach_video_{channel_name.replace(' ', '_')}.csv"
    attachment = MIMEApplication(csv_bytes, Name=filename)
    attachment['Content-Disposition'] = f'attachment; filename="{filename}"'
    msg.attach(attachment)

    body = f"Gửi bạn danh sách tất cả các video dài (>125s) của kênh {channel_name}.\nTổng cộng: {video_count} video."
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
        server.quit()
        log(">> Đã gửi email danh sách video!")
    except Exception as e:
        log(f"Lỗi gửi email: {e}")

def main():
    log("=== KHỞI CHẠY LẤY DANH SÁCH VIDEO ===")
    start_time = time.time()
    
    # Lấy tên kênh
    channel_name = "Unknown Channel"
    try:
        ch_info = youtube.channels().list(id=CHANNEL_ID, part='snippet').execute()
        channel_name = ch_info['items'][0]['snippet']['title']
    except: pass

    videos = get_all_long_videos(CHANNEL_ID)
    
    for index, v in enumerate(videos):
        CSV_DATA.append([index + 1, v['title'], v['url'], v['duration']])

    if CSV_DATA:
        send_email_with_csv(channel_name, len(videos))
    else:
        log("Không tìm thấy video nào phù hợp.")

    elapsed = round(time.time() - start_time, 2)
    log(f"=== HOÀN TẤT TRONG {elapsed} GIÂY ===")

if __name__ == "__main__":
    main()
