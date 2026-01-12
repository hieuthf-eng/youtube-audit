import os
import re
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from googleapiclient.discovery import build
from bs4 import BeautifulSoup

# --- CẤU HÌNH ---
API_KEY = os.environ.get('YOUTUBE_API_KEY')
CHANNEL_ID = os.environ.get('CHANNEL_ID')
EMAIL_USER = os.environ.get('EMAIL_USER')
EMAIL_PASS = os.environ.get('EMAIL_PASS')
EMAIL_TO = os.environ.get('EMAIL_TO')
PYTHONUNBUFFERED = os.environ.get('PYTHONUNBUFFERED')

# Tắt warning bảo mật cũ nếu có
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

youtube = build('youtube', 'v3', developerKey=API_KEY)
report_lines = []

def log(message):
    print(message, flush=True) # Flush để hiện log ngay lập tức
    report_lines.append(message)

# HÀM MỚI: Chuyển đổi thời lượng ISO 8601 (PT1M20S) sang giây
def parse_duration(duration_str):
    match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration_str)
    if not match:
        return 0
    
    hours = int(match.group(1)[:-1]) if match.group(1) else 0
    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
    seconds = int(match.group(3)[:-1]) if match.group(3) else 0
    
    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds

def check_link_status(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.head(url, headers=headers, allow_redirects=True, timeout=5)
        if response.status_code >= 400:
            return f"DEAD ({response.status_code})"
        return "OK"
    except Exception as e:
        return f"ERROR"

def get_long_videos(channel_id):
    long_videos = []
    
    # 1. Lấy playlist Uploads
    ch_response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    uploads_playlist_id = ch_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None
    log("Đang tải danh sách video...")
    
    while True:
        # Lấy danh sách ID video từ Playlist
        pl_request = youtube.playlistItems().list(
            playlistId=uploads_playlist_id,
            part='contentDetails',
            maxResults=50,
            pageToken=next_page_token
        )
        pl_response = pl_request.execute()
        
        video_ids = [item['contentDetails']['videoId'] for item in pl_response['items']]
        
        # 2. Gọi API lần 2 để lấy chi tiết Duration (Thời lượng)
        if video_ids:
            vid_request = youtube.videos().list(
                id=','.join(video_ids),
                part='snippet,contentDetails'
            )
            vid_response = vid_request.execute()
            
            for item in vid_response['items']:
                duration_str = item['contentDetails']['duration']
                seconds = parse_duration(duration_str)
                
                # --- LỌC VIDEO SHORT ---
                # Nếu video dưới 185 giây (cho dư 5s), coi là Short -> Bỏ qua
                if seconds <= 185:
                    continue 
                
                long_videos.append({
                    'id': item['id'],
                    'title': item['snippet']['title'],
                    'desc': item['snippet']['description']
                })

        next_page_token = pl_response.get('nextPageToken')
        if not next_page_token:
            break
            
    return long_videos

def audit_text_links(video_id, text, source_type):
    urls = re.findall(r'(https?://\S+)', text)
    issues = []
    for url in urls:
        # Bỏ qua các link nội bộ YouTube/Google để đỡ tốn thời gian
        if "youtube.com" in url or "youtu.be" in url or "google.com" in url:
            continue
            
        status = check_link_status(url)
        if "OK" not in status:
            issues.append(f"   [{source_type}] Link hỏng: {url} -> {status}")
    return issues

def audit_end_screens(video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"
    issues = []
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(response.text, 'lxml')
        page_source = str(soup)
        
        linked_video_ids = re.findall(r'/watch\?v=([a-zA-Z0-9_-]{11})', page_source)
        unique_ids = set(linked_video_ids)
        if video_id in unique_ids:
            unique_ids.remove(video_id)

        if unique_ids:
            ids_list = list(unique_ids)[:50]
            vid_request = youtube.videos().list(id=','.join(ids_list), part='status')
            vid_response = vid_request.execute()
            
            found_ids = [item['id'] for item in vid_response['items']]
            
            for linked_id in ids_list:
                if linked_id not in found_ids:
                    issues.append(f"   [Màn hình kết thúc] Video được gắn link ĐÃ CHẾT/ẨN: {linked_id}")
    except:
        pass
    return issues

def main():
    log("=== BẮT ĐẦU QUÉT (CHỈ QUÉT VIDEO DÀI) ===")
    
    # Lấy danh sách video (Đã lọc Shorts)
    videos = get_long_videos(CHANNEL_ID)
    log(f"Tổng số video dài (trên 60s) cần quét: {len(videos)} video.")
    
    error_count = 0
    
    for index, video in enumerate(videos):
        vid_id = video['id']
        log(f"[{index+1}/{len(videos)}] Kiểm tra: {video['title']}")
        
        vid_issues = []
        
        # 1. Check Mô tả
        vid_issues.extend(audit_text_links(vid_id, video['desc'], "Mô tả"))
        
        # 2. Check End Screen (Màn hình kết thúc)
        vid_issues.extend(audit_end_screens(vid_id))
        
        # 3. Check Comment (Chỉ check comment ghim hoặc mới nhất)
        try:
            cmt_req = youtube.commentThreads().list(
                videoId=vid_id, part='snippet', maxResults=5, order='relevance', textFormat='plainText'
            )
            cmt_res = cmt_req.execute()
            for item in cmt_res.get('items', []):
                cmt_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                vid_issues.extend(audit_text_links(vid_id, cmt_text, "Bình luận"))
        except:
            pass

        if vid_issues:
            error_count += 1
            log(f"❌ CẢNH BÁO TẠI: https://youtu.be/{vid_id}")
            for issue in vid_issues:
                log(issue)
            log("-" * 20)

    send_email_report(error_count)

def send_email_report(error_count):
    if error_count == 0:
        log("✅ Không có lỗi nào. Kết thúc.")
        return

    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    msg['Subject'] = f"[CẢNH BÁO] Có {error_count} video cần sửa lỗi Link/Màn hình kết thúc"
    
    body = "\n".join(report_lines)
    msg.attach(MIMEText(body, 'plain'))
    
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
        server.quit()
        print("Đã gửi email báo cáo.")
    except Exception as e:
        print(f"Lỗi gửi email: {e}")

if __name__ == "__main__":
    main()
