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
EMAIL_PASS = os.environ.get('EMAIL_PASS') # App Password
EMAIL_TO = os.environ.get('EMAIL_TO')

youtube = build('youtube', 'v3', developerKey=API_KEY)
report_lines = []

def log(message):
    print(message)
    report_lines.append(message)

# 1. Hàm kiểm tra trạng thái Link (Alive or Dead)
def check_link_status(url):
    try:
        # Giả lập trình duyệt để tránh bị chặn
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.head(url, headers=headers, allow_redirects=True, timeout=5)
        if response.status_code >= 400:
            return f"DEAD ({response.status_code})"
        return "OK"
    except Exception as e:
        return f"ERROR ({str(e)})"

# 2. Hàm lấy tất cả video của kênh (Tối ưu quota bằng cách dùng Playlist Uploads)
def get_all_videos(channel_id):
    videos = []
    # Lấy ID của playlist "Uploads"
    ch_response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    uploads_playlist_id = ch_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None
    while True:
        pl_request = youtube.playlistItems().list(
            playlistId=uploads_playlist_id,
            part='snippet,contentDetails',
            maxResults=50,
            pageToken=next_page_token
        )
        pl_response = pl_request.execute()
        
        for item in pl_response['items']:
            video_id = item['contentDetails']['videoId']
            title = item['snippet']['title']
            desc = item['snippet']['description']
            videos.append({'id': video_id, 'title': title, 'desc': desc})

        next_page_token = pl_response.get('nextPageToken')
        if not next_page_token:
            break
    return videos

# 3. Quét Description & Comments
def audit_text_links(video_id, text, source_type):
    urls = re.findall(r'(https?://\S+)', text)
    issues = []
    for url in urls:
        status = check_link_status(url)
        if "OK" not in status:
            issues.append(f"   [{source_type}] Link hỏng: {url} -> {status}")
    return issues

# 4. Quét Màn hình kết thúc & Thẻ (Scraping HTML)
def audit_end_screens(video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"
    issues = []
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(response.text, 'lxml')
        page_source = str(soup)

        # Logic: YouTube nhúng dữ liệu Endscreen trong biến JSON trong HTML
        # Chúng ta tìm các video ID được link trong source
        # Lưu ý: Đây là cách "lách" vì API không hỗ trợ, có thể thay đổi cấu trúc HTML
        
        # Tìm các link video khác (thường là màn hình kết thúc hoặc thẻ)
        # Pattern tìm ID video youtube: /watch?v=VIDEO_ID
        linked_video_ids = re.findall(r'/watch\?v=([a-zA-Z0-9_-]{11})', page_source)
        
        # Lọc trùng và bỏ qua chính video hiện tại
        unique_ids = set(linked_video_ids)
        if video_id in unique_ids:
            unique_ids.remove(video_id)

        # Kiểm tra xem các video được gắn thẻ/màn hình kết thúc có còn sống không
        # Chúng ta dùng API batch check cho nhanh
        if unique_ids:
            ids_list = list(unique_ids)[:50] # Check tối đa 50 video liên kết
            vid_request = youtube.videos().list(id=','.join(ids_list), part='status')
            vid_response = vid_request.execute()
            
            found_ids = [item['id'] for item in vid_response['items']]
            
            for linked_id in ids_list:
                if linked_id not in found_ids:
                    issues.append(f"   [EndScreen/Card] Video được gắn link đã bị XÓA/ẨN: {linked_id}")
                # Nếu cần kỹ hơn, check status.uploadStatus, status.privacyStatus
                
    except Exception as e:
        # Không báo lỗi quá chi tiết tránh spam log nếu mạng lỗi nhẹ
        pass
        
    return issues

# --- MAIN ---
def main():
    log("=== BẮT ĐẦU QUÉT KÊNH YOUTUBE ===")
    videos = get_all_videos(CHANNEL_ID)
    log(f"Tìm thấy tổng cộng: {len(videos)} video.")
    
    error_count = 0
    
    # Chia nhỏ hoặc chạy hết (Ở đây chạy hết vì GitHub cho phép 6 tiếng)
    for index, video in enumerate(videos):
        vid_id = video['id']
        log(f"Đang kiểm tra {index+1}/{len(videos)}: {video['title']}")
        
        vid_issues = []
        
        # 1. Check Description
        vid_issues.extend(audit_text_links(vid_id, video['desc'], "Mô tả"))
        
        # 2. Check End Screen / Cards (Scraping)
        vid_issues.extend(audit_end_screens(vid_id))
        
        # 3. Check Comments (Top 20 comment mới nhất)
        try:
            cmt_req = youtube.commentThreads().list(
                videoId=vid_id, part='snippet', maxResults=20, textFormat='plainText'
            )
            cmt_res = cmt_req.execute()
            for item in cmt_res.get('items', []):
                cmt_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                vid_issues.extend(audit_text_links(vid_id, cmt_text, "Bình luận"))
        except:
            pass # Có thể video tắt comment

        # Tổng hợp lỗi của video này
        if vid_issues:
            error_count += 1
            log(f"Phát hiện vấn đề tại video: https://youtu.be/{vid_id}")
            for issue in vid_issues:
                log(issue)
            log("-" * 20)

    # Gửi Email
    send_email_report(error_count)

def send_email_report(error_count):
    if error_count == 0:
        log("Không phát hiện lỗi nào. Không gửi email.")
        return

    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    msg['Subject'] = f"[BÁO CÁO] YouTube Audit: Phát hiện lỗi trong {error_count} video"
    
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
