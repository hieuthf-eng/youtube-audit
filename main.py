import os
import re
import json
import requests
import smtplib
import warnings
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from googleapiclient.discovery import build

# --- CẤU HÌNH ---
API_KEY = os.environ.get('YOUTUBE_API_KEY')
CHANNEL_ID = os.environ.get('CHANNEL_ID')
EMAIL_USER = os.environ.get('EMAIL_USER')
EMAIL_PASS = os.environ.get('EMAIL_PASS')
EMAIL_TO = os.environ.get('EMAIL_TO')
PYTHONUNBUFFERED = os.environ.get('PYTHONUNBUFFERED')

# Tắt cảnh báo SSL
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

youtube = build('youtube', 'v3', developerKey=API_KEY)
report_lines = []

# --- CẤU HÌNH LOGIC CHECK LINK ---
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]
WHITELIST_DOMAINS = ['facebook.com', 'fb.me', 'twitter.com', 'x.com', 'linkedin.com', 'instagram.com', 'tiktok.com', 't.me', 'zalo.me', 'youtube.com', 'youtu.be', 'google.com']
TRACKING_KEYWORDS = ['pipaffiliates', 'affiliate', 'clicks.', 'track.', 'go.', 'bit.ly', 'tinyurl', 'ref=', 'click', 'partner', 'redirect']

def log(message):
    print(message, flush=True)
    report_lines.append(message)

def parse_duration(duration_str):
    match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration_str)
    if not match: return 0
    hours = int(match.group(1)[:-1]) if match.group(1) else 0
    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
    seconds = int(match.group(3)[:-1]) if match.group(3) else 0
    return (hours * 3600) + (minutes * 60) + seconds

# --- MODULE 1: CHECK LINK WEBSITE ---
def is_whitelist_domain(url):
    for domain in WHITELIST_DOMAINS:
        if domain in url: return True
    return False

def is_tracking_link(url):
    for kw in TRACKING_KEYWORDS:
        if kw in url: return True
    return False

def check_link_status_advanced(url):
    import random
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    try:
        response = requests.get(url, headers=headers, timeout=15, verify=False, stream=True)
        content_snippet = ""
        try:
            for chunk in response.iter_content(chunk_size=1024):
                content_snippet += chunk.decode('utf-8', errors='ignore')
                if len(content_snippet) > 10000: break
        except: pass

        code = response.status_code
        has_title = '<title' in content_snippet.lower()

        if 200 <= code < 400: return "OK"
        if code in [400, 403, 406, 429, 503, 999, 401]:
            if is_whitelist_domain(url) or has_title: return "OK"
            return f"DEAD ({code} - Blocked & No Title)"
        if code in [404, 410]: return f"DEAD ({code} - Not Found)"
        return f"WARNING ({code})"
    except requests.exceptions.RequestException:
        if is_tracking_link(url): return "OK"
        return f"ERROR (Connection Failed)"

# --- MODULE 2: QUÉT MÀN HÌNH KẾT THÚC (SỬA LẠI HOÀN TOÀN) ---
def audit_end_screens_smart(video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"
    issues = []
    
    try:
        # 1. Tải HTML
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=10)
        html = response.text
        
        # 2. Tìm JSON data (ytInitialPlayerResponse)
        # Đây là nơi chứa cấu hình chính xác của End Screen
        match = re.search(r'var ytInitialPlayerResponse\s*=\s*({.+?});', html)
        if not match:
            return [] # Không tìm thấy dữ liệu, bỏ qua (có thể video không có Endscreen)

        data = json.loads(match.group(1))
        
        # 3. Truy cập vào EndScreen Renderer
        try:
            end_screen_elements = data['endscreen']['endScreenRenderer']['elements']
        except KeyError:
            return [] # Video này không bật Màn hình kết thúc

        # 4. Duyệt qua từng ô thành phần
        for el in end_screen_elements:
            try:
                # Lấy thông tin cơ bản
                element_type = "Không rõ"
                target_id = None
                
                # Trường hợp: Video
                if 'endScreenVideoRenderer' in el:
                    renderer = el['endScreenVideoRenderer']
                    # Check xem là video cụ thể hay tự động
                    if 'videoId' in renderer:
                        target_id = renderer['videoId']
                        element_type = "Video cụ thể"
                    else:
                        # Video "Phù hợp nhất" hoặc "Gần đây nhất" thường không có videoId cứng ở đây
                        element_type = "Video tự động (Phù hợp nhất/Mới nhất)"
                        continue # Bỏ qua, không check vì nó là thuật toán Youtube
                
                # Trường hợp: Playlist
                elif 'endScreenPlaylistRenderer' in el:
                    renderer = el['endScreenPlaylistRenderer']
                    if 'playlistId' in renderer:
                        target_id = renderer['playlistId']
                        element_type = "Danh sách phát"

                # Nếu tìm thấy ID cụ thể -> Kiểm tra xem nó còn sống không
                if target_id:
                    # Kiểm tra ID này bằng API
                    if element_type == "Video cụ thể":
                        check_vid = youtube.videos().list(id=target_id, part='status').execute()
                        if not check_vid['items']:
                             issues.append(f"   [Màn hình kết thúc] Video được gắn đã BỊ XÓA/ẨN: {target_id}")
                    
                    elif element_type == "Danh sách phát":
                        check_pl = youtube.playlists().list(id=target_id, part='status').execute()
                        if not check_pl['items']:
                             issues.append(f"   [Màn hình kết thúc] Playlist được gắn đã BỊ XÓA/ẨN: {target_id}")

            except Exception as e:
                continue # Bỏ qua lỗi nhỏ trong parsing element

    except Exception as e:
        pass # Lỗi mạng hoặc parsing chung
        
    return issues

# --- MAIN ---
def get_long_videos(channel_id):
    long_videos = []
    ch_response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    uploads_playlist_id = ch_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    log("Đang tải danh sách video...")
    
    while True:
        pl_request = youtube.playlistItems().list(playlistId=uploads_playlist_id, part='contentDetails', maxResults=50, pageToken=next_page_token)
        pl_response = pl_request.execute()
        video_ids = [item['contentDetails']['videoId'] for item in pl_response['items']]
        
        if video_ids:
            vid_request = youtube.videos().list(id=','.join(video_ids), part='snippet,contentDetails')
            vid_response = vid_request.execute()
            for item in vid_response['items']:
                duration = parse_duration(item['contentDetails']['duration'])
                if duration <= 125: continue # Lọc Shorts
                
                long_videos.append({'id': item['id'], 'title': item['snippet']['title'], 'desc': item['snippet']['description']})
        
        next_page_token = pl_response.get('nextPageToken')
        if not next_page_token: break
    return long_videos

def audit_text_links(video_id, text, source_type):
    if not text: return []
    urls = re.findall(r'(https?://\S+)', text)
    issues = []
    cleaned_urls = list(set([u.rstrip('.,;)"\'') for u in urls]))

    for url in cleaned_urls:
        if any(d in url for d in ['youtube.com', 'youtu.be', 'google.com']): continue
        status = check_link_status_advanced(url)
        if "OK" not in status:
            issues.append(f"   [{source_type}] Link hỏng: {url} -> {status}")
    return issues

def main():
    log("=== BẮT ĐẦU QUÉT (LINK & SMART END-SCREEN) ===")
    videos = get_long_videos(CHANNEL_ID)
    log(f"Tổng số video dài (trên 2 phút) cần quét: {len(videos)} video.")
    error_count = 0
    
    for index, video in enumerate(videos):
        vid_id = video['id']
        log(f"[{index+1}/{len(videos)}] {video['title']}")
        vid_issues = []
        
        # 1. Check Link Mô tả
        vid_issues.extend(audit_text_links(vid_id, video['desc'], "Mô tả"))
        
        # 2. Check Màn hình kết thúc (Mới)
        vid_issues.extend(audit_end_screens_smart(vid_id))
        
        # 3. Check Comment
        try:
            cmt_req = youtube.commentThreads().list(videoId=vid_id, part='snippet', maxResults=10, order='relevance', textFormat='plainText')
            cmt_res = cmt_req.execute()
            for item in cmt_res.get('items', []):
                cmt_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                vid_issues.extend(audit_text_links(vid_id, cmt_text, "Bình luận"))
        except: pass

        if vid_issues:
            error_count += 1
            log(f"❌ CẢNH BÁO TẠI: https://youtu.be/{vid_id}")
            for issue in vid_issues: log(issue)
            log("-" * 20)

    send_email_report(error_count)

def send_email_report(error_count):
    if error_count == 0:
        log("✅ Không có lỗi nào. Kết thúc.")
        return
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    msg['Subject'] = f"[CẢNH BÁO] Có {error_count} video cần xử lý"
    body = "\n".join(report_lines)
    msg.attach(MIMEText(body, 'plain'))
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
        server.quit()
        print("Đã gửi email báo cáo.")
    except Exception as e: print(f"Lỗi gửi email: {e}")

if __name__ == "__main__":
    main()
