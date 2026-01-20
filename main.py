import os
import re
import json
import requests
import smtplib
import time
import traceback
import concurrent.futures
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

# --- MODULE 1: CHECK LINK WEBSITE (ĐA LUỒNG) ---
def is_whitelist_domain(url):
    for domain in WHITELIST_DOMAINS:
        if domain in url: return True
    return False

def is_tracking_link(url):
    for kw in TRACKING_KEYWORDS:
        if kw in url: return True
    return False

def check_single_link(url):
    """Hàm kiểm tra 1 link cụ thể, dùng cho ThreadPool"""
    if any(d in url for d in ['youtube.com', 'youtu.be', 'google.com']): return None # Bỏ qua link nội bộ
    
    import random
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    try:
        # Giảm timeout xuống 10s để chạy nhanh hơn
        response = requests.get(url, headers=headers, timeout=10, verify=False, stream=True)
        
        # Chỉ lấy 5KB đầu tiên để check title, tiết kiệm băng thông
        content_snippet = ""
        try:
            for chunk in response.iter_content(chunk_size=1024):
                content_snippet += chunk.decode('utf-8', errors='ignore')
                if len(content_snippet) > 5000: break
        except: pass

        code = response.status_code
        has_title = '<title' in content_snippet.lower()

        if 200 <= code < 400: return None # Link tốt, return None
        
        # Xử lý Anti-Bot
        if code in [400, 403, 406, 429, 503, 999, 401]:
            if is_whitelist_domain(url) or has_title: return None
            return f"DEAD ({code} - Blocked/No Title) - {url}"
            
        if code in [404, 410]: return f"DEAD ({code} - Not Found) - {url}"
        return f"WARNING ({code}) - {url}"
        
    except requests.exceptions.RequestException:
        if is_tracking_link(url): return None
        return f"ERROR (Connection Failed) - {url}"

def audit_text_links_parallel(video_id, text, source_type):
    if not text: return []
    urls = re.findall(r'(https?://\S+)', text)
    cleaned_urls = list(set([u.rstrip('.,;)"\'') for u in urls]))
    
    if not cleaned_urls: return []

    issues = []
    # Sử dụng tối đa 5 luồng song song để check link -> Tăng tốc độ 5 lần
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(check_single_link, url): url for url in cleaned_urls}
        for future in concurrent.futures.as_completed(future_to_url):
            result = future.result()
            if result: # Nếu có lỗi trả về
                issues.append(f"   [{source_type}] {result}")
    
    return issues

# --- MODULE 2: QUÉT MÀN HÌNH KẾT THÚC (AN TOÀN HƠN) ---
def audit_end_screens_smart(video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"
    issues = []
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=10)
        html = response.text
        
        match = re.search(r'var ytInitialPlayerResponse\s*=\s*({.+?});', html)
        if not match: return []

        try:
            data = json.loads(match.group(1))
        except json.JSONDecodeError:
            return [] # Bỏ qua nếu lỗi parsing JSON (tránh crash)

        try:
            end_screen_elements = data['endscreen']['endScreenRenderer']['elements']
        except KeyError:
            return []

        for el in end_screen_elements:
            try:
                element_type = "Không rõ"
                target_id = None
                
                if 'endScreenVideoRenderer' in el:
                    renderer = el['endScreenVideoRenderer']
                    if 'videoId' in renderer:
                        target_id = renderer['videoId']
                        element_type = "Video cụ thể"
                    else:
                        continue 
                
                elif 'endScreenPlaylistRenderer' in el:
                    renderer = el['endScreenPlaylistRenderer']
                    if 'playlistId' in renderer:
                        target_id = renderer['playlistId']
                        element_type = "Danh sách phát"

                if target_id:
                    # Gọi API check xem video/playlist đích còn sống không
                    if element_type == "Video cụ thể":
                        check_vid = youtube.videos().list(id=target_id, part='status').execute()
                        if not check_vid['items']:
                            issues.append(f"   [Màn hình kết thúc] Video gắn BỊ XÓA: {target_id}")
                    
                    elif element_type == "Danh sách phát":
                        check_pl = youtube.playlists().list(id=target_id, part='status').execute()
                        if not check_pl['items']:
                            issues.append(f"   [Màn hình kết thúc] Playlist gắn BỊ XÓA: {target_id}")

            except Exception: continue

    except Exception: pass
    return issues

# --- MAIN ---
def get_long_videos(channel_id):
    long_videos = []
    try:
        ch_response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
        if not ch_response['items']: return []
        uploads_playlist_id = ch_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        next_page_token = None
        log("Đang tải danh sách video...")
        
        while True:
            pl_request = youtube.playlistItems().list(playlistId=uploads_playlist_id, part='contentDetails', maxResults=50, pageToken=next_page_token)
            pl_response = pl_request.execute()
            video_ids = [item['contentDetails']['videoId'] for item in pl_response['items']]
            
            if video_ids:
                # Gom nhóm 50 video để lấy duration (Batch Request)
                vid_request = youtube.videos().list(id=','.join(video_ids), part='snippet,contentDetails')
                vid_response = vid_request.execute()
                for item in vid_response['items']:
                    duration = parse_duration(item['contentDetails']['duration'])
                    if duration <= 125: continue # Lọc Shorts
                    
                    long_videos.append({'id': item['id'], 'title': item['snippet']['title'], 'desc': item['snippet']['description']})
            
            next_page_token = pl_response.get('nextPageToken')
            if not next_page_token: break
    except Exception as e:
        log(f"Lỗi khi lấy danh sách video: {e}")
        
    return long_videos

def send_email_report(error_count, crash_message=None):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    
    # --- LOGIC TIÊU ĐỀ EMAIL (ĐÃ CẢI TIẾN) ---
    if crash_message:
        msg['Subject'] = "[LỖI NGHIÊM TRỌNG] Script bị dừng đột ngột"
        body_content = f"Hệ thống gặp lỗi nghiêm trọng:\n\n{crash_message}\n\n=== LOG HOẠT ĐỘNG ===\n" + "\n".join(report_lines)
    
    elif error_count == 0:
        msg['Subject'] = f"[OK] Kênh Sạch - Đã quét xong (Log: {len(report_lines)} dòng)"
        body_content = "Hệ thống đã chạy xong và KHÔNG phát hiện lỗi nào.\nKênh của bạn đang ở trạng thái tốt.\n\n=== LOG HOẠT ĐỘNG ===\n" + "\n".join(report_lines)
        log("✅ Đang gửi email báo cáo (Kênh sạch)...")
    
    else:
        msg['Subject'] = f"[CẢNH BÁO] Có {error_count} vấn đề cần xử lý ngay"
        body_content = f"Tìm thấy {error_count} lỗi.\n\n=== CHI TIẾT LOG HOẠT ĐỘNG ===\n" + "\n".join(report_lines)
        log("⚠️ Đang gửi email cảnh báo lỗi...")

    msg.attach(MIMEText(body_content, 'plain'))
    
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
        server.quit()
        print(">> Đã gửi email thành công!")
    except Exception as e:
        print(f"Lỗi không gửi được email: {e}")

def main():
    log("=== BẮT ĐẦU QUÉT (PHIÊN BẢN TỐI ƯU GITHUB ACTIONS) ===")
    start_time = time.time()
    
    try:
        videos = get_long_videos(CHANNEL_ID)
        log(f"Tổng số video dài cần quét: {len(videos)} video.")
        error_count = 0
        
        for index, video in enumerate(videos):
            vid_id = video['id']
            log(f"[{index+1}/{len(videos)}] {video['title']}")
            vid_issues = []
            
            # 1. Check Link Mô tả (Đa luồng)
            vid_issues.extend(audit_text_links_parallel(vid_id, video['desc'], "Mô tả"))
            
            # 2. Check Màn hình kết thúc
            vid_issues.extend(audit_end_screens_smart(vid_id))
            
            # 3. Check Comment (Lấy 10 comment nổi bật nhất)
            try:
                cmt_req = youtube.commentThreads().list(videoId=vid_id, part='snippet', maxResults=10, order='relevance', textFormat='plainText')
                cmt_res = cmt_req.execute()
                for item in cmt_res.get('items', []):
                    cmt_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                    vid_issues.extend(audit_text_links_parallel(vid_id, cmt_text, "Bình luận"))
            except: pass

            if vid_issues:
                error_count += 1
                log(f"❌ CẢNH BÁO TẠI: https://youtu.be/{vid_id}")
                for issue in vid_issues: log(issue)
                log("-" * 20)
            
            # Nghỉ nhẹ 1s sau mỗi video để bảo vệ IP khỏi bị ban
            time.sleep(1)

        # Tổng kết thời gian
        elapsed = round(time.time() - start_time, 2)
        log(f"=== HOÀN TẤT TRONG {elapsed} GIÂY ===")
        
        # Gửi báo cáo kết quả (Dù không có lỗi cũng gửi)
        send_email_report(error_count)

    except Exception as e:
        # Bắt toàn bộ lỗi crash
        error_msg = traceback.format_exc()
        print("GẶP LỖI NGHIÊM TRỌNG:", error_msg)
        send_email_report(0, crash_message=error_msg)

if __name__ == "__main__":
    main()
