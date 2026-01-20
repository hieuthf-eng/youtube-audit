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

# --- BIẾN TOÀN CỤC ĐỂ THỐNG KÊ ---
STATS = {
    "videos_scanned": 0,
    "total_links_found": 0,
    "links_ok": 0,
    "links_error": 0,
    "endscreen_issues": 0
}

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
    if any(d in url for d in ['youtube.com', 'youtu.be', 'google.com']): return "INTERNAL" # Link nội bộ, không tính là outlink check
    
    import random
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10, verify=False, stream=True)
        content_snippet = ""
        try:
            for chunk in response.iter_content(chunk_size=1024):
                content_snippet += chunk.decode('utf-8', errors='ignore')
                if len(content_snippet) > 5000: break
        except: pass

        code = response.status_code
        has_title = '<title' in content_snippet.lower()

        if 200 <= code < 400: return None # OK
        
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
    
    # Lọc bỏ link youtube nội bộ để đếm cho chính xác số outlink thực tế
    external_links = [u for u in cleaned_urls if not any(d in u for d in ['youtube.com', 'youtu.be', 'google.com'])]

    if not external_links: 
        return []

    # LOG THÔNG BÁO CHO NGƯỜI DÙNG BIẾT LÀ CÓ LINK
    log(f"   > [{source_type}] Phát hiện {len(external_links)} link ngoài. Đang kiểm tra...")
    STATS["total_links_found"] += len(external_links)

    issues = []
    # Sử dụng tối đa 5 luồng song song
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(check_single_link, url): url for url in external_links}
        for future in concurrent.futures.as_completed(future_to_url):
            result = future.result()
            if result == "INTERNAL":
                continue
            elif result: # Có lỗi
                issues.append(f"   [{source_type}] {result}")
                STATS["links_error"] += 1
            else: # Không có lỗi (None)
                STATS["links_ok"] += 1
    
    return issues

# --- MODULE 2: QUÉT MÀN HÌNH KẾT THÚC ---
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
            end_screen_elements = data['endscreen']['endScreenRenderer']['elements']
        except (KeyError, json.JSONDecodeError, AttributeError):
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
                    else: continue 
                
                elif 'endScreenPlaylistRenderer' in el:
                    renderer = el['endScreenPlaylistRenderer']
                    if 'playlistId' in renderer:
                        target_id = renderer['playlistId']
                        element_type = "Danh sách phát"

                if target_id:
                    # Gọi API check
                    if element_type == "Video cụ thể":
                        check_vid = youtube.videos().list(id=target_id, part='status').execute()
                        if not check_vid['items']:
                            issues.append(f"   [Màn hình kết thúc] Video gắn BỊ XÓA: {target_id}")
                            STATS["endscreen_issues"] += 1
                    
                    elif element_type == "Danh sách phát":
                        check_pl = youtube.playlists().list(id=target_id, part='status').execute()
                        if not check_pl['items']:
                            issues.append(f"   [Màn hình kết thúc] Playlist gắn BỊ XÓA: {target_id}")
                            STATS["endscreen_issues"] += 1

            except Exception: continue

    except Exception: pass
    return issues

# --- MODULE 0: QUÉT THÔNG TIN KÊNH ---
def audit_channel_info(channel_id):
    log(">> Đang kiểm tra thông tin Kênh (About Section)...")
    issues = []
    try:
        response = youtube.channels().list(id=channel_id, part='snippet').execute()
        if not response['items']: return []
        item = response['items'][0]
        description = item['snippet']['description']
        issues.extend(audit_text_links_parallel("CHANNEL_HOME", description, "Mô tả Kênh"))
    except Exception as e:
        log(f"Lỗi khi quét kênh: {e}")
    return issues

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

def send_email_report(total_issues, channel_name, crash_message=None):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    
    # --- TẠO BẢNG THỐNG KÊ (DASHBOARD) ---
    summary_block = (
        f"=== THỐNG KÊ TỔNG QUAN KÊNH {channel_name} ===\n"
        f"- Tổng video đã quét: {STATS['videos_scanned']}\n"
        f"- Tổng Outlink tìm thấy: {STATS['total_links_found']}\n"
        f"- Link hoạt động tốt (OK): {STATS['links_ok']}\n"
        f"- Link lỗi/Chết (Error): {STATS['links_error']}\n"
        f"- Lỗi màn hình kết thúc: {STATS['endscreen_issues']}\n"
        f"======================================\n\n"
    )

    if crash_message:
        msg['Subject'] = f"[{channel_name}] ❌ LỖI NGHIÊM TRỌNG - Script dừng đột ngột"
        body_content = f"Hệ thống gặp lỗi nghiêm trọng:\n{crash_message}\n\n{summary_block}=== LOG HOẠT ĐỘNG ===\n" + "\n".join(report_lines)
    
    elif total_issues == 0:
        msg['Subject'] = f"[{channel_name}] ✅ Kênh Sạch ({STATS['total_links_found']} links OK)"
        body_content = f"{summary_block}Hệ thống đã chạy xong và KHÔNG phát hiện lỗi nào.\n\n=== LOG HOẠT ĐỘNG ===\n" + "\n".join(report_lines)
        log("✅ Đang gửi email báo cáo (Kênh sạch)...")
    
    else:
        msg['Subject'] = f"[{channel_name}] ⚠️ CẢNH BÁO - {total_issues} lỗi cần xử lý"
        body_content = f"{summary_block}Chi tiết các lỗi được liệt kê bên dưới:\n\n=== CHI TIẾT LOG HOẠT ĐỘNG ===\n" + "\n".join(report_lines)
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
    log("=== BẮT ĐẦU QUÉT (FULL REPORT) ===")
    start_time = time.time()
    total_issues_count = 0
    current_channel_name = "Unknown Channel"
    
    try:
        # LẤY TÊN KÊNH
        try:
            ch_info = youtube.channels().list(id=CHANNEL_ID, part='snippet').execute()
            if ch_info['items']:
                current_channel_name = ch_info['items'][0]['snippet']['title']
                log(f"Kênh mục tiêu: {current_channel_name}")
        except Exception: pass

        # BƯỚC 1: QUÉT KÊNH
        channel_issues = audit_channel_info(CHANNEL_ID)
        if channel_issues:
            total_issues_count += 1
            log(f"❌ CẢNH BÁO TẠI THÔNG TIN KÊNH:")
            for issue in channel_issues: log(issue)
            log("-" * 20)

        # BƯỚC 2: QUÉT VIDEO
        videos = get_long_videos(CHANNEL_ID)
        STATS['videos_scanned'] = len(videos)
        log(f"Tổng số video dài: {len(videos)}")
        
        for index, video in enumerate(videos):
            vid_id = video['id']
            log(f"[{index+1}/{len(videos)}] {video['title']}")
            vid_issues = []
            
            # Check Link
            vid_issues.extend(audit_text_links_parallel(vid_id, video['desc'], "Mô tả"))
            
            # Check End Screen
            es_issues = audit_end_screens_smart(vid_id)
            vid_issues.extend(es_issues)
            
            # Check Comment
            try:
                cmt_req = youtube.commentThreads().list(videoId=vid_id, part='snippet', maxResults=10, order='relevance', textFormat='plainText')
                cmt_res = cmt_req.execute()
                for item in cmt_res.get('items', []):
                    cmt_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                    vid_issues.extend(audit_text_links_parallel(vid_id, cmt_text, "Bình luận"))
            except: pass

            if vid_issues:
                total_issues_count += 1
                log(f"❌ CẢNH BÁO TẠI: https://youtu.be/{vid_id}")
                for issue in vid_issues: log(issue)
                log("-" * 20)
            
            time.sleep(1)

        elapsed = round(time.time() - start_time, 2)
        log(f"=== HOÀN TẤT TRONG {elapsed} GIÂY ===")
        
        send_email_report(total_issues_count, current_channel_name)

    except Exception as e:
        error_msg = traceback.format_exc()
        print("GẶP LỖI NGHIÊM TRỌNG:", error_msg)
        send_email_report(0, current_channel_name, crash_message=error_msg)

if __name__ == "__main__":
    main()
