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
from email.mime.application import MIMEApplication  # <--- Thêm mới để xử lý file đính kèm
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

# --- KHAI BÁO BIẾN LOG ---
full_log_lines = []       # Lưu TẤT CẢ (để ghi ra file .txt đính kèm)
email_error_lines = []    # Chỉ lưu LỖI (để hiện trong nội dung email)

# --- BIẾN TOÀN CỤC ĐỂ THỐNG KÊ ---
STATS = {
    "videos_scanned": 0,
    "total_occurrences": 0,
    "unique_links_count": 0,
    "links_error": 0,
    "endscreen_issues": 0
}

# CACHE: Lưu kết quả đã check
LINK_CACHE = {} 
UNIQUE_LINKS_SET = set() 

# --- CẤU HÌNH LOGIC CHECK LINK ---
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]
WHITELIST_DOMAINS = ['facebook.com', 'fb.me', 'twitter.com', 'x.com', 'linkedin.com', 'instagram.com', 'tiktok.com', 't.me', 'zalo.me', 'youtube.com', 'youtu.be', 'google.com']
TRACKING_KEYWORDS = ['pipaffiliates', 'affiliate', 'clicks.', 'track.', 'go.', 'bit.ly', 'tinyurl', 'ref=', 'click', 'partner', 'redirect']

def log(message, is_error=False, is_summary=False):
    """
    Hàm log thông minh:
    - Luôn in ra màn hình (Console)
    - Luôn lưu vào full_log_lines (File đính kèm)
    - Chỉ lưu vào email_error_lines nếu đó là Lỗi hoặc Tổng kết
    """
    print(message, flush=True)
    full_log_lines.append(message)
    
    if is_error or is_summary:
        email_error_lines.append(message)

def parse_duration(duration_str):
    match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration_str)
    if not match: return 0
    hours = int(match.group(1)[:-1]) if match.group(1) else 0
    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
    seconds = int(match.group(3)[:-1]) if match.group(3) else 0
    return (hours * 3600) + (minutes * 60) + seconds

# --- MODULE 1: CHECK LINK WEBSITE (CÓ CACHE) ---
def is_whitelist_domain(url):
    for domain in WHITELIST_DOMAINS:
        if domain in url: return True
    return False

def is_tracking_link(url):
    for kw in TRACKING_KEYWORDS:
        if kw in url: return True
    return False

def check_single_link(url):
    if any(d in url for d in ['youtube.com', 'youtu.be', 'google.com']): return "INTERNAL"
    
    if url in LINK_CACHE: return LINK_CACHE[url]

    import random
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    result = None 
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

        if 200 <= code < 400: result = None
        elif code in [400, 403, 406, 429, 503, 999, 401]:
            if is_whitelist_domain(url) or has_title: result = None
            else: result = f"DEAD ({code} - Blocked/No Title) - {url}"
        elif code in [404, 410]: result = f"DEAD ({code} - Not Found) - {url}"
        else: result = f"WARNING ({code}) - {url}"
            
    except requests.exceptions.RequestException:
        if is_tracking_link(url): result = None
        else: result = f"ERROR (Connection Failed) - {url}"

    LINK_CACHE[url] = result
    return result

def audit_text_links_parallel(video_id, text, source_type):
    if not text: return []
    urls = re.findall(r'(https?://\S+)', text)
    cleaned_urls = list(set([u.rstrip('.,;)"\'') for u in urls]))
    external_links = [u for u in cleaned_urls if not any(d in u for d in ['youtube.com', 'youtu.be', 'google.com'])]

    if not external_links: return []

    STATS["total_occurrences"] += len(external_links)
    for u in external_links: UNIQUE_LINKS_SET.add(u)
    
    # Chỉ log vào full log (console/file), KHÔNG log vào email body
    log(f"   > [{source_type}] Phát hiện {len(external_links)} link ngoài. Đang kiểm tra...", is_error=False)

    issues = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(check_single_link, url): url for url in external_links}
        for future in concurrent.futures.as_completed(future_to_url):
            result = future.result()
            if result and result != "INTERNAL":
                issues.append(f"   [{source_type}] {result}")
                
    if issues:
        STATS["links_error"] += len(issues)
    
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
        except: return []

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
                    cache_key = f"YT_API_{target_id}"
                    if cache_key in LINK_CACHE:
                        api_result = LINK_CACHE[cache_key]
                    else:
                        api_result = "OK"
                        if element_type == "Video cụ thể":
                            check_vid = youtube.videos().list(id=target_id, part='status').execute()
                            if not check_vid['items']: api_result = "DEAD"
                        elif element_type == "Danh sách phát":
                            check_pl = youtube.playlists().list(id=target_id, part='status').execute()
                            if not check_pl['items']: api_result = "DEAD"
                        LINK_CACHE[cache_key] = api_result

                    if api_result == "DEAD":
                        issues.append(f"   [Màn hình kết thúc] {element_type} BỊ XÓA: {target_id}")
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
                    if duration <= 125: continue 
                    long_videos.append({'id': item['id'], 'title': item['snippet']['title'], 'desc': item['snippet']['description']})
            next_page_token = pl_response.get('nextPageToken')
            if not next_page_token: break
    except Exception as e:
        log(f"Lỗi khi lấy danh sách video: {e}")
    return long_videos

def send_email_report(total_issues_count, channel_name, crash_message=None):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    STATS['unique_links_count'] = len(UNIQUE_LINKS_SET)

    # 1. TẠO FILE LOG ĐÍNH KÈM (.txt)
    # Gom toàn bộ full_log_lines thành 1 chuỗi
    full_log_content = "\n".join(full_log_lines)
    # Tạo attachment
    attachment = MIMEApplication(full_log_content.encode('utf-8'), Name=f"Full_Audit_Log_{channel_name}.txt")
    attachment['Content-Disposition'] = f'attachment; filename="Full_Audit_Log_{channel_name}.txt"'
    msg.attach(attachment)

    # 2. TẠO NỘI DUNG EMAIL (CHỈ HIỆN THỐNG KÊ + LỖI)
    summary_block = (
        f"=== THỐNG KÊ KÊNH: {channel_name} ===\n"
        f"- Tổng video đã quét: {STATS['videos_scanned']}\n"
        f"- Link DUY NHẤT (Unique): {STATS['unique_links_count']} link\n"
        f"  (Tổng số lần xuất hiện link: {STATS['total_occurrences']} lần)\n"
        f"- Số lỗi phát hiện: {total_issues_count}\n"
        f"======================================\n\n"
    )

    if crash_message:
        msg['Subject'] = f"[{channel_name}] ❌ LỖI NGHIÊM TRỌNG"
        body_content = f"Hệ thống gặp lỗi:\n{crash_message}\n\n{summary_block}DANH SÁCH LỖI:\n" + "\n".join(email_error_lines)
    elif total_issues_count == 0:
        msg['Subject'] = f"[{channel_name}] ✅ Kênh Sạch (Xem log đính kèm)"
        body_content = f"{summary_block}Kênh hoạt động tốt.\nChi tiết quá trình quét vui lòng xem file đính kèm: Full_Audit_Log_{channel_name}.txt"
        log("✅ Đang gửi email báo cáo (Kênh sạch)...")
    else:
        msg['Subject'] = f"[{channel_name}] ⚠️ CẢNH BÁO - {total_issues_count} lỗi"
        # Chỉ hiển thị danh sách lỗi trong body, không hiện các dòng "Scanning..."
        body_content = f"{summary_block}Dưới đây là danh sách các lỗi cần xử lý:\n\n" + "\n".join(email_error_lines) + \
                       f"\n\n--- HẾT BÁO CÁO ---\nChi tiết đầy đủ xem file đính kèm."
        log("⚠️ Đang gửi email cảnh báo lỗi...", is_error=True)

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
    log("=== BẮT ĐẦU QUÉT (GỌN GÀNG + ĐÍNH KÈM LOG) ===")
    start_time = time.time()
    total_issues_count = 0
    current_channel_name = "Unknown Channel"
    
    try:
        try:
            ch_info = youtube.channels().list(id=CHANNEL_ID, part='snippet').execute()
            if ch_info['items']:
                current_channel_name = ch_info['items'][0]['snippet']['title']
                log(f"Kênh mục tiêu: {current_channel_name}")
        except Exception: pass

        # QUÉT KÊNH
        channel_issues = audit_channel_info(CHANNEL_ID)
        if channel_issues:
            total_issues_count += 1
            log(f"❌ CẢNH BÁO TẠI THÔNG TIN KÊNH:", is_error=True)
            for issue in channel_issues: log(issue, is_error=True)
            log("-" * 20, is_error=True)

        # QUÉT VIDEO
        videos = get_long_videos(CHANNEL_ID)
        STATS['videos_scanned'] = len(videos)
        log(f"Tổng số video dài: {len(videos)}")
        
        for index, video in enumerate(videos):
            vid_id = video['id']
            # Chỉ log tiến trình vào full log (console), KHÔNG vào email body
            log(f"[{index+1}/{len(videos)}] {video['title']}")
            
            vid_issues = []
            vid_issues.extend(audit_text_links_parallel(vid_id, video['desc'], "Mô tả"))
            vid_issues.extend(audit_end_screens_smart(vid_id))
            
            try:
                cmt_req = youtube.commentThreads().list(videoId=vid_id, part='snippet', maxResults=10, order='relevance', textFormat='plainText')
                cmt_res = cmt_req.execute()
                for item in cmt_res.get('items', []):
                    cmt_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                    vid_issues.extend(audit_text_links_parallel(vid_id, cmt_text, "Bình luận"))
            except: pass

            if vid_issues:
                total_issues_count += 1
                # Khi có lỗi, ta bật cờ is_error=True để nó ghi vào Email Body
                log(f"❌ CẢNH BÁO TẠI: https://youtu.be/{vid_id}", is_error=True)
                for issue in vid_issues: log(issue, is_error=True)
                log("-" * 20, is_error=True)
            
            # time.sleep(0.5)

        elapsed = round(time.time() - start_time, 2)
        log(f"=== HOÀN TẤT TRONG {elapsed} GIÂY ===")
        
        send_email_report(total_issues_count, current_channel_name)

    except Exception as e:
        error_msg = traceback.format_exc()
        print("GẶP LỖI NGHIÊM TRỌNG:", error_msg)
        # Ghi lỗi crash vào full log để lưu file
        full_log_lines.append(error_msg)
        send_email_report(0, current_channel_name, crash_message=error_msg)

if __name__ == "__main__":
    main()
