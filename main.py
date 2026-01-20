import os
import re
import json
import requests
import smtplib
import time
import traceback
import concurrent.futures
import csv
import io
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
PYTHONUNBUFFERED = os.environ.get('PYTHONUNBUFFERED')

# Tắt cảnh báo SSL
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

youtube = build('youtube', 'v3', developerKey=API_KEY)

# --- KHAI BÁO BIẾN DỮ LIỆU BÁO CÁO (CSV) ---
# Format: [Video_URL, Video_Title, Location, Found_Link, Status, Note]
CSV_DATA = []
# Header cho file CSV
CSV_HEADER = ['Video URL', 'Tiêu đề Video', 'Vị trí (Mô tả/Cmt)', 'Link Tìm Thấy', 'Trạng Thái', 'Ghi Chú']

# Biến để hiện trong Email Body (chỉ lỗi)
email_error_lines = []

# --- BIẾN TOÀN CỤC ĐỂ THỐNG KÊ ---
STATS = {
    "videos_scanned": 0,
    "total_links_found": 0,
    "links_ok": 0,
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

def log(message):
    print(message, flush=True)

def add_to_report(video_url, video_title, location, found_link, status, note=""):
    """Thêm 1 dòng vào báo cáo CSV"""
    CSV_DATA.append([video_url, video_title, location, found_link, status, note])
    
    # Nếu là lỗi, thêm vào nội dung email body để cảnh báo nhanh
    if status == "ERROR":
        email_error_lines.append(f"[{location}] {found_link} -> {note}")

def parse_duration(duration_str):
    match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration_str)
    if not match: return 0
    hours = int(match.group(1)[:-1]) if match.group(1) else 0
    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
    seconds = int(match.group(3)[:-1]) if match.group(3) else 0
    return (hours * 3600) + (minutes * 60) + seconds

# --- MODULE 1: CHECK LINK WEBSITE (TRẢ VỀ KẾT QUẢ CHI TIẾT) ---
def is_whitelist_domain(url):
    for domain in WHITELIST_DOMAINS:
        if domain in url: return True
    return False

def check_single_link_detailed(url):
    """
    Trả về: (Status_Type, Message)
    Status_Type: 'OK' hoặc 'ERROR'
    """
    if any(d in url for d in ['youtube.com', 'youtu.be', 'google.com']): return "INTERNAL", "Link nội bộ"
    
    if url in LINK_CACHE: return LINK_CACHE[url]

    import random
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    result = ("OK", "200 OK") 
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

        if 200 <= code < 400: 
            result = ("OK", f"{code} OK")
        elif code in [400, 403, 406, 429, 503, 999, 401]:
            if is_whitelist_domain(url) or has_title: 
                result = ("OK", f"{code} (Anti-Bot but OK)")
            else: 
                result = ("ERROR", f"DEAD ({code} - Blocked)")
        elif code in [404, 410]: 
            result = ("ERROR", f"DEAD ({code} - Not Found)")
        else: 
            result = ("ERROR", f"WARNING ({code})")
            
    except requests.exceptions.RequestException:
        result = ("ERROR", "Connection Failed")

    LINK_CACHE[url] = result
    return result

def audit_text_links_parallel(video_id, video_title, text, source_type):
    if not text: return
    urls = re.findall(r'(https?://\S+)', text)
    cleaned_urls = list(set([u.rstrip('.,;)"\'') for u in urls]))
    
    # Lọc link ngoài
    external_links = [u for u in cleaned_urls if not any(d in u for d in ['youtube.com', 'youtu.be', 'google.com'])]

    if not external_links: return

    video_full_url = f"https://youtu.be/{video_id}"
    STATS["total_links_found"] += len(external_links)
    
    log(f"   > [{source_type}] Check {len(external_links)} links...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(check_single_link_detailed, url): url for url in external_links}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            status_type, msg = future.result()
            
            if status_type == "INTERNAL":
                continue # Không ghi link nội bộ vào báo cáo để cho gọn
            
            # GHI VÀO FILE BÁO CÁO CSV
            add_to_report(video_full_url, video_title, source_type, url, status_type, msg)
            
            if status_type == "ERROR":
                STATS["links_error"] += 1
            else:
                STATS["links_ok"] += 1

# --- MODULE 2: QUÉT MÀN HÌNH KẾT THÚC ---
def audit_end_screens_smart(video_id, video_title):
    url = f"https://www.youtube.com/watch?v={video_id}"
    video_full_url = f"https://youtu.be/{video_id}"
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=10)
        html = response.text
        match = re.search(r'var ytInitialPlayerResponse\s*=\s*({.+?});', html)
        if not match: return

        try:
            data = json.loads(match.group(1))
            end_screen_elements = data['endscreen']['endScreenRenderer']['elements']
        except: return

        for el in end_screen_elements:
            try:
                element_type = "Không rõ"
                target_id = None
                
                if 'endScreenVideoRenderer' in el:
                    renderer = el['endScreenVideoRenderer']
                    if 'videoId' in renderer:
                        target_id = renderer['videoId']
                        element_type = "Video"
                    else: continue 
                elif 'endScreenPlaylistRenderer' in el:
                    renderer = el['endScreenPlaylistRenderer']
                    if 'playlistId' in renderer:
                        target_id = renderer['playlistId']
                        element_type = "Playlist"

                if target_id:
                    # Check API
                    api_status = "OK"
                    note = "Active"
                    
                    if element_type == "Video":
                        check_vid = youtube.videos().list(id=target_id, part='status').execute()
                        if not check_vid['items']: 
                            api_status = "ERROR"
                            note = f"Video {target_id} bị xóa/ẩn"
                    elif element_type == "Playlist":
                        check_pl = youtube.playlists().list(id=target_id, part='status').execute()
                        if not check_pl['items']: 
                            api_status = "ERROR"
                            note = f"Playlist {target_id} bị xóa/ẩn"
                    
                    if api_status == "ERROR":
                        add_to_report(video_full_url, video_title, "EndScreen", target_id, "ERROR", note)
                        STATS["endscreen_issues"] += 1

            except Exception: continue
    except Exception: pass

# --- MODULE 0: QUÉT THÔNG TIN KÊNH ---
def audit_channel_info(channel_id):
    log(">> Đang kiểm tra thông tin Kênh (About Section)...")
    try:
        response = youtube.channels().list(id=channel_id, part='snippet').execute()
        if not response['items']: return
        item = response['items'][0]
        description = item['snippet']['description']
        audit_text_links_parallel("CHANNEL_HOME", "Trang Chủ Kênh", description, "Mô tả Kênh")
    except Exception as e:
        log(f"Lỗi khi quét kênh: {e}")

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

def send_email_with_csv(total_issues_count, channel_name, crash_message=None):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    
    # 1. TẠO FILE CSV (Trong bộ nhớ)
    # Dùng io.StringIO để ghi file CSV ảo
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    # Ghi header
    csv_writer.writerow(CSV_HEADER)
    # Ghi dữ liệu
    csv_writer.writerows(CSV_DATA)
    
    # Chuyển sang bytes để đính kèm (UTF-8 with BOM để mở Excel không lỗi font tiếng Việt)
    csv_bytes = csv_buffer.getvalue().encode('utf-8-sig')
    
    # Tạo attachment
    filename = f"Bao_Cao_Kenh_{channel_name.replace(' ', '_')}.csv"
    attachment = MIMEApplication(csv_bytes, Name=filename)
    attachment['Content-Disposition'] = f'attachment; filename="{filename}"'
    msg.attach(attachment)

    # 2. TẠO NỘI DUNG EMAIL
    summary_block = (
        f"=== THỐNG KÊ KÊNH: {channel_name} ===\n"
        f"- Tổng video đã quét: {STATS['videos_scanned']}\n"
        f"- Tổng Link đã check: {STATS['total_links_found']}\n"
        f"- Link Tốt (OK): {STATS['links_ok']}\n"
        f"- Link Lỗi (ERROR): {STATS['links_error']}\n"
        f"- Lỗi Màn hình kết thúc: {STATS['endscreen_issues']}\n"
        f"======================================\n\n"
    )

    if crash_message:
        msg['Subject'] = f"[{channel_name}] ❌ LỖI - Script dừng đột ngột"
        body_content = f"Lỗi hệ thống:\n{crash_message}\n\n{summary_block}Vui lòng xem file CSV đính kèm để biết chi tiết những gì đã quét được."
    elif total_issues_count == 0:
        msg['Subject'] = f"[{channel_name}] ✅ Kênh Sạch - Xem báo cáo Excel"
        body_content = f"{summary_block}Hệ thống không phát hiện link hỏng nào.\nChi tiết toàn bộ link trên kênh vui lòng xem file Excel đính kèm."
        log("✅ Đang gửi email báo cáo (Kênh sạch)...")
    else:
        msg['Subject'] = f"[{channel_name}] ⚠️ CẢNH BÁO - {total_issues_count} link hỏng"
        body_content = f"{summary_block}Dưới đây là tóm tắt các link lỗi:\n\n" + "\n".join(email_error_lines[:20]) + \
                       f"\n\n(Danh sách quá dài, vui lòng mở file CSV đính kèm để xem đầy đủ cột Link và Video URL)"
        log("⚠️ Đang gửi email cảnh báo lỗi...")

    msg.attach(MIMEText(body_content, 'plain'))
    
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)
        server.quit()
        print(">> Đã gửi email kèm file CSV thành công!")
    except Exception as e:
        print(f"Lỗi không gửi được email: {e}")

def main():
    log("=== BẮT ĐẦU QUÉT (CHẾ ĐỘ XUẤT BÁO CÁO EXCEL) ===")
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
        audit_channel_info(CHANNEL_ID)

        # QUÉT VIDEO
        videos = get_long_videos(CHANNEL_ID)
        STATS['videos_scanned'] = len(videos)
        log(f"Tổng số video dài: {len(videos)}")
        
        for index, video in enumerate(videos):
            vid_id = video['id']
            # Console Log giờ sẽ hiện cả Link video cho bạn dễ copy nếu cần gấp
            log(f"[{index+1}/{len(videos)}] {video['title']} (https://youtu.be/{vid_id})")
            
            # Check Link
            audit_text_links_parallel(vid_id, video['title'], video['desc'], "Mô tả")
            audit_end_screens_smart(vid_id, video['title'])
            
            try:
                cmt_req = youtube.commentThreads().list(videoId=vid_id, part='snippet', maxResults=10, order='relevance', textFormat='plainText')
                cmt_res = cmt_req.execute()
                for item in cmt_res.get('items', []):
                    cmt_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                    audit_text_links_parallel(vid_id, video['title'], cmt_text, "Bình luận")
            except: pass
            
        # Tính tổng lỗi
        total_issues_count = STATS['links_error'] + STATS['endscreen_issues']
        
        elapsed = round(time.time() - start_time, 2)
        log(f"=== HOÀN TẤT TRONG {elapsed} GIÂY ===")
        
        send_email_with_csv(total_issues_count, current_channel_name)

    except Exception as e:
        error_msg = traceback.format_exc()
        print("GẶP LỖI NGHIÊM TRỌNG:", error_msg)
        send_email_with_csv(0, current_channel_name, crash_message=error_msg)

if __name__ == "__main__":
    main()
