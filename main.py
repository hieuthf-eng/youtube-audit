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
CSV_DATA = []
CSV_HEADER = ['Video URL', 'Tiêu đề Video', 'Danh Sách Link (Mô Tả)', 'Danh Sách Link (Comment)', 'Trạng Thái EndScreen']
email_error_lines = []

# --- BIẾN TOÀN CỤC ---
STATS = {
    "videos_scanned": 0,
    "total_links_found": 0,
    "links_ok": 0,
    "links_error": 0,
    "endscreen_issues": 0
}
LINK_CACHE = {} 
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]
WHITELIST_DOMAINS = ['facebook.com', 'fb.me', 'twitter.com', 'x.com', 'linkedin.com', 'instagram.com', 'tiktok.com', 't.me', 'zalo.me', 'youtube.com', 'youtu.be', 'google.com']
TRACKING_KEYWORDS = ['pipaffiliates', 'affiliate', 'clicks.', 'track.', 'go.', 'bit.ly', 'tinyurl', 'ref=', 'click', 'partner', 'redirect']

def log(message):
    print(message, flush=True)

def parse_duration(duration_str):
    match = re.match(r'PT(\d+H)?(\d+M)?(\d+S)?', duration_str)
    if not match: return 0
    hours = int(match.group(1)[:-1]) if match.group(1) else 0
    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
    seconds = int(match.group(3)[:-1]) if match.group(3) else 0
    return (hours * 3600) + (minutes * 60) + seconds

# --- MODULE: KIỂM TRA ĐỊNH DẠNG VIDEO (MỚI) ---
def is_actually_short(video_id):
    """
    Kiểm tra xem video có phải là Shorts (Dọc) hay không bằng cách ping URL /shorts/
    Trả về: True (Là Shorts - Bỏ qua), False (Là Video Dài - Cần quét)
    """
    url = f"https://www.youtube.com/shorts/{video_id}"
    try:
        # Gửi request HEAD (nhẹ, không tải body) và KHÔNG cho phép tự động redirect
        r = requests.head(url, headers={'User-Agent': USER_AGENTS[0]}, allow_redirects=False, timeout=5)
        
        # Nếu Status là 200 -> Link /shorts/ tồn tại -> Đây là Video Dọc (Shorts)
        if r.status_code == 200:
            return True
        
        # Nếu Status là 303 (See Other) hoặc 301/302 -> YouTube đá về /watch -> Đây là Video Ngang (Dài)
        if r.status_code in [301, 302, 303, 307]:
            return False
            
    except:
        return False # Nếu lỗi mạng, tạm coi là Video dài để quét cho chắc
    
    return False

# --- MODULE CHECK LINK ---
def is_whitelist_domain(url):
    for domain in WHITELIST_DOMAINS:
        if domain in url: return True
    return False

def check_single_link_detailed(url):
    if any(d in url for d in ['youtube.com', 'youtu.be', 'google.com']): return "INTERNAL", "Nội bộ"
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
        code = response.status_code
        
        # Lấy snippet nhỏ để check title
        content_snippet = ""
        try:
            for chunk in response.iter_content(chunk_size=1024):
                content_snippet += chunk.decode('utf-8', errors='ignore')
                if len(content_snippet) > 5000: break
        except: pass
        has_title = '<title' in content_snippet.lower()

        if 200 <= code < 400: result = ("OK", f"OK ({code})")
        elif code in [400, 403, 406, 429, 503, 999, 401]:
            if is_whitelist_domain(url) or has_title: result = ("OK", f"OK (Anti-Bot {code})")
            else: result = ("ERROR", f"DEAD ({code})")
        elif code in [404, 410]: result = ("ERROR", f"DEAD ({code} Not Found)")
        else: result = ("ERROR", f"WARNING ({code})")
            
    except requests.exceptions.RequestException:
        result = ("ERROR", "Connection Failed")

    LINK_CACHE[url] = result
    return result

def audit_text_links_return_list(text, source_type):
    if not text: return []
    urls = re.findall(r'(https?://\S+)', text)
    cleaned_urls = list(set([u.rstrip('.,;)"\'') for u in urls]))
    external_links = [u for u in cleaned_urls if not any(d in u for d in ['youtube.com', 'youtu.be', 'google.com'])]

    if not external_links: return []

    STATS["total_links_found"] += len(external_links)
    results_list = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(check_single_link_detailed, url): url for url in external_links}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            status_type, msg = future.result()
            
            if status_type == "INTERNAL": continue
            
            display_line = f"{url} -> [{msg}]"
            results_list.append(display_line)
            
            if status_type == "ERROR":
                STATS["links_error"] += 1
                email_error_lines.append(f"[{source_type}] {display_line}")
            else:
                STATS["links_ok"] += 1
    return results_list

def audit_end_screens_return_list(video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"
    results_list = []
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
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
                target_id = None
                element_type = "Unknown"
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
                    api_status = "OK"
                    if element_type == "Video":
                        check = youtube.videos().list(id=target_id, part='status').execute()
                        if not check['items']: api_status = "DEAD"
                    elif element_type == "Playlist":
                        check = youtube.playlists().list(id=target_id, part='status').execute()
                        if not check['items']: api_status = "DEAD"
                    
                    if api_status == "DEAD":
                        msg = f"{element_type} {target_id} bị xóa/ẩn"
                        results_list.append(msg)
                        STATS["endscreen_issues"] += 1
                        email_error_lines.append(f"[EndScreen] {msg}")
            except: continue
    except: pass
    return results_list

def get_real_long_videos(channel_id):
    """
    Hàm lấy video dài với logic lọc Shorts thông minh
    """
    final_videos = []
    try:
        ch_response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
        if not ch_response['items']: return []
        uploads_playlist_id = ch_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        next_page_token = None
        log("Đang tải và lọc danh sách video (bỏ qua Shorts)...")
        
        while True:
            pl_request = youtube.playlistItems().list(playlistId=uploads_playlist_id, part='contentDetails', maxResults=50, pageToken=next_page_token)
            pl_response = pl_request.execute()
            video_ids = [item['contentDetails']['videoId'] for item in pl_response['items']]
            
            if video_ids:
                vid_request = youtube.videos().list(id=','.join(video_ids), part='snippet,contentDetails')
                vid_response = vid_request.execute()
                
                for item in vid_response['items']:
                    duration = parse_duration(item['contentDetails']['duration'])
                    vid_id = item['id']
                    
                    # LOGIC LỌC SHORTS THÔNG MINH
                    is_video_target = False
                    
                    # 1. Nếu video dài hơn 3 phút (180s) -> Chắc chắn là Video Dài (Shorts max 60s)
                    if duration > 180:
                        is_video_target = True
                    # 2. Nếu video <= 3 phút (Vùng nghi vấn) -> Check kỹ bằng Network
                    else:
                        # Nếu KHÔNG phải là Short (hàm trả về False) -> Thì nó là Video Dài
                        if not is_actually_short(vid_id):
                            is_video_target = True
                    
                    if is_video_target:
                        final_videos.append({'id': vid_id, 'title': item['snippet']['title'], 'desc': item['snippet']['description']})
            
            next_page_token = pl_response.get('nextPageToken')
            if not next_page_token: break
            
    except Exception as e:
        log(f"Lỗi khi lấy danh sách video: {e}")
        
    return final_videos

def send_email_with_csv(total_issues_count, channel_name, crash_message=None):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    
    # FILE CSV
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(CSV_HEADER)
    csv_writer.writerows(CSV_DATA)
    
    csv_bytes = csv_buffer.getvalue().encode('utf-8-sig')
    filename = f"Bao_Cao_{channel_name.replace(' ', '_')}.csv"
    attachment = MIMEApplication(csv_bytes, Name=filename)
    attachment['Content-Disposition'] = f'attachment; filename="{filename}"'
    msg.attach(attachment)

    summary_block = (
        f"=== THỐNG KÊ KÊNH: {channel_name} ===\n"
        f"- Tổng video đã quét (Đã lọc Shorts): {STATS['videos_scanned']}\n"
        f"- Link Tốt (OK): {STATS['links_ok']}\n"
        f"- Link Lỗi (ERROR): {STATS['links_error']}\n"
        f"======================================\n\n"
    )

    if crash_message:
        msg['Subject'] = f"[{channel_name}] ❌ LỖI HỆ THỐNG"
        body_content = f"Lỗi: {crash_message}\n\n{summary_block}"
    elif total_issues_count == 0:
        msg['Subject'] = f"[{channel_name}] ✅ Kênh Sạch - Xem Excel"
        body_content = f"{summary_block}Kênh hoạt động tốt. Chi tiết xem file đính kèm."
        log("✅ Đang gửi email báo cáo (Kênh sạch)...")
    else:
        msg['Subject'] = f"[{channel_name}] ⚠️ CẢNH BÁO - {total_issues_count} vấn đề"
        body_content = f"{summary_block}Tóm tắt lỗi:\n" + "\n".join(email_error_lines[:15]) + \
                       "\n\n... (Xem đầy đủ trong file Excel đính kèm)"
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
        print(f"Lỗi gửi email: {e}")

def main():
    log("=== BẮT ĐẦU QUÉT (LỌC SHORTS BẰNG CHECK URL) ===")
    start_time = time.time()
    total_issues_count = 0
    current_channel_name = "Unknown Channel"
    
    try:
        try:
            ch_info = youtube.channels().list(id=CHANNEL_ID, part='snippet').execute()
            if ch_info['items']:
                current_channel_name = ch_info['items'][0]['snippet']['title']
                log(f"Kênh: {current_channel_name}")
        except: pass

        # Sử dụng hàm lấy video mới (đã lọc Shorts thông minh)
        videos = get_real_long_videos(CHANNEL_ID)
        STATS['videos_scanned'] = len(videos)
        log(f"Tổng số video thực tế cần quét: {len(videos)}")
        
        for index, video in enumerate(videos):
            vid_id = video['id']
            log(f"[{index+1}/{len(videos)}] {video['title']}")
            
            desc_results = audit_text_links_return_list(video['desc'], "Mô tả")
            
            cmt_results = []
            try:
                cmt_req = youtube.commentThreads().list(videoId=vid_id, part='snippet', maxResults=10, order='relevance', textFormat='plainText')
                cmt_res = cmt_req.execute()
                for item in cmt_res.get('items', []):
                    cmt_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                    cmt_results.extend(audit_text_links_return_list(cmt_text, "Comment"))
            except: pass

            es_results = audit_end_screens_return_list(vid_id)

            row = [
                f"https://youtu.be/{vid_id}",
                video['title'],
                "\n".join(desc_results),
                "\n".join(cmt_results),
                "\n".join(es_results)
            ]
            CSV_DATA.append(row)

        total_issues_count = STATS['links_error'] + STATS['endscreen_issues']
        elapsed = round(time.time() - start_time, 2)
        log(f"=== HOÀN TẤT TRONG {elapsed} GIÂY ===")
        
        send_email_with_csv(total_issues_count, current_channel_name)

    except Exception as e:
        error_msg = traceback.format_exc()
        print("LỖI:", error_msg)
        send_email_with_csv(0, current_channel_name, crash_message=error_msg)

if __name__ == "__main__":
    main()
