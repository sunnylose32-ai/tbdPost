import json
import random
import time
from datetime import datetime
from pathlib import Path

from telegram import Bot
import firebase_admin
from firebase_admin import credentials, db
from google.auth.exceptions import RefreshError

# ── CONFIG ───────────────────────────────────────
TOKEN = "8528019795:AAG_AjFfpIARLEj676m7TroUoUvLYccsu7U"
CHANNEL = "-1003019960457"
POSTS_PER_BATCH = 1
FIREBASE_JSON = "promovie-77716-firebase-adminsdk-fbsvc-237cc06372.json"  # ← Use NEW KEY
FIREBASE_URL = "https://promovie-77716-default-rtdb.firebaseio.com/"

# Times in 24h format (HH:MM)
now = datetime.now()
SCHEDULE_HM = now.strftime("%H:%M")

POSTED_PATH = Path("websitePostedRecord.json")
# ─────────────────────────────────────────────────

# Initialize Telegram bot
bot = Bot(token=TOKEN)

# Load already posted video URLs
posted = set()
if POSTED_PATH.exists():
    try:
        posted = set(json.loads(POSTED_PATH.read_text(encoding="utf-8")))
        print(f"Loaded {len(posted)} already posted video URLs")
    except Exception as e:
        print(f"Failed to load posted_videos.json → {type(e).__name__}: {e}")

# ── FIREBASE INITIALIZATION ──────────────────────
try:
    cred = credentials.Certificate(FIREBASE_JSON)
    print(f"Service account loaded → {cred.service_account_email}")
    print(f"Project ID: {cred.project_id}")

    firebase_admin.initialize_app(cred, {'databaseURL': FIREBASE_URL})
    print("Firebase initialized successfully")
except Exception as e:
    print(f"CRITICAL: Firebase init failed → {type(e).__name__}: {e}")
    exit(1)

# ── FUNCTIONS ────────────────────────────────────

def get_movies_to_post(retries=3, delay=3):
    """Fetch movies from Firebase, retry on temporary auth/network issues"""
    for attempt in range(1, retries + 1):
        try:
            ref = db.reference("/movies")
            data = ref.get() or {}
            movies = [
                m for m in data.values()
                if isinstance(m, dict)
                and (url := m.get("videoUrl"))
                and url not in posted
            ]
            print(f"[DEBUG] Fetched {len(movies)} eligible movies from Firebase")
            return movies
        except RefreshError as e:
            print(f"[Attempt {attempt}] Firebase auth failed → {type(e).__name__}: {e}")
        except Exception as e:
            print(f"[Attempt {attempt}] Firebase read failed → {type(e).__name__}: {e}")
        if attempt < retries:
            time.sleep(delay)
    print("All Firebase fetch attempts failed")
    return []

def send_one_movie(movie):
    title = movie.get("title", "Untitled")
    poster = movie.get("poster")
    video_url = movie.get("videoUrl")

    if not video_url:
        print(f"Skipping '{title}' → no videoUrl")
        return False

    # Clean slug for blogspot link
    url_slug = "".join(c if c.isalnum() or c in "- " else "-" for c in title.lower()).replace(" ", "-").strip("-")
    website_url = f"https://www.videolink.online/#video-{url_slug}"

    caption = f"🎬 {title}\n\n📺 Watch here: {website_url} \n\nEkdom direct video dekho aso"

    try:
        if poster:
            bot.send_photo(chat_id=CHANNEL, photo=poster, caption=caption, disable_notification=True)
        else:
            bot.send_message(chat_id=CHANNEL, text=caption, disable_notification=True)

        posted.add(video_url)  # Mark original video URL as posted
        print(f"Posted → {title}")
        time.sleep(2.5)
        return True
    except Exception as e:
        print(f"Failed to post '{title}' → {type(e).__name__}: {e}")
        return False

def save_posted_list():
    try:
        POSTED_PATH.write_text(json.dumps(list(posted), ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Saved posted list ({len(posted)} items)")
    except Exception as e:
        print(f"Failed to save posted list → {type(e).__name__}: {e}")

def post_batch():
    movies = get_movies_to_post()
    if not movies:
        print("No new videos available to post")
        return

    print(f"Preparing to send up to {POSTS_PER_BATCH} videos...")
    random.shuffle(movies)

    sent_count = 0
    for movie in movies:
        if sent_count >= POSTS_PER_BATCH:
            break
        if send_one_movie(movie):
            sent_count += 1

    if sent_count > 0:
        save_posted_list()
        print(f"Batch done — {sent_count} sent | Total posted: {len(posted)}")

# ── MAIN LOOP ────────────────────────────────────

def main():
    print("Starting Telegram + Firebase poster bot...")
    print(f"Channel: {CHANNEL}")
    print(f"Schedule: {', '.join(SCHEDULE_HM)}")
    print(f"Already posted: {len(posted)}")


    last_run = ""

    while True:
        now = datetime.now()
        hm = now.strftime("%H:%M")

        if hm in SCHEDULE_HM and hm != last_run:
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] Triggering batch at {hm}")
            post_batch()
            last_run = hm
            time.sleep(70)  # avoid double trigger

        time.sleep(5)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped (Ctrl+C)")
    except Exception as exc:
        print(f"Main crashed: {type(exc).__name__}: {exc}")
