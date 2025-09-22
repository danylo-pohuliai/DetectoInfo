import os
import re, uuid, html, psycopg2, string, random, base64, logging
from datetime import datetime, timezone,  timedelta
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto,
    User, InlineQueryResultArticle, InputTextMessageContent
)
from telegram.ext import (
    ApplicationBuilder, ConversationHandler,
    CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes, InlineQueryHandler
)
from telegram.helpers import escape_markdown
from telegram.error import BadRequest
from colorama import Fore
from typing import Callable, Any
from urllib.parse import quote

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
# ====== Стани ======
CATEGORY, CITY, PRICE, DESC, PHOTO, CONFIRM, EDIT_CATEGORY, EDIT_CITY, EDIT_PRICE, EDIT_DESC, EDIT_PHOTO = range(11)
REVIEW_RATING, REVIEW_COMMENT = range(20, 22)

load_dotenv() 
# ====== DB CONFIG ======
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

DB_PARAMS = {
    "host": DB_HOST,
    "dbname": DB_NAME,
    "user": DB_USER,
    "password": DB_PASS,
}

BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH")
DOMAIN = os.getenv("DOMAIN")
WEBHOOK_URL = f"https://{DOMAIN}/{WEBHOOK_PATH}"

MIN_QUERY_LEN = 2
PAGE_SIZE = 8   # скільки оголошень показувати за раз
NAV_SIZE  = 5

MAX_CITY_LEN = 70
MAX_PRICE_LEN = 50
MAX_DESC_LEN = 500

CHANGE_NICK = 25 
NICK_CHANGE_COOLDOWN = timedelta(days=30)
 
RE_NICK = re.compile(r'^[A-Za-zА-Яа-яЁёЇїІіЄєҐґ0-9_ ]{3,50}$')

CATEGORY_LABELS = {
    "general": "Оголошення від поліграфологів",
    "search":  "Шукаю поліграфолога",
    "other":   "Купівля, продаж, вакансії",
}

APPS_LABELS = {
    "accepted": "Прийнята",
    "pending": "В обробці",
    "rejected": "Відхилена" 
}

# --- DATABASE ---

def bot_username_exists(nick: str) -> bool:
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    with conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM users WHERE bot_username = %s", (nick,))
        return cur.fetchone() is not None

def generate_bot_username(cur) -> str:
    while True:
        suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=7))
        candidate = f"Користувач_{suffix}"
        cur.execute("SELECT 1 FROM users WHERE bot_username = %s", (candidate,))
        if not cur.fetchone():
            return candidate

def save_ad(ad: dict, user_id: int):
    city = ad['city'][:MAX_CITY_LEN]
    price = ad['price'][:MAX_PRICE_LEN]
    desc  = ad['desc'][:MAX_DESC_LEN]
    photo = ad.get('photo_id')
    category = ad.get('category')

    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ads (user_id, city, price, description, photo_id, category)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (user_id, city, price, desc, photo, category)
            )
    conn.close()

def fetch_ads(category: str):
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
              a.id,
              a.city,
              a.price,
              a.created_at,
              u.id AS user_id,
              u.username,
              u.full_name,
              u.bot_username,
              u.avg_rating 
            FROM ads a
            JOIN users u ON a.user_id = u.id
            WHERE a.category = %s
            ORDER BY a.created_at DESC
        """, (category,))
        rows = cur.fetchall()
    conn.close()
    return rows

def fetch_ad_by_id(ad_id: int):
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  a.id,
                  a.city,
                  a.price,
                  a.description AS desc,
                  a.photo_id,
                  a.created_at,
                  a.category,
                  u.id   AS author_id,
                  u.username,
                  u.full_name,
                  u.bot_username,
                  u.avg_rating  
                FROM ads a
                JOIN users u ON a.user_id = u.id
                WHERE a.id = %s
            """, (ad_id,))
            ad = cur.fetchone()
    conn.close()

    if not ad:
        return None

    ad['author'] = {
        'id': ad.pop('author_id'),
        'username': ad.pop('username'),
        'full_name': ad.pop('full_name'),
        'bot_username': ad.pop('bot_username'),
        'avg_rating': ad.pop('avg_rating')
    }
    return ad

def fetch_user_by_id(user_id: int) -> dict:
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, username, full_name, ad_quota, bot_username, bot_username_changed_at
                  FROM users
                 WHERE id = %s
            """, (user_id,))
            return cur.fetchone()

def save_user(tg_user: User):
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT bot_username FROM users WHERE id = %s", (tg_user.id,))
            row = cur.fetchone()
            if row is None:
                bot_username = generate_bot_username(cur)
                cur.execute(
                    """
                    INSERT INTO users (id, username, full_name, bot_username, created_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    """,
                    (
                        tg_user.id,
                        tg_user.username or None,
                        f"{tg_user.first_name or ''} {tg_user.last_name or ''}".strip() or None,
                        bot_username
                    )
                )
            else:
                cur.execute(
                    """
                    UPDATE users
                       SET username   = %s,
                           full_name  = %s
                     WHERE id = %s
                       AND (
                         users.username IS DISTINCT FROM %s
                         OR users.full_name IS DISTINCT FROM %s
                       )
                    """,
                    (
                        tg_user.username or None,
                        f"{tg_user.first_name or ''} {tg_user.last_name or ''}".strip() or None,
                        tg_user.id,
                        tg_user.username or None,
                        f"{tg_user.first_name or ''} {tg_user.last_name or ''}".strip() or None
                    )
                )
    conn.close()

def fetch_distinct_cities(prefix: str, limit: int = 10, category: str = None) -> list[str]:
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    with conn:
        with conn.cursor() as cur:
            if category:
                cur.execute("""
                    SELECT
                      city,
                      COUNT(*) AS cnt
                    FROM ads
                    WHERE city ILIKE %s
                      AND LOWER(category) = LOWER(%s)
                    GROUP BY city
                    ORDER BY cnt DESC, city ASC
                    LIMIT %s
                """, (f"%{prefix}%", category, limit))
            else:
                cur.execute("""
                    SELECT
                      city,
                      COUNT(*) AS cnt
                    FROM ads
                    WHERE city ILIKE %s
                    GROUP BY city
                    ORDER BY cnt DESC, city ASC
                    LIMIT %s
                """, (f"%{prefix}%", limit))
            rows = cur.fetchall()
    conn.close()

    return [row['city'] for row in rows]

def fetch_ads_by_city(city: str, category: str = None):
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    with conn:
        with conn.cursor() as cur:
            if category:
                cur.execute("""
                    SELECT
                      a.id,
                      a.city,
                      a.price,
                      a.description AS desc,
                      a.photo_id,
                      a.created_at,
                      u.id   AS author_id,
                      u.username,
                      u.full_name,
                      u.bot_username,
                      u.avg_rating
                    FROM ads a
                    JOIN users u ON a.user_id = u.id
                    WHERE LOWER(a.city) = LOWER(%s)
                      AND LOWER(a.category) = LOWER(%s)
                    ORDER BY a.created_at DESC
                """, (city, category))
            else:
                cur.execute("""
                    SELECT
                      a.id,
                      a.city,
                      a.price,
                      a.description AS desc,
                      a.photo_id,
                      a.created_at,
                      u.id   AS author_id,
                      u.username,
                      u.full_name,
                      u.bot_username,
                      u.avg_rating
                    FROM ads a
                    JOIN users u ON a.user_id = u.id
                    WHERE LOWER(a.city) = LOWER(%s)
                    ORDER BY a.created_at DESC
                """, (city,))
            ads = cur.fetchall()
    conn.close()

    for ad in ads:
        ad['author'] = {
            'id': ad.pop('author_id'),
            'username': ad.pop('username'),
            'full_name': ad.pop('full_name'),
            'bot_username': ad.pop('bot_username'),
            'avg_rating': ad.pop('avg_rating'),
        }
    return ads

def fetch_top_cities_list(category: str, top_n: int = None):
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    with conn, conn.cursor() as cur:
        sql = """
            SELECT
                city,
                COUNT(*) AS cnt
            FROM ads
            WHERE category = %s
            GROUP BY city
            ORDER BY cnt DESC, city ASC
        """
        params = [category]

        if top_n:
            sql += " LIMIT %s"
            params.append(top_n)

        cur.execute(sql, params)
        rows = cur.fetchall()

    conn.close()
    return rows

def fetch_top_ads_list(category: str, limit: int = 100):
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                  a.id,
                  a.city,
                  a.price,
                  a.created_at,
                  u.username,
                  u.full_name,
                  u.bot_username,
                  u.id AS user_id,
                  u.avg_rating
                FROM ads a
                JOIN users u ON a.user_id = u.id
                WHERE a.category = %s
                ORDER BY
                  u.avg_rating DESC,
                  a.created_at DESC
                LIMIT %s
            """, (category, limit))
            rows = cur.fetchall()
    finally:
        conn.close()

    return rows

def fetch_ads_by_user(user_id: int) -> list[dict]:
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                      a.id,
                      a.city,
                      a.price,
                      a.description AS desc,
                      a.photo_id,
                      a.created_at
                    FROM ads a
                    WHERE a.user_id = %s
                    ORDER BY a.created_at DESC
                """, (user_id,))
                ads = cur.fetchall()
    finally:
        conn.close()

    return ads

def update_ad(ad: dict, ad_id: int):
    city = ad['city'][:MAX_CITY_LEN]
    price = ad['price'][:MAX_PRICE_LEN]
    desc  = ad['desc'][:MAX_DESC_LEN]
    photo = ad.get('photo_id')
    category = ad.get('category')
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE ads
                   SET city        = %s,
                       price       = %s,
                       description = %s,
                       photo_id    = %s,
                       category    = %s
                 WHERE id = %s
            """, (city, price, desc, photo, category, ad_id))
    conn.close()

def save_review(review: dict):
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO reviews (author_id, target_id, ad_id, rating, comment)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    review['author_id'],
                    review['target_id'],
                    review.get('ad_id'),
                    review['rating'],
                    review.get('comment')
                )
            )
    conn.close()

def fetch_reviews_by_author(author_id: int) -> list[dict]:
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                      r.id,
                      r.target_id,
                      r.ad_id,
                      r.rating,
                      r.comment,
                      r.created_at,
                      u.username,
                      u.full_name,
                      u.bot_username
                    FROM reviews r
                    JOIN users u ON r.target_id = u.id
                    WHERE r.author_id = %s
                    ORDER BY r.created_at DESC
                """, (author_id,))
                rows = cur.fetchall()
    finally:
        conn.close()

    return rows

def fetch_review_by_id(review_id: int) -> dict | None:
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  r.id,
                  r.author_id,
                  r.target_id,
                  r.ad_id,
                  r.rating,
                  r.comment,
                  r.created_at,
                  u.id   AS target_id,
                  u.username,
                  u.full_name,
                  u.bot_username
                FROM reviews r
                JOIN users u ON r.target_id = u.id
                WHERE r.id = %s
            """, (review_id,))
            row = cur.fetchone()
    conn.close()
    if not row:
        return None
    
    row['target'] = {
        'id':       row.pop('target_id'),
        'username': row.pop('username'),
        'full_name':row.pop('full_name'),
        'bot_username': row.pop('bot_username')
    }
    return row

def fetch_reviews_for_user(target_id: int) -> list[dict]:
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                      r.id,
                      r.author_id,
                      r.rating,
                      r.comment,
                      r.created_at,
                      u.id   AS auth_id,
                      u.username,
                      u.full_name,
                      u.bot_username
                    FROM reviews r
                    JOIN users u ON r.author_id = u.id
                    WHERE r.target_id = %s
                    ORDER BY r.created_at DESC
                """, (target_id,))
                rows = cur.fetchall()
    finally:
        conn.close()

    for row in rows:
        row['author'] = {
            'id':        row.pop('auth_id'),
            'username':  row.pop('username'),
            'full_name': row.pop('full_name'),
            'bot_username': row.pop('bot_username')
        }
    return rows

def has_applied(ad_id: int, user_id: int) -> bool:
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME,
                            user=DB_USER, password=DB_PASS,
                            cursor_factory=RealDictCursor)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT EXISTS(SELECT 1 FROM applications "
                    "WHERE ad_id=%s AND requester_id=%s)",
                    (ad_id, user_id)
                )
                return cur.fetchone()['exists']
    finally:
        conn.close()

def save_application(ad_id: int, requester_id: int, executor_id: int) -> int:
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO applications
                      (ad_id, requester_id, executor_id, status)
                    VALUES (%s, %s, %s, 'pending')
                    RETURNING id
                    """,
                    (ad_id, requester_id, executor_id)
                )
                return cur.fetchone()['id']
    finally:
        conn.close()

def update_application_status(app_id: int, new_status: str):
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE applications
                       SET status = %s,
                           updated_at = CURRENT_TIMESTAMP
                     WHERE id = %s
                    """,
                    (new_status, app_id)
                )
    finally:
        conn.close()

def fetch_application(app_id: int) -> dict | None:
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM applications WHERE id = %s", (app_id,))
            return cur.fetchone()
    finally:
        conn.close()

def has_completed_application(requester_id: int, executor_id: int) -> bool:
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                  FROM applications
                 WHERE requester_id = %s
                   AND executor_id  = %s
                   AND status       = 'accepted'
                 LIMIT 1
                """,
                (requester_id, executor_id)
            )
            return cur.fetchone() is not None
    finally:
        conn.close()

def has_pending_application(ad_id: int, user_id: int) -> bool:
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS(
                        SELECT 1
                          FROM applications
                         WHERE ad_id = %s
                           AND requester_id = %s
                           AND status = 'pending'
                    )
                    """,
                    (ad_id, user_id)
                )
                return cur.fetchone()['exists']
    finally:
        conn.close()

def count_accepted_applications(author_id: int, target_id: int) -> int:
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                      FROM applications
                     WHERE status = 'accepted'
                       AND (
                             (requester_id = %s AND executor_id  = %s)
                          OR (requester_id = %s AND executor_id  = %s)
                           )
                    """,
                    (author_id, target_id, target_id, author_id)
                )
                return cur.fetchone()['cnt']
    finally:
        conn.close()

def count_reviews_by_author_for_executor(author_id: int, target_id: int) -> int:
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                      FROM reviews
                     WHERE author_id = %s
                       AND target_id = %s
                    """,
                    (author_id, target_id)
                )
                return cur.fetchone()['cnt']
    finally:
        conn.close()

def fetch_applications_for_requester(user_id: int) -> list[dict]:
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  ap.id      AS app_id,
                  ap.ad_id,
                  ap.status,
                  ap.created_at,
                  ads.city,
                  ads.price,
                  u.id   AS executor_id,
                  u.bot_username AS executor_bot_username
                FROM applications ap
                JOIN ads   ON ap.ad_id = ads.id
                JOIN users u  ON ap.executor_id = u.id
                WHERE ap.requester_id = %s
                ORDER BY
                  CASE ap.status
                    WHEN 'pending'  THEN 1
                    WHEN 'accepted' THEN 2
                    WHEN 'rejected' THEN 3
                  END,
                  ap.created_at DESC
            """, (user_id,))
            return cur.fetchall()
    finally:
        conn.close()

def fetch_user_subscriptions(user_id: int) -> list[dict]:
    conn = psycopg2.connect(**DB_PARAMS, cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT u.id, u.bot_username
                  FROM user_subscriptions us
                  JOIN users u ON us.author_id = u.id
                 WHERE us.subscriber_id = %s
                 ORDER BY us.created_at DESC
            """, (user_id,))
            return cur.fetchall()
    finally:
        conn.close()

def fetch_category_subscriptions(user_id: int) -> list[str]:
    conn = psycopg2.connect(**DB_PARAMS, cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT category
                  FROM category_subscriptions
                 WHERE subscriber_id = %s
                 ORDER BY created_at DESC
            """, (user_id,))
            return [r['category'] for r in cur.fetchall()]
    finally:
        conn.close()

def ad_exists(ad_id: int, category: str) -> bool:
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                  FROM ads
                 WHERE id = %s
                   AND category = %s
                LIMIT 1
                """,
                (ad_id, category)
            )
            return cur.fetchone() is not None
    finally:
        conn.close()

# ====== Створення клавіатури головного меню ======
def main_menu() -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(f"🖥️ {CATEGORY_LABELS['general']}", callback_data="view_ads_general")],
        [InlineKeyboardButton(f"🕵️ {CATEGORY_LABELS['search']}", callback_data="view_ads_search")],
        [InlineKeyboardButton(f"💼 {CATEGORY_LABELS['other']}", callback_data="view_ads_other")],
        [InlineKeyboardButton("➕ Розмістити оголошення", callback_data="post_ad")],
        [InlineKeyboardButton("🗂️ Особистий кабінет", callback_data="account")],
        [
            InlineKeyboardButton("🤝 Спільнота", callback_data="community"),
            InlineKeyboardButton("🆘 Підтримка", callback_data="support"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)

# ====== Хендлери ConversationHandler ======
async def post_ad_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await update.callback_query.answer()
        send = update.callback_query.edit_message_text
    else:
        send = update.message.reply_text
    
    user_id = update.effective_user.id

    user = fetch_user_by_id(user_id)
    quota = user.get('ad_quota', 3)
    current_ads = len(fetch_ads_by_user(user_id))

    if current_ads >= quota:
        text = (
            f"❌ Ви досягли ліміту у {quota} активних оголошень.\n"
            f"Зараз у вас {current_ads}/{quota}.\n"
            "Видаліть непотрібні оголошення або зверніться до підтримки для підвищення квоти."
        )
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(
                text=text,
                reply_markup=main_menu()
            )
        else:
            await update.message.reply_text(
                text=text,
                reply_markup=main_menu()
            )
        return ConversationHandler.END

    keyboard = [
        [InlineKeyboardButton(f"🖥️ {CATEGORY_LABELS['general']}", callback_data="cat_general")],
        [InlineKeyboardButton(f"🕵️ {CATEGORY_LABELS['search']}",       callback_data="cat_search")],
        [InlineKeyboardButton(f"💼 {CATEGORY_LABELS['other']}", callback_data="cat_other")],
        [InlineKeyboardButton("❌ Відмінити створення оголошення",              callback_data="cancel")],
    ]
    await send(
        "Оберіть категорію вашого оголошення:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return CATEGORY

async def category_chosen(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    ctx.user_data['category'] = query.data.split("_", 1)[1]

    await query.edit_message_text("Вкажіть місто вашого оголошення:")
    return CITY

async def city_received(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data['city'] = update.message.text
    await update.message.reply_text("Яка ціна ваших послуг?\n (Наприклад: 2000 грн., 2000₴, 100$ або діапазон 2000-3000₴)")
    return PRICE

async def price_received(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        data = query.data
        if data.startswith("preset_"):
            ctx.user_data['price'] = data.split("_")[1]
            await query.edit_message_text("Опишіть вашу послугу в декілька речень:")
            return DESC
        else:
            await query.edit_message_text("Введіть свою ціну цифрами:")
            return PRICE
    else:
        ctx.user_data['price'] = update.message.text
        await update.message.reply_text("Опишіть вашу послугу в декілька речень:")
        return DESC

async def desc_received(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data['desc'] = update.message.text
    kb = [[InlineKeyboardButton("Пропустити фото", callback_data="no_photo")]]
    await update.message.reply_text("Відправте фото оголошення, або натисність «Пропустити фото»", reply_markup=InlineKeyboardMarkup(kb))
    return PHOTO

async def photo_received(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query and update.callback_query.data == "no_photo":
        await update.callback_query.answer()
        ctx.user_data['photo_id'] = None
    else:
        ctx.user_data['photo_id'] = update.message.photo[-1].file_id
    return await send_summary(update, ctx)

async def send_summary(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ad = ctx.user_data
    cat_label = CATEGORY_LABELS.get(ad.get('category'), ad.get('category', '—'))

    summary = (
        f"Ваше оголошення:\n"
        f"Категорія: <b>{html.escape(cat_label)}</b>\n"
        f"Місто: {escape_markdown(ad['city'], version=2)}\n"
        f"Ціна: {escape_markdown(ad['price'], version=2)}\n"
        f"Опис: {escape_markdown(ad['desc'], version=2)}"
    )

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Підтвердити", callback_data="confirm")],
        [InlineKeyboardButton("✏️ Категорія", callback_data="edit_category")],
        [InlineKeyboardButton("✏️ Місто",     callback_data="edit_city"),
         InlineKeyboardButton("✏️ Ціну",      callback_data="edit_price")],
        [InlineKeyboardButton("✏️ Опис",      callback_data="edit_desc"),
         InlineKeyboardButton("✏️ Фото",      callback_data="edit_photo")],
        [InlineKeyboardButton("🛑 Скасувати",  callback_data="cancel")],
    ])

    if ad.get('photo_id'):
        media = InputMediaPhoto(ad['photo_id'], caption=summary, parse_mode="HTML")
        if update.callback_query:
            try:
                await update.callback_query.edit_message_media(media=media, reply_markup=kb)
            except BadRequest:
                await update.effective_chat.send_photo(
                    photo=ad['photo_id'],
                    caption=summary,
                    parse_mode="HTML",
                    reply_markup=kb
                )
        else:
            await update.message.reply_photo(
                photo=ad['photo_id'],
                caption=summary,
                parse_mode="HTML",
                reply_markup=kb
            )
    else:
        if update.callback_query:
            try:
                await update.callback_query.edit_message_text(
                    summary,
                    parse_mode="HTML",
                    reply_markup=kb
                )
            except BadRequest:
                await update.effective_chat.send_message(
                    text=summary,
                    parse_mode="HTML",
                    reply_markup=kb
                )
        else:
            await update.message.reply_text(
                text=summary,
                parse_mode="HTML",
                reply_markup=kb
            )

    return CONFIRM

async def confirm_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    mapping = {
        "edit_category": EDIT_CATEGORY,
        "edit_city":     EDIT_CITY,
        "edit_price":    EDIT_PRICE,
        "edit_desc":     EDIT_DESC,
        "edit_photo":    EDIT_PHOTO,
        "cancel":        None,
    }

    if query.data in mapping and query.data != "confirm":
        if query.data == "cancel":
            await query.message.delete()
            await ctx.bot.send_message(
                chat_id=update.effective_chat.id,
                text="🛑 Створення оголошення перервано.",
            )
            await ctx.bot.send_message(
                chat_id=update.effective_chat.id,
                text="🏠 Вітаю! Ось головне меню:",
                reply_markup=main_menu()
            )
            return ConversationHandler.END

        if query.data == "edit_category":
            return await edit_category_start(update, ctx)

        await query.message.delete()
        return mapping[query.data]

    if query.data == "confirm":
        await query.message.delete()
        if 'id' in ctx.user_data:
            update_ad(ctx.user_data, ctx.user_data['id'])
            msg = "✅ Ваше оголошення оновлено!"
        else:
            save_ad(ctx.user_data, query.from_user.id)
            msg = "✅ Ваше оголошення розміщено!"
        await ctx.bot.send_message(chat_id=update.effective_chat.id, text=msg)
        await ctx.bot.send_message(
            chat_id=update.effective_chat.id,
            text="🏠 Вітаю! Ось головне меню:",
            reply_markup=main_menu()
        )
        return ConversationHandler.END

    return ConversationHandler.END

# ====== Хендлери для редагування ======
async def edit_category_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.message.delete()
    keyboard = [
        [InlineKeyboardButton(f"🖥️ {CATEGORY_LABELS['general']}", callback_data="cat_general")],
        [InlineKeyboardButton(f"🕵️ {CATEGORY_LABELS['search']}",       callback_data="cat_search")],
        [InlineKeyboardButton(f"💼 {CATEGORY_LABELS['other']}", callback_data="cat_other")],
        [InlineKeyboardButton("◀️ Повернутись", callback_data="back_to_summary")],
    ]
    
    await ctx.bot.send_message(
        chat_id=query.message.chat_id,
        text="Оберіть нову категорію:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return EDIT_CATEGORY

async def edit_category_received(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    category = query.data.split("_", 1)[1]
    ctx.user_data['category'] = category
    await query.message.delete()
    return await send_summary(update, ctx)

async def edit_city_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.message.delete()

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_summary")]
    ])

    await ctx.bot.send_message(
        chat_id=query.message.chat_id,
        text="Введіть нове місто:",
        reply_markup=kb
    )
    return EDIT_CITY

async def edit_city_received(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data['city'] = update.message.text
    return await send_summary(update, ctx)

async def edit_price_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.message.delete()

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_summary")]
    ])

    await ctx.bot.send_message(
        chat_id=query.message.chat_id,
        reply_markup=kb,
        text="Яка ціна ваших послуг?\n (Наприклад: 2000 грн., 2000₴, 100$ або діапазон 2000-3000₴)"
    )
    return EDIT_PRICE

async def edit_price_received(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        data = query.data
        if data.startswith("preset_"):
            ctx.user_data['price'] = data.split("_")[1]
            return await send_summary(update, ctx)
        else:
            await query.edit_message_text("Введіть свою ціну цифрами:")
            return EDIT_PRICE
    else:
        ctx.user_data['price'] = update.message.text
        return await send_summary(update, ctx)

async def edit_desc_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.message.delete()

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Назад", callback_data="back_to_summary")]
    ])

    await ctx.bot.send_message(
        chat_id=query.message.chat_id,
        reply_markup=kb,
        text="Введіть новий опис:"
    )
    return EDIT_DESC

async def edit_desc_received(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data['desc'] = update.message.text
    return await send_summary(update, ctx)

async def edit_photo_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.message.delete()
    kb = [[InlineKeyboardButton("Пропустити фото", callback_data="no_photo")]]
    await ctx.bot.send_message(
        chat_id=query.message.chat_id,
        reply_markup=InlineKeyboardMarkup(kb),
        text="Надішліть нове фото або натисніть «Пропустити фото»."
    )
    return EDIT_PHOTO

async def edit_photo_received(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query and update.callback_query.data == "no_photo":
        await update.callback_query.answer()
        ctx.user_data['photo_id'] = None
    else:
        ctx.user_data['photo_id'] = update.message.photo[-1].file_id
    return await send_summary(update, ctx)

async def cancel_conversation(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await update.callback_query.answer()
        try:
            await update.callback_query.message.delete()
        except Exception as e:
            print(f"Не вдалося видалити повідомлення: {e}")
        
        if 'id' in ctx.user_data:
            msg = "🛑 Редагування оголошення перервано."
        else:
            msg = "🛑 Створення оголошення перервано."
        await ctx.bot.send_message(
            chat_id=update.callback_query.message.chat.id,
            text=msg
        )
    else:
        
        if 'id' in ctx.user_data:
            await update.message.reply_text("🛑 Редагування оголошення перервано.")
        else:
            await update.message.reply_text("🛑 Створення оголошення перервано.")
    
    await start(update, ctx)
    return ConversationHandler.END

# ====== Перегляд оголошень ======
async def view_ads_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    _, _, category = query.data.split("_", 2)
    ctx.user_data['ads_category'] = category

    label = CATEGORY_LABELS.get(category, "Оголошення")
    header = f"🔍 Меню перегляду: {label}"

    keyboard = [
        [InlineKeyboardButton(
            "⭐ Популярні оголошення",
            callback_data=f"menu_top_ads_{category}"
        )],
        [InlineKeyboardButton(
            "🏙 Популярні міста",
            callback_data=f"menu_top_cities_{category}"
        )],
        [InlineKeyboardButton(
            "🔍 Пошук за містом",
            switch_inline_query_current_chat=""
        )],
        [InlineKeyboardButton(
            "📄 Всі оголошення",
            callback_data=f"menu_all_ads_{category}"
        )],
        [InlineKeyboardButton(
            text="🔔 Підписка на категорію",
            callback_data=f"menu_cat_{category}_view_ads"
        )],
        [InlineKeyboardButton(
            "🔙 До головного меню",
            callback_data="back"
        )],
    ]

    await safe_update(
        update,
        new_text=header,
        new_markup=InlineKeyboardMarkup(keyboard)
    )

async def show_ad_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data  # "show_ad_42|top_ads|3|general"
    parts = data.split("|")
    if len(parts) != 4:
        return await ctx.bot.send_message(
            chat_id=query.from_user.id,
            text=f"❌ Невідомий формат callback_data. {data}"
        )

    ad_id_part, origin, page_str, category = parts
    try:
        ad_id = int(ad_id_part.split("_", 2)[2])
        page  = int(page_str)
    except ValueError:
        return await query.edit_message_text("❌ Невірний формат id або сторінки.")

    ctx.user_data['_caller_id'] = query.from_user.id
    ctx.user_data['current_ad_id']  = ad_id
    ctx.user_data['current_origin'] = origin
    ctx.user_data['current_page']   = page
    ctx.user_data['ads_category'] = category
    
    return await display_ad(
        ad_id=ad_id,
        origin=origin,
        page=page,
        category=category,
        ctx=ctx,
        chat_id=query.message.chat.id,
        reply_to_message_id=query.message.message_id
    )

async def all_ads_handler(update, ctx):
    query = update.callback_query
    await query.answer()
    data = query.data  # "menu_all_ads_general" або "all_ads_general_3"

    parts = data.split("_")
    if parts[0] == "menu" and parts[1] == "all" and parts[2] == "ads":
        page = 1
        category = parts[3]
    elif parts[0] == "all" and parts[1] == "ads":
        category = parts[2]
        page     = int(parts[3])
    else:
        return await query.edit_message_text("❌ Невідомий формат callback_data.")

    ctx.user_data['ads_category'] = category

    ads = fetch_ads(category=category)
    if not ads:
        return await safe_update(update, new_text="📭 Поки немає оголошень у цій категорії")

    total_pages = (len(ads) + PAGE_SIZE - 1) // PAGE_SIZE

    kb = paginate_keyboard(
        items=ads,
        page=page,
        page_size=PAGE_SIZE,
        nav_size=NAV_SIZE,
        label_fn=lambda ad: f"{ad.get('bot_username')} — {ad['city']} — {ad['price']} — {ad['avg_rating']} ⭐",
        callback_fn=lambda ad: f"show_ad_{ad['id']}|all_ads|{page}|{category}",
        page_callback_prefix=f"all_ads_{category}",
        back_button=InlineKeyboardButton(
            "🔙 Назад",
            callback_data=f"view_ads_{category}"
        )
    )

    title = f"📄 Всі оголошення в категорії <b>{CATEGORY_LABELS[category]}</b>, стор. {page}/{total_pages}"
    await safe_update(update, new_text=title, new_markup=kb)

async def menu_top_cities_handler(update, ctx):
    query = update.callback_query
    await query.answer()

    data = query.data # "menu_top_cities_general" або "top_cities_general_3"
    parts = data.split("_")
    if parts[0] == "menu" and parts[1] == "top" and parts[2] == "cities":
        page = 1
        category = parts[3]
    elif parts[0] == "top" and parts[1] == "cities":
        category = parts[2]
        page     = int(parts[3])
    else:
        return await query.edit_message_text("❌ Невідомий формат callback_data.")

    ctx.user_data['ads_category'] = category
    cities = fetch_top_cities_list(category=category)
    if not cities:
        return await safe_update(update, new_text="🏙 Даних немає.")
    total_pages = (len(cities) + PAGE_SIZE - 1) // PAGE_SIZE

    kb = paginate_keyboard(
        items=cities,
        page=page,
        page_size=PAGE_SIZE,
        nav_size=NAV_SIZE,
        label_fn=lambda r: f"{r['city']} — {r['cnt']}",
        callback_fn=lambda r: f"city_{r['city']}_{category}_{page}",
        page_callback_prefix=f"top_cities_{category}",
        back_button=InlineKeyboardButton("🔙 Назад", callback_data=f"view_ads_{category}")
    )

    title = f"🏙 Популярні міста в категорії <b>{CATEGORY_LABELS[category]}</b>, стор. {page}/{total_pages}"

    await safe_update(update, new_text=title, new_markup=kb)

async def menu_top_ads_handler(update, ctx):
    query = update.callback_query
    await query.answer()

    data = query.data  # "menu_top_ads_general" або "top_ads_general_3"
    parts = data.split("_")
    if parts[0] == "menu" and parts[1] == "top" and parts[2] == "ads":
        page = 1
        category = parts[3]
    elif parts[0] == "top" and parts[1] == "ads":
        category = parts[2]
        page = int(parts[3])
    else:
        return await query.edit_message_text("❌ Невідомий формат callback_data.")

    ctx.user_data['ads_category'] = category

    ads = fetch_top_ads_list(category=category)
    if not ads:
        return await safe_update(update, new_text="⭐ Даних немає в цій категорії.")
    total_pages = (len(ads) + PAGE_SIZE - 1) // PAGE_SIZE

    kb = paginate_keyboard(
        items=ads,
        page=page,
        page_size=PAGE_SIZE,
        nav_size=NAV_SIZE,
        label_fn=lambda ad: f"{ad.get('bot_username')} — {ad['city']} — {ad['price']} — {ad['avg_rating']} ⭐",
        callback_fn=lambda ad: f"show_ad_{ad['id']}|top_ads|{page}|{category}",
        page_callback_prefix=f"top_ads_{category}",
        back_button=InlineKeyboardButton(
            "🔙 Назад",
            callback_data=f"view_ads_{category}"
        )
    )

    title = f"⭐ Популярні оголошення в категорії <b>{CATEGORY_LABELS[category]}</b>, стор. {page}/{total_pages}"
    await safe_update(update, new_text=title, new_markup=kb)

async def reviews_about_user_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data  
    m = re.match(r"^reviews_about_user_(\d+)_([^_]+)_(\d+)$", data)
    if not m:
        ad_id    = ctx.user_data.get('current_ad_id')
        origin   = ctx.user_data.get('current_origin')
        origpage = ctx.user_data.get('current_page')
        cat      = ctx.user_data.get('ads_category')
        back_cb  = f"show_ad_{ad_id}|{origin}|{origpage}|{cat}"
        await query.message.delete()
        return await ctx.bot.send_message(
            chat_id=update.effective_chat.id,
            text="❌ Невідомий формат callback_data.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 До оголошення", callback_data=back_cb)]]
            )
        )

    target_id = int(m.group(1))
    category  = m.group(2)
    page      = int(m.group(3) or 1)

    ad_id    = ctx.user_data.get('current_ad_id')
    origin   = ctx.user_data.get('current_origin')
    origpage = ctx.user_data.get('current_page')
    back_cb  = f"show_ad_{ad_id}|{origin}|{origpage}|{category}"

    reviews = fetch_reviews_for_user(target_id)
    if not reviews:
        return await ctx.bot.send_message(
            chat_id=update.effective_chat.id,
            text="📭 Цей виконавець ще не отримував відгуків.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("🔙 До оголошення", callback_data=back_cb)]]
            )
        )

    count = len(reviews)
    avg   = sum(r['rating'] for r in reviews) / count

    with_text    = sorted([r for r in reviews if r['comment']], key=lambda r: r['created_at'], reverse=True)
    without_text = sorted([r for r in reviews if not r['comment']], key=lambda r: r['created_at'], reverse=True)
    sorted_reviews = with_text + without_text
    total_pages = (count + PAGE_SIZE - 1) // PAGE_SIZE

    kb = paginate_keyboard(
        items=sorted_reviews,
        page=page,
        page_size=PAGE_SIZE,
        nav_size=NAV_SIZE,
        label_fn=lambda r: (
            f"{r['author']['bot_username']} — {r['rating']}⭐️" +
            (f" — {r['comment']}" if r['comment'] else "")
        ),
        callback_fn=lambda r: f"show_review_{r['id']}|reviews_about_user_{target_id}|{category}|{page}",
        page_callback_prefix=f"reviews_about_user_{target_id}_{category}",
        back_button=InlineKeyboardButton("🔙 До оголошення", callback_data=back_cb)
    )

    author = fetch_user_by_id(target_id)
    name   = html.escape(author.get('bot_username'))
    text = (
        f"<b>Виконавець:</b> {name}\n"
        f"<b>Кількість відгуків:</b> {count}\n"
        f"<b>Середній рейтинг:</b> {avg:.2f}⭐️\n\n"
        f"Оберіть відгук для перегляду (стор. {page}/{total_pages}):"
    )

    await safe_update(
        update,
        new_text=text,
        new_markup=kb
    )

async def noop_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()

async def apply_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    ad_id = int(query.data.split("_", 1)[1])
    requester_id = query.from_user.id

    ad = fetch_ad_by_id(ad_id)
    executor_id = ad['author']['id']

    app_id = save_application(ad_id, requester_id, executor_id)
    requester = fetch_user_by_id(requester_id)
    requester_bot_username = requester.get('bot_username')
    
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Прийняти", callback_data=f"accept_{app_id}"),
            InlineKeyboardButton("❌ Відхилити", callback_data=f"reject_{app_id}")
        ]
    ])
    await ctx.bot.send_message(
        chat_id=executor_id,
        text=(
            f"📬 Користувач {requester_bot_username} відгукнувся на ваше оголошення №{ad_id}.\n"
            f"Натисніть \"Прийняти\", щоб обмінятися контактами або \"Відхилити\", щоб відхилити зявку."
        ),
        parse_mode="HTML",
        reply_markup=kb
    )
    ad_id    = ctx.user_data.get('current_ad_id')
    origin   = ctx.user_data.get('current_origin')
    origpage = ctx.user_data.get('current_page')
    category = ctx.user_data.get('ads_category')
    back_cb  = f"show_ad_{ad_id}|{origin}|{origpage}|{category}"
    
    await query.edit_message_reply_markup(
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⏳ Заявка відправлена – чекайте рішення виконавця", callback_data="noop")],
            [InlineKeyboardButton("🔙 Назад", callback_data=back_cb)],
        ])
    )

async def accept_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    app_id = int(query.data.split("_", 1)[1])

    application = fetch_application(app_id)
    if not application:
        return await query.edit_message_text("❌ Заявка не знайдена.")
    if application['status'] != 'pending':
        return await query.answer("Ця заявка вже оброблена.", show_alert=True)

    update_application_status(app_id, 'accepted')

    requester_id = application['requester_id']
    executor_id  = application['executor_id']
    ad_id        = application['ad_id']
    client_button_sent   = True
    executor_button_sent = True

    try:
        kb_client = InlineKeyboardMarkup([[
            InlineKeyboardButton("✉️ Написати виконавцю", url=f"tg://user?id={executor_id}")
        ]])
        await ctx.bot.send_message(
            chat_id=requester_id,
            text=(
                f"✅ Ваша заявка на оголошення #{ad_id} ПРИЙНЯТА!\n\n"
                "Тепер ви можете написати виконавцю:"
            ),
            reply_markup=kb_client
        )
    except BadRequest:
        client_button_sent = False

    try:
        kb_executor = InlineKeyboardMarkup([[
            InlineKeyboardButton("✉️ Написати клієнту", url=f"tg://user?id={requester_id}")
        ]])
        await ctx.bot.send_message(
            chat_id=executor_id,
            text=(
                f"📬 У вас нова прийнята заявка на оголошення #{ad_id}!\n\n"
                "Тепер ви можете написати клієнту:"
            ),
            reply_markup=kb_executor
        )
    except BadRequest:
        executor_button_sent = False

    if not client_button_sent and executor_button_sent:
        await ctx.bot.send_message(
            chat_id=requester_id,
            text=(
                f"✅ Ваша заявка на оголошення #{ad_id} ПРИЙНЯТА!\n\n"
                "Чекайте повідомлення від виконавця.\n\n"
            )
        )
        await ctx.bot.send_message(
            chat_id=executor_id,
            text=(
                f"Увага! Клієнту не надійшов ваш контакт через те, що ви не маєте @username в Telegram\n"
                "🔔 Щоб клієнт міг вам відповісти прямо з боту, створіть собі @username у налаштуваннях Telegram."
            )
        )
    
    if not executor_button_sent and client_button_sent:
        await ctx.bot.send_message(
            chat_id=executor_id,
            text=(
                f"📬 У вас нова прийнята заявка на оголошення #{ad_id}!\n\n"
                "Чекайте повідомлення від клієнта.\n\n"
            )
        )
        await ctx.bot.send_message(
            chat_id=requester_id,
            text=(
                f"Увага! Виконавцю не надійшов ваш контакт через те, що ви не маєте @username в Telegram\n"
                "🔔 Щоб клієнт міг вам відповісти прямо з боту, створіть собі @username у налаштуваннях Telegram."
            )
        )
    
    if not client_button_sent and not executor_button_sent:
        advice = (
            "🔔 Увага: щоб можна було обмінюватися контактами через бота, "
            "зробіть собі @username у налаштуваннях Telegram і повторіть спробу взаємодії"
        )
        await ctx.bot.send_message(chat_id=requester_id, text=advice)
        await ctx.bot.send_message(chat_id=executor_id, text=advice)

    await query.edit_message_text("✅ Ви прийняли заявку.")

async def reject_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    app_id = int(query.data.split("_",1)[1])

    application = fetch_application(app_id)
    if not application:
        return await query.edit_message_text("❌ Заявка не знайдена.")
    if application['status'] != 'pending':
        return await query.answer("Цю заявку вже обробили.", show_alert=True)

    update_application_status(app_id, 'rejected')

    requester_id = application['requester_id']
    await ctx.bot.send_message(
        chat_id=requester_id,
        text=f"❌ Вашу заявку на оголошення #{application['ad_id']} ВІДХИЛЕНО."
    )
    await query.edit_message_text("❌ Ви відхилили заявку.")

async def display_ad(
    ad_id: int,
    origin: str,
    page: int,
    category: str,
    ctx: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    reply_to_message_id: int | None = None
):
    ad = fetch_ad_by_id(ad_id)
    if not ad:
        return await ctx.bot.send_message(chat_id=chat_id, text="❌ Оголошення не знайдено.")

    author = ad['author']
    esc_author   = escape_markdown(author['bot_username'], version=2)
    esc_cat      = escape_markdown(CATEGORY_LABELS[ad['category']], version=2)
    esc_rating   = escape_markdown(f"{author['avg_rating']:.2f}", version=2)
    esc_city     = escape_markdown(ad['city'], version=2)
    esc_price    = escape_markdown(ad['price'], version=2)
    esc_desc     = escape_markdown(ad['desc'], version=2)

    caption = (
        f"Оголошення №{ad_id}:\n"
        f"👤 Виконавець: {esc_author}\n"
        f"⭐️ Середній рейтинг виконавця: {esc_rating} ⭐️\n"
        f"📂 Категорія: {esc_cat}\n"
        f"📍 Місто: {esc_city}\n"
        f"💰 Ціна: {esc_price}\n\n"
        f"📝 Опис:\n{esc_desc}"
    )

    payload = f"show_ad_{ad_id}|{origin}|{page}|{category}"
    b64     = base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")
    bot_u   = ctx.bot.username
    bot_link = f"https://t.me/{bot_u}?start={b64}"
    short = ad['desc'][:47] + "..." if len(ad['desc']) > 50 else ad['desc']
    share_text = (
        f"📍 {ad['city']}\n"
        f"💰 {ad['price']}\n"
        f"📝 {short}\n\n"
        f"Переглянути в боті: {bot_link}"
    )
    encoded = quote(share_text, safe=':/?&=')
    kb = []
    ctx.user_data['current_ad_id']  = ad_id
    ctx.user_data['current_origin'] = origin
    ctx.user_data['current_page']   = page
    ctx.user_data['ads_category'] = category
    me = ctx.bot.id
    user = ctx.user_data.get('_caller_id') or chat_id
    if user == author['id']:
        kb.append([InlineKeyboardButton("✏️ Редагувати", callback_data=f"edit_ad_{ad_id}")])
        kb.append([InlineKeyboardButton("🗑 Видалити",  callback_data=f"delete_ad_{ad_id}")])
    else:
        if not has_pending_application(ad_id, user):
            kb.append([InlineKeyboardButton("📥 Відгукнутися", callback_data=f"apply_{ad_id}")])
        else:
            kb.append([InlineKeyboardButton("✅ Ви вже відгукнулися", callback_data="noop")])
        
        kb.append([InlineKeyboardButton(
            "💬 Залишити відгук",
            callback_data=f"review_ad_{ad_id}_{author['id']}"
        ),InlineKeyboardButton(
            "💬 Переглянути відгуки",
            callback_data=f"reviews_about_user_{author['id']}_{category}_{page}"
        )])
        kb.append([InlineKeyboardButton(
            "🔔 Підписка на автора",
            callback_data=f"menu_user_{author['id']}_show_ad"
        ),
            InlineKeyboardButton(
            "🔗 Поділитись",
            url=f"https://t.me/share/url?url={encoded}"
        )])
        
    if origin and category and page:
        back_cb = f"{origin}_{category}_{page}"
        kb.append([InlineKeyboardButton("🔙 До списку оголошень", callback_data=back_cb)])
    else:
        back_cb = "back"
        kb.append([InlineKeyboardButton("🏠 Головне меню", callback_data=back_cb)])

    markup = InlineKeyboardMarkup(kb)

    if reply_to_message_id:
        try:
            return await ctx.bot.edit_message_media(
                media=InputMediaPhoto(ad.get('photo_id'), caption=caption, parse_mode="MarkdownV2")
                if ad.get('photo_id') else None,
                chat_id=chat_id,
                message_id=reply_to_message_id,
                reply_markup=markup
            )
        except BadRequest:
            try:
                return await ctx.bot.edit_message_text(
                    text=caption,
                    chat_id=chat_id,
                    message_id=reply_to_message_id,
                    parse_mode="MarkdownV2",
                    reply_markup=markup
                )
            except BadRequest:
                pass

    if ad.get('photo_id'):
        return await ctx.bot.send_photo(
            chat_id=chat_id,
            photo=ad['photo_id'],
            caption=caption,
            parse_mode="MarkdownV2",
            reply_markup=markup
        )
    else:
        return await ctx.bot.send_message(
            chat_id=chat_id,
            text=caption,
            parse_mode="MarkdownV2",
            reply_markup=markup
        )

async def category_subscription_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        _, rest = query.data.split("menu_cat_", 1)
        category, origin = rest.split("_", 1)
    except ValueError:
        return await query.answer("❌ Невірний формат даних.", show_alert=True)

    user_id = query.from_user.id

    conn = psycopg2.connect(**DB_PARAMS, cursor_factory=RealDictCursor)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1
                      FROM category_subscriptions
                     WHERE subscriber_id = %s
                       AND category      = %s
                """, (user_id, category))
                subscribed = cur.fetchone() is not None
    finally:
        conn.close()

    if subscribed:
        text = (
            f"🔔 Ви <b>підписані</b> на категорію «{CATEGORY_LABELS[category]}».\n\n"
            "Вам надходитимуть повідомлення про нові оголошення у цій категорії."
        )
        action_btn = InlineKeyboardButton("🔕 Відписатися", callback_data=f"unsub_cat_{category}_{origin}")
    else:
        text = (
            f"🔕 Ви <b>не підписані</b> на категорію «{CATEGORY_LABELS[category]}».\n\n"
            "У разі підписки ви отримуватимете сповіщення про нові оголошення у цій категорії."
        )
        action_btn = InlineKeyboardButton("🔔 Підписатися", callback_data=f"sub_cat_{category}_{origin}")

    if origin == "my_subs":
        back_cb = "my_subs"
        back_btn = InlineKeyboardButton("🔙 Назад до підписок", callback_data=back_cb)
    else:
        back_cb = f"view_ads_{category}"
        back_btn = InlineKeyboardButton("🔙 Назад до оголошень", callback_data=back_cb)

    kb = InlineKeyboardMarkup([
        [action_btn],
        [back_btn]
    ])

    await query.edit_message_text(
        text=text,
        parse_mode="HTML",
        reply_markup=kb
    )

async def user_subscription_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        _, rest = query.data.split("menu_user_", 1)
        author_id_str, origin = rest.split("_", 1)
        author_id = int(author_id_str)
    except Exception:
        return await query.answer("❌ Невірний формат даних.", show_alert=True)

    subscriber_id = query.from_user.id

    author = fetch_user_by_id(author_id)
    if not author:
        return await query.edit_message_text("❌ Автор не знайдений.")
    bot_username = author.get("bot_username")
    safe_label   = html.escape(bot_username)

    conn = psycopg2.connect(**DB_PARAMS, cursor_factory=RealDictCursor)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1
                      FROM user_subscriptions
                     WHERE subscriber_id = %s
                       AND author_id     = %s
                """, (subscriber_id, author_id))
                subscribed = cur.fetchone() is not None
    finally:
        conn.close()

    if subscribed:
        text = (
            f"🔔 Ви <b>підписані</b> на автора: <i>{safe_label}</i>.\n\n"
            "Вам надходитимуть сповіщення про нові оголошення від цього автора."
        )
        action_btn = InlineKeyboardButton(
            "🔕 Відписатися", callback_data=f"unsub_user_{author_id}_{origin}"
        )
    else:
        text = (
            f"🔕 Ви <b>не підписані</b> на автора: <i>{safe_label}</i>.\n\n"
            "У разі підписки ви отримуватимете сповіщення про нові оголошення від цього автора."
        )
        action_btn = InlineKeyboardButton(
            "🔔 Підписатися", callback_data=f"sub_user_{author_id}_{origin}"
        )

    if origin == "my_subs":
        back_cb = "my_subs"
    else:
        ad_id    = ctx.user_data.get('current_ad_id')
        page     = ctx.user_data.get('current_page')
        category = ctx.user_data.get('ads_category')
        origin = ctx.user_data.get('current_origin')
        back_cb  = f"show_ad_{ad_id}|{origin}|{page}|{category}"

    back_btn = InlineKeyboardButton("🔙 Назад", callback_data=back_cb)
    kb = InlineKeyboardMarkup([[action_btn], [back_btn]])

    await safe_update(update, new_text=text, new_markup=kb)
    
async def subscribe_category_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        _, rest = query.data.split("sub_cat_", 1)
        category, origin = rest.split("_", 1)
    except Exception:
        return await query.answer("❌ Невірний формат даних.", show_alert=True)

    user_id = query.from_user.id

    conn = psycopg2.connect(**DB_PARAMS)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO category_subscriptions(subscriber_id, category)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (user_id, category))
    finally:
        conn.close()

    await query.edit_message_reply_markup(
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Підписані", callback_data="noop")]])
    )

    if origin == "my_subs":
        back_cb = "my_subs"
        back_btn = InlineKeyboardButton("🔙 Назад до підписок", callback_data=back_cb)
    else:
        back_cb = f"view_ads_{category}"
        back_btn = InlineKeyboardButton("🔙 Назад до оголошень", callback_data=back_cb)

    kb = InlineKeyboardMarkup([[back_btn]])

    await ctx.bot.send_message(
        chat_id=user_id,
        text=f"✅ Ви успішно підписалися на категорію «{CATEGORY_LABELS[category]}».",
        reply_markup=kb
    )

async def subscribe_user_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        _, rest = query.data.split("sub_user_", 1)
        author_id_str, origin = rest.split("_", 1)
        author_id = int(author_id_str)
    except Exception:
        return await query.answer("❌ Невірний формат даних.", show_alert=True)

    subscriber_id = query.from_user.id
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO user_subscriptions(subscriber_id, author_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (subscriber_id, author_id))
    finally:
        conn.close()

    await query.edit_message_reply_markup(
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Підписані", callback_data="noop")]])
    )

    author = fetch_user_by_id(author_id)
    bot_username = author.get("bot_username") or author.get("username") or str(author_id)
    text = f"✅ Ви успішно підписалися на автора «{bot_username}»."

    if origin == "my_subs":
        back_cb = "my_subs"
        back_btn_text = "🔙 Назад до підписок"
    else:
        ad_id    = ctx.user_data.get('current_ad_id')
        page     = ctx.user_data.get('current_page')
        category = ctx.user_data.get('ads_category')
        origin = ctx.user_data.get('current_origin')
        back_cb = f"show_ad_{ad_id}|{origin}|{page}|{category}"
        back_btn_text = "🔙 Назад до оголошення"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton(back_btn_text, callback_data=back_cb)]])

    await ctx.bot.send_message(
        chat_id=subscriber_id,
        text=text,
        reply_markup=kb
    )

async def unsubscribe_category_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        _, rest = query.data.split("unsub_cat_", 1)
        category, origin = rest.split("_", 1)
    except Exception:
        return await query.answer("❌ Невірний формат даних.", show_alert=True)

    user_id = query.from_user.id
    conn = psycopg2.connect(**DB_PARAMS)

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM category_subscriptions
                     WHERE subscriber_id = %s
                       AND category      = %s
                """, (user_id, category))
    finally:
        conn.close()

    await query.edit_message_reply_markup(
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔕 Відписано", callback_data="noop")]])
    )

    if origin == "my_subs":
        back_cb = "my_subs"
        back_btn = InlineKeyboardButton("🔙 Назад до підписок", callback_data=back_cb)
    else:
        back_cb = f"view_ads_{category}"
        back_btn = InlineKeyboardButton("🔙 Назад до оголошень", callback_data=back_cb)

    kb = InlineKeyboardMarkup([[back_btn]])

    await ctx.bot.send_message(
        chat_id=user_id,
        text=f"🔕 Ви відписалися від категорії «{CATEGORY_LABELS[category]}».",
        reply_markup=kb
    )

async def unsubscribe_user_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        _, rest = query.data.split("unsub_user_", 1)
        author_id_str, origin = rest.split("_", 1)
        author_id = int(author_id_str)
    except Exception:
        return await query.answer("❌ Невірний формат даних.", show_alert=True)

    subscriber_id = query.from_user.id

    conn = psycopg2.connect(**DB_PARAMS)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM user_subscriptions
                     WHERE subscriber_id = %s
                       AND author_id     = %s
                """, (subscriber_id, author_id))
    finally:
        conn.close()

    await query.edit_message_reply_markup(
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔕 Відписано", callback_data="noop")]])
    )

    author = fetch_user_by_id(author_id)
    bot_username = author.get("bot_username") or author.get("username") or str(author_id)
    text = f"🔕 Ви відписалися від автора «{bot_username}»."

    if origin == "my_subs":
        back_cb = "my_subs"
        back_btn_text = "🔙 Назад до підписок"
    else:
        ad_id    = ctx.user_data.get('current_ad_id')
        page     = ctx.user_data.get('current_page')
        category = ctx.user_data.get('ads_category')
        origin = ctx.user_data.get('current_origin')
        back_cb = f"show_ad_{ad_id}|{origin}|{page}|{category}"
        back_btn_text = "🔙 Назад до оголошення"

    kb = InlineKeyboardMarkup([[InlineKeyboardButton(back_btn_text, callback_data=back_cb)]])

    await ctx.bot.send_message(
        chat_id=subscriber_id,
        text=text,
        reply_markup=kb
    )

async def back_to_main_handler(update, ctx):
    query = update.callback_query
    await query.answer()
    await safe_update(
        update,
        new_text="🏠 Вітаю! Ось головне меню:",
        new_markup=main_menu()
    )
# ====== Основний бот на меню ======

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    save_user(update.effective_user)

    if update.callback_query:
        chat_id = update.callback_query.message.chat.id
        message_id = update.callback_query.message.message_id
        await update.callback_query.answer()
    else:
        chat_id = update.effective_chat.id
        message_id = None

    if context.args:
        b64 = context.args[0]
        pad = len(b64) % 4
        if pad:
            b64 += "=" * (4 - pad)
        try:
            raw = base64.urlsafe_b64decode(b64.encode()).decode()
            parts = raw.split("|")
            ad_id    = int(parts[0].split("_", 2)[2])
            origin   = parts[1]
            page     = int(parts[2])
            category = parts[3]
            return await display_ad(
                ad_id=ad_id,
                origin=origin,
                page=page,
                category=category,
                ctx=context,
                chat_id=chat_id,
                reply_to_message_id=message_id
            )
        except Exception as e:
            logger.warning(f"[start] Не вдалося розпакувати deep‑link payload: {b64!r}, помилка: {e}")

    await context.bot.send_message(
        chat_id=chat_id,
        text="🏠 Вітаю! Ось головне меню:",
        reply_markup=main_menu()
    )

# ====== Особистий кабінет ======
async def account_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    user = fetch_user_by_id(user_id)
    name = html.escape(user.get('bot_username'))
    quota = user.get('ad_quota', 3)
    ads = fetch_ads_by_user(user_id)
    ads_count = len(ads)

    my_reviews = fetch_reviews_by_author(user_id)
    my_reviews_count = len(my_reviews)
    
    reviews_about = fetch_reviews_for_user(user_id)
    about_count = len(reviews_about)
    avg = (sum(r['rating'] for r in reviews_about) / about_count) if about_count else 0.0

    stats_text = (
        f"📊 <b>Ваша статистика, {name}</b>\n\n"
        f"• Оголошень: {ads_count}/{quota}\n"
        f"• Власних відгуків: {my_reviews_count}\n"
        f"• Відгуків про вас: {about_count}\n"
        f"• Середній рейтинг: {avg:.2f}⭐️"
    )

    keyboard = [
        [InlineKeyboardButton(f"📄 Мої оголошення {ads_count}/{quota}", callback_data="my_ads")],
        [InlineKeyboardButton("✍️ Мої відгуки",       callback_data="my_reviews"), InlineKeyboardButton("💬 Відгуки про мене",  callback_data="reviews_about")],
        [InlineKeyboardButton("📥 Мої заявки",  callback_data="my_apps"), InlineKeyboardButton("🔔 Мої підписки",  callback_data="my_subs")],
        [InlineKeyboardButton("✏️ Змінити нікнейм в боті",  callback_data="change_nick_start")],
        [InlineKeyboardButton("🏠 Головне меню",     callback_data="back")],
    ]

    await query.edit_message_text(
        text=stats_text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def my_ads_handler(update, ctx):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data  
    page = int(data.split("_")[2]) if data.startswith("my_ads_") else 1

    ads = fetch_ads_by_user(user_id)
    if not ads:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Особистий кабінет", callback_data="account")]
        ])
        return await ctx.bot.send_message(
            chat_id=update.effective_chat.id,
            text="📭 Поки немає оголошень.",
            reply_markup=kb
        )
    
    total_pages = (len(ads) + PAGE_SIZE - 1) // PAGE_SIZE
    kb = paginate_keyboard(
        items=ads,
        page=page,
        page_size=PAGE_SIZE,
        nav_size=NAV_SIZE,
        label_fn=lambda ad: f"{ad['city']} — {ad['price']}",
        callback_fn=lambda ad: f"show_ad_{ad['id']}|my_ads|{page}|",
        page_callback_prefix="my_ads",
        back_button=InlineKeyboardButton("🔙 Назад", callback_data="account")
    )

    title = f"📄 Мої оголошення (стор. {page}/{total_pages})"

    await safe_update(update, new_text=title, new_markup=kb)

async def delete_ad_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        ad_id = int(query.data.rsplit("_", 1)[1])
    except ValueError:
        return await query.edit_message_text("❌ Невірний ідентифікатор оголошення.")

    ad = fetch_ad_by_id(ad_id)
    if not ad or ad['author']['id'] != query.from_user.id:
        return await query.edit_message_text("❌ Ви не маєте прав видалити це оголошення.")

    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    with conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ads WHERE id = %s", (ad_id,))
    conn.close()

    await update.callback_query.message.chat.send_message(
        text="✅ Оголошення успішно видалено."
    )
    return await my_ads_handler(update, ctx)

async def edit_ad_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    ad_id = int(query.data.rsplit("_", 1)[1])
    ad = fetch_ad_by_id(ad_id)
    if not ad or ad['author']['id'] != query.from_user.id:
        return await query.edit_message_text("❌ Ви не можете редагувати це оголошення.")

    ctx.user_data.update({
        'id': ad_id,
        'city': ad['city'],
        'price': ad['price'],
        'desc': ad['desc'],
        'photo_id': ad.get('photo_id'),
    })

    return await send_summary(update, ctx)

async def change_nick_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    user = fetch_user_by_id(user_id)
    last_changed = user.get('bot_username_changed_at')
    
    if last_changed is not None:
        last_changed = last_changed.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    if last_changed and (now - last_changed) < NICK_CHANGE_COOLDOWN:
        days_left = (NICK_CHANGE_COOLDOWN - (now - last_changed)).days
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Особистий кабінет", callback_data="account")]
        ])
        await ctx.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"⏳ Ви зможете змінити нікнейм лише через {days_left} днів.",
            reply_markup=kb
        )
        return ConversationHandler.END
    
    await query.edit_message_text(
        "Введіть, будь ласка, новий нікнейм (3–50 символів, дозволена латиниця та кирилиця, цифри та символ підкреслення \"_\" ):"
    )
    return CHANGE_NICK

async def change_nick_received(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    new_nick = update.message.text.strip()
    user_id = update.effective_user.id

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Відмінити", callback_data="nick_cancel")]
    ])

    if not RE_NICK.match(new_nick):
        await update.message.reply_text(
            "❌ Невірний формат. Дозволені латинські та кириличні літери, цифри, підкреслення та пробіл, 3–50 символів.\n"
            "Спробуйте ще раз або натисніть \"Відминити\"",
            reply_markup=kb
        )
        return CHANGE_NICK

    if bot_username_exists(new_nick):
        await update.message.reply_text(
            "❌ Цей нікнейм уже зайнято. Оберіть інший.",
            reply_markup=kb
        )
        return CHANGE_NICK
    
    kb1 = InlineKeyboardMarkup([
            [InlineKeyboardButton("Особистий кабінет", callback_data="account")]
        ])
    now = datetime.now(timezone.utc)
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE users
                   SET bot_username = %s,
                       bot_username_changed_at = %s
                 WHERE id = %s
            """, (new_nick, now, user_id))
    conn.close()

    await update.message.reply_text(f"✅ Ваш новий внутрішній нікнейм: {new_nick}", reply_markup=kb1)
    return ConversationHandler.END

async def change_nick_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Особистий кабінет", callback_data="account")]
        ])
    await ctx.bot.send_message(
        chat_id=update.effective_chat.id,
            text="🛑 Зміна нікнейму скасована.",
            reply_markup=kb
        )
    return ConversationHandler.END

async def my_apps_handler(update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    data = query.data
    if data.startswith("my_apps_"):
        page = int(data.split("_", 2)[2])
    else:
        page = 1

    apps = fetch_applications_for_requester(user_id)
    if not apps:
        return await ctx.bot.send_message(
            chat_id=update.effective_chat.id,
            text="📭 У вас поки що немає заявок.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад до кабінету", callback_data="account")
            ]])
        )

    total_pages = (len(apps) + PAGE_SIZE - 1) // PAGE_SIZE

    kb = paginate_keyboard(
        items=apps,
        page=page,
        page_size=PAGE_SIZE,
        nav_size=NAV_SIZE,
        label_fn=lambda a: (
            f"#{a['app_id']} [{APPS_LABELS.get(a['status'])}] — {a['city']} — {a['price']}"
        ),
        callback_fn=lambda a: f"show_app_{a['app_id']}|my_apps|{page}",
        page_callback_prefix="my_apps",
        back_button=InlineKeyboardButton("🔙 Назад", callback_data="account")
    )

    title = f"📥 Мої заявки (стор. {page}/{total_pages})"

    await safe_update(update, new_text=title, new_markup=kb)

async def show_app_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    parts = data.split("|")
    if len(parts) != 3 or not parts[0].startswith("show_app_"):
        return await query.edit_message_text("❌ Невідомий формат callback_data.")
    try:
        app_id = int(parts[0].split("_", 2)[2])
        page   = int(parts[2])
    except ValueError:
        return await query.edit_message_text("❌ Невірний формат id або сторінки.")

    app_row = fetch_application(app_id)
    if not app_row:
        return await query.edit_message_text("❌ Заявка не знайдена.")

    requester = fetch_user_by_id(app_row["requester_id"])
    executor  = fetch_user_by_id(app_row["executor_id"])
    ad         = fetch_ad_by_id(app_row["ad_id"])
    if not ad:
        return await query.edit_message_text("❌ Пов’язане оголошення не знайдене.")

    text = (
        f"<b>Заявка №{app_id}</b>\n"
        f"<b>Статус:</b> {html.escape(APPS_LABELS[app_row['status']])}\n"
        f"<b>Оголошення:</b> №{ad['id']} — {html.escape(ad['city'])}, {html.escape(ad['price'])}\n\n"
        f"<b>👤 Клієнт:</b> {html.escape(requester.get("bot_username"))}\n"
        f"<b>👤 Виконавець:</b> {html.escape(executor.get("bot_username"))}\n"
        f"<b>🕒 Створено:</b> {app_row['created_at'].strftime('%Y-%m-%d %H:%M')}"
    )

    back_btn = InlineKeyboardButton(
        "🔙 Назад до списку заявок",
        callback_data=f"my_apps_{page}"
    )
    kb = InlineKeyboardMarkup([[back_btn]])

    await query.edit_message_text(
        text=text,
        parse_mode="HTML",
        reply_markup=kb
    )

async def my_subs_handler(update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id

    data = query.data
    if data.startswith("my_subs_"):
        page = int(data.split("_",2)[2])
    else:
        page = 1

    users = fetch_user_subscriptions(user_id)
    cats  = fetch_category_subscriptions(user_id)

    items = []
    for u in users:
        items.append({"type":"user", "id":u["id"], "label":u["bot_username"]})
    for c in cats:
        items.append({"type":"cat", "category":c, "label":CATEGORY_LABELS[c]})

    if not items:
        return await query.edit_message_text(
            "📭 Ви ще ні на кого не підписані.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад до кабінету", callback_data="account")
            ]])
        )

    total_pages = (len(items) + PAGE_SIZE - 1)//PAGE_SIZE

    kb = paginate_keyboard(
        items=items,
        page=page,
        page_size=PAGE_SIZE,
        nav_size=NAV_SIZE,
        label_fn=lambda it: (
            f"👤 {it['label']}" if it["type"]=="user"
            else f"📂 {it['label']}"
        ),
        callback_fn=lambda it: (
            f"menu_user_{it['id']}_my_subs" if it["type"]=="user"
            else f"menu_cat_{it['category']}_my_subs"
        ),
        page_callback_prefix="my_subs",
        back_button=InlineKeyboardButton("🔙 Назад", callback_data="account")
    )

    header = f"🔔 Мої підписки (стор. {page}/{total_pages})"
    await safe_update(update, new_text=header, new_markup=kb)

# ====== Спільнота ======
async def community_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "👥 Приєднуйтесь до нашої спільноти: https://t.me/spilnota_poligraph",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
    )

# ====== Підтримка ======
async def support_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "🆘 Якщо виникли питання, пишіть до @Poligraph_Support\nРозробник: @danylo_cr",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="back")]])
    )

# ========== Пошук ==========

async def inline_city_suggest(update, ctx):
    query = update.inline_query.query.strip()
    results = []
    cat = ctx.user_data['ads_category']
    if len(query) < MIN_QUERY_LEN:
        top_cities = fetch_top_cities_list(category=cat)[:10]
        for item in top_cities:
            city = item['city']
            results.append(
                InlineQueryResultArticle(
                    id=str(uuid.uuid4()),
                    title=f"{city} ({item['cnt']})",
                    input_message_content=InputTextMessageContent(f"/city {city}")
                )
            )
    else:
        cities = fetch_distinct_cities(prefix=query, limit=10, category=cat)
        if not cities:
            results.append(
                InlineQueryResultArticle(
                    id=str(uuid.uuid4()),
                    title="Нічого не знайдено 😕",
                    input_message_content=InputTextMessageContent(
                        f"За запитом «{query}» нічого не знайдено в категорії {CATEGORY_LABELS[cat]}."
                    )
                )
            )
        else:
            for city in cities:
                results.append(
                    InlineQueryResultArticle(
                        id=str(uuid.uuid4()),
                        title=city,
                        input_message_content=InputTextMessageContent(f"/city {city}")
                    )
                )
    
    await update.inline_query.answer(results, cache_time=0)

async def city_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.message:
        if not ctx.args:
            return await update.message.reply_text("Будь ласка, вкажіть місто: /city Київ")
        city = " ".join(ctx.args).strip()
        page = 1
        category = ctx.user_data['ads_category']
    else:
        query = update.callback_query
        await query.answer()
        data = query.data 
        try:
            prefix, city_raw, category, page_str = data.split("_", 3)
            city = city_raw
            page = int(page_str)
        except ValueError:
            return await safe_update(update, new_text="❌ Неправильні дані пагінації.")

    ads = fetch_ads_by_city(city, category=category)
    if not ads:
        text = f"На жаль, в місті «{city}» поки що немає оголошень."
        if update.message:
            return await update.message.reply_text(text)
        else:
            return await safe_update(update, new_text=text)

    total_pages = (len(ads) + PAGE_SIZE - 1) // PAGE_SIZE

    prefix = f"city_{city.replace(' ', '_')}_{category}"
    kb = paginate_keyboard(
        items=ads,
        page=page,
        page_size=PAGE_SIZE,
        nav_size=NAV_SIZE,
        label_fn=lambda ad: f"{ad['price']} — {ad['author']['bot_username']} — {ad['author']['avg_rating']} ⭐",
        callback_fn=lambda ad: f"show_ad_{ad['id']}|city_{city}|{page}|{category}",
        page_callback_prefix=f"{prefix}",
        back_button=InlineKeyboardButton("🔙 Назад", callback_data=f"top_cities_{category}_{page}")
    )

    title = f"🔍 Оголошення в місті «{city}» в категорії {CATEGORY_LABELS[category]} (стор. {page}/{total_pages})"

    if update.message:
        await update.message.reply_text(title, reply_markup=kb)
    else:
        await safe_update(update, new_text=title, new_markup=kb)

#=========== Допоміжні функції ==========

def paginate_keyboard(
    items: list,
    page: int,
    page_size: int,
    nav_size: int,
    label_fn: Callable[[Any], str],
    callback_fn: Callable[[Any], str],
    page_callback_prefix: str,
    back_button: InlineKeyboardButton | None = None
) -> InlineKeyboardMarkup:
    total = len(items)
    total_pages = (total + page_size - 1) // page_size
    page = max(1, min(page, total_pages))

    start = (page - 1) * page_size
    end   = start + page_size
    slice_items = items[start:end]

    keyboard: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(label_fn(item), callback_data=callback_fn(item))]
        for item in slice_items
    ]

    half = nav_size // 2
    left = max(1, page - half)
    right = min(total_pages, left + nav_size - 1)
    left = max(1, right - nav_size + 1)

    nav_buttons: list[InlineKeyboardButton] = []
    for p in range(left, right+1):
        text = f"[{p}]" if p == page else str(p)
        nav_buttons.append(
            InlineKeyboardButton(text, callback_data=f"{page_callback_prefix}_{p}")
        )

    keyboard.append(nav_buttons)

    if back_button:
        keyboard.append([back_button])

    return InlineKeyboardMarkup(keyboard)

async def safe_update(update, new_text=None, new_markup=None):
    msg = update.callback_query.message

    if new_text is not None and msg.text is not None:
        try:
            await update.callback_query.edit_message_text(
                text=new_text,
                reply_markup=new_markup or msg.reply_markup,
                parse_mode="HTML"
            )
            return
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                raise

    await msg.delete()

    await update.callback_query.message.chat.send_message(
        text=new_text or msg.text or "",
        reply_markup=new_markup,
        parse_mode="HTML"
    )

#================== REVIEW ==============
async def review_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()

    m = re.match(r"^review_ad_(\d+)_(\d+)$", query.data)
    if not m:
        await query.edit_message_text("❌ Невірний формат callback_data.")
        return ConversationHandler.END

    ad_id = int(m.group(1))
    executor_id = int(m.group(2))
    requester_id = query.from_user.id

    ad = fetch_ad_by_id(ad_id)
    if not ad:
        await query.edit_message_text("❌ Це оголошення більше не існує.")
        return ConversationHandler.END

    accepted_count = count_accepted_applications(requester_id, executor_id)
    if accepted_count == 0:
        await ctx.bot.send_message(
            chat_id=query.message.chat_id,
            text="❌ Спочатку маєте мати прийняту заявку на це оголошення, щоб залишити відгук."
        )
        return ConversationHandler.END

    review_count = count_reviews_by_author_for_executor(requester_id, executor_id)
    if review_count >= accepted_count:
        await ctx.bot.send_message(
            chat_id=query.message.chat_id,
            text=(
                "❌ Ви вже залишили всі можливі відгуки для цього користувача.\n"
                f"У вас {accepted_count} успішних заявок ⇒ {review_count} відгуків."
            )
        )
        return ConversationHandler.END

    ctx.user_data['ad_id']     = ad_id
    ctx.user_data['author_id'] = requester_id
    ctx.user_data['target_id'] = executor_id
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(str(i), callback_data=str(i))
        for i in range(1, 6)
    ]])
    await safe_update(
        update,
        new_text="Оцініть взаємодію з даним користувачем:\n1 – погано, 5 – дуже добре",
        new_markup=kb
    )
    return REVIEW_RATING

async def rating_received(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    rating = int(query.data)
    ctx.user_data['rating'] = rating

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ Пропустити", callback_data="skip")]
    ])
    await query.edit_message_text(
        f"Ваша оцінка: {rating} ⭐\n\n"
        "Напишіть, будь ласка, коментар до відгуку (або натисніть «Пропустити»).",
        reply_markup=kb
    )
    return REVIEW_COMMENT

async def comment_skip(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    save_review({
        'author_id': ctx.user_data['author_id'],
        'target_id': ctx.user_data['target_id'],
        'ad_id': ctx.user_data['ad_id'],
        'rating': ctx.user_data['rating'],
        'comment': None
    })
    ad_id    = ctx.user_data.get('current_ad_id')
    origin   = ctx.user_data.get('current_origin')
    origpage = ctx.user_data.get('current_page')
    category = ctx.user_data.get('ads_category')

    if ad_exists(ad_id, category):
        back_cb = f"show_ad_{ad_id}|{origin}|{origpage}|{category}"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 До оголошення", callback_data=back_cb)]
        ])
    else:
        back_cb = "account"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 В особистий кабінет", callback_data=back_cb)]
        ])

    await query.edit_message_text("✅ Дякуємо! Ваш відгук збережено.", reply_markup=kb)
    return ConversationHandler.END

async def comment_received(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    ctx.user_data['comment'] = text

    save_review({
        'author_id': ctx.user_data['author_id'],
        'target_id': ctx.user_data['target_id'],
        'ad_id': ctx.user_data['ad_id'],
        'rating': ctx.user_data['rating'],
        'comment': text
    })
    ad_id    = ctx.user_data.get('current_ad_id')
    origin   = ctx.user_data.get('current_origin')
    origpage = ctx.user_data.get('current_page')
    category = ctx.user_data.get('ads_category')
    if ad_id and origin and origpage and category:
        back_cb = f"show_ad_{ad_id}|{origin}|{origpage}|{category}"
    else:
        back_cb = "account"

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Назад", callback_data=back_cb)]
    ])
    await update.message.reply_text("✅ Дякуємо! Ваш відгук збережено.", reply_markup=kb)
    
    return ConversationHandler.END

async def review_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("🛑 Відгук не збережено.")
    else:
        await update.message.reply_text("🛑 Відгук не збережено.")
    await ctx.bot.send_message(
        chat_id=update.effective_chat.id,
        text="🏠 Вітаю! Ось головне меню:",
        reply_markup=main_menu()
    )
    return ConversationHandler.END

async def my_reviews_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id

    data = query.data
    if data.startswith("my_reviews_"):
        try:
            page = int(data.split("_", 2)[2])
        except ValueError:
            page = 1
    else:
        page = 1

    reviews = fetch_reviews_by_author(user_id)
    if not reviews:
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("Особистий кабінет", callback_data="account")]
        ])
        return await ctx.bot.send_message(
            chat_id=update.effective_chat.id,
            text="📭 Ви ще не залишали жодного відгуку.",
            reply_markup=reply_markup
        )
    
    total_pages = (len(reviews) + PAGE_SIZE - 1) // PAGE_SIZE

    kb = paginate_keyboard(
        items=reviews,
        page=page,
        page_size=PAGE_SIZE,
        nav_size=NAV_SIZE,
        label_fn=lambda rev: (
            f"{rev['bot_username']} — {rev['rating']}⭐ " +
            (f" — {rev['comment']}" if rev['comment'] else "")
        ),
        callback_fn=lambda rev: f"show_review_{rev['id']}|my_reviews|{page}",
        page_callback_prefix="my_reviews",
        back_button=InlineKeyboardButton("🔙 Назад", callback_data="account")
    )

    title = f"✍️ Ваші відгуки (стор. {page}/{total_pages})"
    await safe_update(update, new_text=title, new_markup=kb)

async def reviews_about_me_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id

    data = query.data
    if data.startswith("reviews_about_"):
        try:
            page = int(data.split("_", 2)[2])
        except ValueError:
            page = 1
    else:
        page = 1

    reviews = fetch_reviews_for_user(user_id)
    if not reviews:
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("Особистий кабінет", callback_data="account")]
        ])
        return await ctx.bot.send_message(
            chat_id=update.effective_chat.id,
            text="📭 Ніхто ще не залишив відгуків про вас.",
            reply_markup=reply_markup
        )

    total_pages = (len(reviews) + PAGE_SIZE - 1) // PAGE_SIZE

    kb = paginate_keyboard(
        items=reviews,
        page=page,
        page_size=PAGE_SIZE,
        nav_size=NAV_SIZE,
        label_fn=lambda rev: (
            f"{rev['author']['bot_username']} — {rev['rating']}⭐" +
            (f" — {rev['comment']}" if rev['comment'] else "")
        ),
        callback_fn=lambda rev: f"show_review_{rev['id']}|reviews_about|{page}",
        page_callback_prefix="reviews_about",
        back_button=InlineKeyboardButton("🔙 Назад", callback_data="account")
    )

    title = f"💬 Відгуки про мене (стор. {page}/{total_pages})"
    await safe_update(update, new_text=title, new_markup=kb)

async def show_review_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    #   - "show_review_<id>|<origin>|<page>"
    #   - "show_review_<id>|<origin>|<category>|<page>"
    parts = query.data.split("|")
    if len(parts) not in (3, 4):
        return await query.edit_message_text("❌ Невідомий формат callback_data.")

    if len(parts) == 3:
        review_id_part = parts[0]
        origin_raw     = parts[1]
        page_str       = parts[2]
        category       = None
    else:    
        review_id_part = parts[0]
        origin_raw     = parts[1]
        category       = parts[2]
        page_str       = parts[3]

    try:
        review_id = int(review_id_part.rsplit("_", 1)[1])
        page      = int(page_str)
    except (IndexError, ValueError):
        return await query.edit_message_text("❌ Невірний формат ідентифікатора або сторінки.")

    review = fetch_review_by_id(review_id)
    if not review:
        return await query.edit_message_text("❌ Відгук не знайдено.")

    author_row = fetch_user_by_id(review['author_id'])
    target_info = review['target']
    target_row  = fetch_user_by_id(target_info['id'])

    author_name = html.escape(author_row.get('bot_username'))
    target_name = html.escape(target_row.get('bot_username'))
    created     = review['created_at'].strftime('%Y-%m-%d %H:%M')
    comment     = html.escape(review['comment'] or 'без коментаря')

    text = (
        f"<b>Відгук №{review['id']}</b>\n"
        f"👤 <b>Від кого:</b> {author_name}\n"
        f"👤 <b>Кому:</b>   {target_name}\n"
        f"⭐ <b>Рейтинг:</b> {review['rating']}/5\n"
        f"🕒 {created}\n\n"
        f"📝 <b>Коментар:</b>\n"
        f"{comment}"
    )

    if category:
        back_cb = f"{origin_raw}_{category}_{page}"
    else:
        back_cb = f"{origin_raw}_{page}"

    buttons = []
    if query.from_user.id == review['author_id']:
        buttons.append([
            InlineKeyboardButton("🗑 Видалити відгук", callback_data=f"delete_review_{review['id']}")
        ])
    buttons.append([
        InlineKeyboardButton("🔙 Назад", callback_data=back_cb)
    ])

    kb = InlineKeyboardMarkup(buttons)

    return await query.edit_message_text(
        text=text,
        parse_mode="HTML",
        reply_markup=kb
    )

async def delete_review_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        review_id = int(query.data.rsplit("_", 1)[1])
    except (IndexError, ValueError):
        return await query.edit_message_text("❌ Невірний формат ідентифікатора відгуку.")

    review = fetch_review_by_id(review_id)
    if not review:
        return await query.edit_message_text("❌ Відгук не знайдено.")
    if review['author_id'] != query.from_user.id:
        return await query.edit_message_text("❌ Ви не можете видалити цей відгук.")

    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM reviews WHERE id = %s", (review_id,))
    conn.close()

    await ctx.bot.send_message(
        chat_id=query.message.chat_id,
        text="✅ Відгук успішно видалено.",
    )
    
    return await my_reviews_handler(update, ctx)

# ============= Нагадування ================

async def send_due_reminders(context):
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, cursor_factory=RealDictCursor)
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, ad_id, requester_id, executor_id
                  FROM applications
                 WHERE status = 'accepted'
                   AND reminder_disabled = FALSE
                   AND reminder_scheduled_at <= NOW()
                   AND reminder_sent_at IS NULL
            """)
            apps = cur.fetchall()
            for a in apps:
                def make_buttons(app_id: int, target_id: int) -> InlineKeyboardMarkup:
                    return InlineKeyboardMarkup([
                        [InlineKeyboardButton("📝 Надати відгук", callback_data=f"review_ad_{app_id}_{target_id}")],
                        [InlineKeyboardButton("⏭ Нагадати пізніше", callback_data=f"snooze_app_{a['id']}")],
                        [InlineKeyboardButton("🚫 Більше не нагадувати", callback_data=f"cancel_reminder_{a['id']}")]
                    ])

                btns_requester = make_buttons(a['ad_id'], a['executor_id'])
                btns_executor = make_buttons(a['ad_id'], a['requester_id'])

                await context.bot.send_message(
                    chat_id=a['requester_id'],
                    text=(
                        f"✅ Ваша заявка на оголошення №{a['ad_id']} була прийнята вчора.\n"
                        "Будь ласка, залиште відгук про виконавця:"
                    ),
                    reply_markup=btns_requester
                )

                await context.bot.send_message(
                    chat_id=a['executor_id'],
                    text=(
                        f"👤 Ви прийняли заявку на оголошення №{a['ad_id']} вчора.\n"
                        "Будь ласка, залиште відгук про клієнта:"
                    ),
                    reply_markup=btns_executor
                )
                cur.execute(
                    "UPDATE applications SET reminder_sent_at = NOW() WHERE id = %s",
                    (a['id'],)
                )
    conn.close()

async def cancel_reminder_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    app_id = int(query.data.rsplit("_", 1)[1])

    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE applications
                   SET reminder_disabled = TRUE
                 WHERE id = %s
            """, (app_id,))
    conn.close()

    try:
        await query.message.delete()
    except Exception:
        pass

    await query.answer("Нагадування відключено ✅", show_alert=True)

async def snooze_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    app_id = int(query.data.rsplit("_", 1)[1])
    conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE applications
                   SET reminder_scheduled_at = NOW() + INTERVAL '1 minute',
                       reminder_sent_at       = NULL
                 WHERE id = %s
            """, (app_id,))
    conn.close()

    try:
        await query.message.delete()
    except Exception:
        pass

    await query.answer("Нагадую ще раз через добу ⏰", show_alert=True)

async def send_new_ads_notifications(context):
    conn = psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS,
        cursor_factory=RealDictCursor
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT last_run
                      FROM notification_state
                     WHERE name = 'ads'
                """)
                row = cur.fetchone()
                last_run = row['last_run']

                now = datetime.now(timezone.utc)

                cur.execute("""
                    SELECT
                      id, user_id, city, price, description, category
                    FROM ads
                   WHERE created_at > %s
                   ORDER BY created_at ASC
                """, (last_run,))
                new_ads = cur.fetchall()

                if not new_ads:
                    cur.execute("""
                        UPDATE notification_state
                           SET last_run = %s
                         WHERE name = 'ads'
                    """, (now,))
                    return

                for ad in new_ads:
                    ad_id     = ad['id']
                    category  = ad['category']
                    author_id = ad['user_id']
                    city      = ad['city']
                    price     = ad['price']
                    desc      = ad['description']
                    short_desc = desc[:50] + "…" if len(desc) > 50 else desc

                    cur.execute("""
                        SELECT subscriber_id
                          FROM category_subscriptions
                         WHERE category = %s
                    """, (category,))
                    cat_subs = {r['subscriber_id'] for r in cur.fetchall()}

                    cur.execute("""
                        SELECT subscriber_id
                          FROM user_subscriptions
                         WHERE author_id = %s
                    """, (author_id,))
                    user_subs = {r['subscriber_id'] for r in cur.fetchall()}

                    targets = (cat_subs | user_subs) - {author_id}

                    if not targets:
                        continue

                    text = (
                        f"🔔 <b>Нове оголошення №{ad_id}</b>\n"
                        f"Категорія: <i>{CATEGORY_LABELS[category]}</i>\n"
                        f"📍 {city}\n"
                        f"💰 {price}\n"
                        f"📝 {short_desc}"
                    )

                    raw = f"show_ad_{ad_id}|all_ads|1|{category}"
                    b64 = base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")
                    bot_username = context.bot.username
                    link = f"https://t.me/{bot_username}?start={b64}"
                    text += f"\n\n👀 <a href=\"{link}\">Переглянути оголошення</a>"

                    for uid in targets:
                        await context.bot.send_message(
                            chat_id=uid,
                            text=text,
                            parse_mode="HTML"
                    )

                cur.execute("""
                    UPDATE notification_state
                       SET last_run = %s
                     WHERE name = 'ads'
                """, (now,))
    finally:
        conn.close()

# ====== ConversationHandler ======
conv_handler = ConversationHandler(
    entry_points=[
        CommandHandler("post_ad", post_ad_start),                 
        CallbackQueryHandler(post_ad_start, pattern="^post_ad$"),
        CallbackQueryHandler(edit_ad_start, pattern="^edit_ad_\\d+$"),
    ],
    states={
        CATEGORY: [
            CallbackQueryHandler(category_chosen, pattern="^cat_")
        ],
        CITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, city_received)],
        PRICE: [
            CallbackQueryHandler(price_received, pattern="^preset_|^custom_price$"),
            MessageHandler(filters.TEXT & ~filters.COMMAND, price_received),
        ],
        DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, desc_received)],
        PHOTO: [
            CallbackQueryHandler(photo_received, pattern="^no_photo$"),
            MessageHandler(filters.PHOTO, photo_received)
        ],
        CONFIRM: [
            CallbackQueryHandler(confirm_handler, pattern="^confirm$"),
            CallbackQueryHandler(edit_category_start,  pattern="^edit_category$"),
            CallbackQueryHandler(edit_city_start, pattern="^edit_city$"),
            CallbackQueryHandler(edit_price_start, pattern="^edit_price$"),
            CallbackQueryHandler(edit_desc_start, pattern="^edit_desc$"),
            CallbackQueryHandler(edit_photo_start, pattern="^edit_photo$"),
            CallbackQueryHandler(cancel_conversation, pattern="^cancel$"),
        ],
        EDIT_CATEGORY: [
            CallbackQueryHandler(edit_category_received, pattern="^cat_"),
            CallbackQueryHandler(send_summary,   pattern="^back_to_summary$")
        ],
        EDIT_CITY: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, edit_city_received),
            CallbackQueryHandler(send_summary, pattern="^back_to_summary$")
        ],
        EDIT_PRICE: [
            CallbackQueryHandler(edit_price_received, pattern="^preset_|^custom_price$"),
            MessageHandler(filters.TEXT & ~filters.COMMAND, edit_price_received),
            CallbackQueryHandler(send_summary, pattern="^back_to_summary$")
        ],
        EDIT_DESC: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, edit_desc_received),
            CallbackQueryHandler(send_summary, pattern="^back_to_summary$")
        ],
        EDIT_PHOTO: [
            CallbackQueryHandler(edit_photo_received, pattern="^no_photo$"),
            MessageHandler(filters.PHOTO, edit_photo_received)
        ],
    },
    fallbacks=[
        CommandHandler("cancel", cancel_conversation),
        CommandHandler("start", cancel_conversation),
        CommandHandler("help", cancel_conversation),
        CallbackQueryHandler(cancel_conversation, pattern="^cancel$")
    ],
    per_chat=True
)

review_conv = ConversationHandler(
    entry_points=[
        CallbackQueryHandler(review_start, pattern=r"^review_ad_(\d+)_(\d+)$")
    ],
    states={
        REVIEW_RATING: [
            CallbackQueryHandler(rating_received, pattern=r"^[1-5]$"),
        ],
        REVIEW_COMMENT: [
            CallbackQueryHandler(comment_skip, pattern="^skip$"),
            MessageHandler(filters.TEXT & ~filters.COMMAND, comment_received),
        ],
    },
    fallbacks=[
        CommandHandler("cancel", review_cancel),
        CallbackQueryHandler(review_cancel, pattern="^cancel$")
    ],
    per_chat=True,
)

change_nick_conv = ConversationHandler(
    entry_points=[ CallbackQueryHandler(change_nick_start, pattern="^change_nick_start$") ],
    states={
        CHANGE_NICK: [
            MessageHandler(filters.TEXT & ~filters.COMMAND, change_nick_received)
        ],
    },
    fallbacks=[CallbackQueryHandler(change_nick_cancel, pattern="^nick_cancel$")],
    per_chat=True
)

if __name__ == "__main__":
    print(f"{Fore.GREEN}База даних готова — стартую бота!")
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(review_conv)
    app.add_handler(conv_handler)
    app.add_handler(change_nick_conv)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", support_handler))
    app.add_handler(CommandHandler("community", community_handler))
    app.add_handler(CommandHandler("account", account_handler))
    app.add_handler(CallbackQueryHandler(view_ads_start, pattern=r"^view_ads_[^_]+$"))
    app.add_handler(CallbackQueryHandler(account_handler, pattern="^account$"))
    app.add_handler(CallbackQueryHandler(community_handler, pattern="^community$"))
    app.add_handler(CallbackQueryHandler(support_handler, pattern="^support$"))
    app.add_handler(CallbackQueryHandler(all_ads_handler, pattern=r"^menu_all_ads_[^_]+$")) 
    app.add_handler(CallbackQueryHandler(all_ads_handler, pattern=r"^all_ads_[^_]+_\d+$" ))
    app.add_handler(CallbackQueryHandler(menu_top_ads_handler,   pattern=r"^menu_top_ads_[^_]+$"))
    app.add_handler(CallbackQueryHandler(menu_top_ads_handler,   pattern=r"^top_ads_[^_]+_\d+$"))
    app.add_handler(CallbackQueryHandler(menu_top_cities_handler,pattern=r"^menu_top_cities_[^_]+$"))
    app.add_handler(CallbackQueryHandler(menu_top_cities_handler,pattern=r"^top_cities_[^_]+_\d+$"))
    app.add_handler(CallbackQueryHandler(show_ad_handler, pattern="^show_ad_"))
    app.add_handler(CallbackQueryHandler(reviews_about_user_handler, pattern=r"^reviews_about_user_\d+_[^_]+_\d+$"))
    app.add_handler(CallbackQueryHandler(back_to_main_handler, pattern="^back$"))
    app.add_handler(InlineQueryHandler(inline_city_suggest))
    app.add_handler(CommandHandler("city", city_command))
    app.add_handler(CallbackQueryHandler(city_command, pattern=r"^city_.+_.+_\d+$"))
    app.add_handler(CallbackQueryHandler(my_ads_handler, pattern="^my_ads(_\\d+)?$"))
    app.add_handler(CallbackQueryHandler(my_apps_handler, pattern=r"^my_apps(?:_\d+)?$"))
    app.add_handler(CallbackQueryHandler(my_subs_handler, pattern=r"^my_subs(?:_\d+)?$"))
    app.add_handler(CallbackQueryHandler(show_app_handler, pattern=r"^show_app_\d+\|my_apps\|\d+$"))
    app.add_handler(CallbackQueryHandler(delete_ad_handler, pattern=r"^delete_ad_\d+$"))
    app.add_handler(CallbackQueryHandler(my_reviews_handler, pattern=r"^my_reviews(_\d+)?$"))
    app.add_handler(CallbackQueryHandler(reviews_about_me_handler, pattern=r"^reviews_about(_\d+)?$"))
    app.add_handler(CallbackQueryHandler(show_review_handler, pattern=r"^show_review_"))
    app.add_handler(CallbackQueryHandler(delete_review_handler, pattern=r"^delete_review_\d+$"))
    app.add_handler(CallbackQueryHandler(apply_handler,  pattern=r"^apply_\d+$"))
    app.add_handler(CallbackQueryHandler(accept_handler, pattern=r"^accept_\d+$"))
    app.add_handler(CallbackQueryHandler(reject_handler, pattern=r"^reject_\d+$"))
    app.add_handler(CallbackQueryHandler(noop_handler,    pattern="^noop$"))
    app.add_handler(CallbackQueryHandler(cancel_reminder_handler, pattern=r"^cancel_reminder_\d+$"))
    app.add_handler(CallbackQueryHandler(snooze_handler,         pattern=r"^snooze_app_\d+$"))
    app.add_handler(CallbackQueryHandler(category_subscription_menu, pattern=r"^menu_cat"))
    app.add_handler(CallbackQueryHandler(user_subscription_menu, pattern=r"^menu_user"))
    app.add_handler(CallbackQueryHandler(subscribe_category_handler, pattern=r"^sub_cat"))
    app.add_handler(CallbackQueryHandler(subscribe_user_handler,     pattern=r"^sub_user"))
    app.add_handler(CallbackQueryHandler(unsubscribe_category_handler, pattern=r"^unsub_cat"))
    app.add_handler(CallbackQueryHandler(unsubscribe_user_handler, pattern=r"^unsub_user"))

    app.job_queue.run_repeating(callback=send_due_reminders, interval= 10 * 60, first=60)

    app.job_queue.run_repeating(send_new_ads_notifications, interval= 10 * 60, first=30)

    logging.basicConfig(level=logging.INFO)

    app.run_webhook(
        listen="0.0.0.0",
        port=8001,
        url_path=WEBHOOK_PATH,
        webhook_url=WEBHOOK_URL,
        drop_pending_updates=True
    )

    print(f"{Fore.GREEN}Бот запущено — очікую повідомлень!")
