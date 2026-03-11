import sqlite3
import datetime
from config import DB_NAME


def init_db():

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS bhavcopy_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT UNIQUE,
        trade_date TEXT,
        year INTEGER,
        month TEXT,
        file_data BLOB,
        is_deleted INTEGER DEFAULT 0
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS download_logs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT UNIQUE,
        trade_date TEXT,
        week_day TEXT,
        status TEXT,
        download_time TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS generated_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT,
        created_at TEXT,
        file_data BLOB
    )
    """)

    conn.commit()
    conn.close()


def save_file_to_db(file_name, file_bytes, date_obj):

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    try:
        cursor.execute("""
        INSERT INTO bhavcopy_files
        (file_name, trade_date, year, month, file_data)
        VALUES (?, ?, ?, ?, ?)
        """, (
            file_name,
            str(date_obj),
            date_obj.year,
            date_obj.strftime("%b").upper(),
            file_bytes
        ))

        conn.commit()

    except sqlite3.IntegrityError:
        pass

    conn.close()


def save_log(file_name, trade_date, week_day, status):

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute("""
    INSERT INTO download_logs
    (file_name, trade_date, week_day, status, download_time)
    VALUES (?,?,?,?,?)
    ON CONFLICT(file_name)
    DO UPDATE SET
        trade_date=excluded.trade_date,
        week_day=excluded.week_day,
        status=excluded.status,
        download_time=excluded.download_time
    """, (file_name, trade_date, week_day, status, now))

    conn.commit()
    conn.close()