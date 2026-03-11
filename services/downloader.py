import datetime
import zipfile
import io
import sqlite3

from config import DB_NAME, session
from utils.url_builder import build_bse_url
from database.db import save_file_to_db, save_log
from services.processor import (
    process_csv_before_2024,
    process_bhavcopy_after_2024
)


def download_bhavcopy(date_obj):

    day_name = date_obj.strftime("%A")

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT 1 FROM bhavcopy_files WHERE trade_date=?",
        (str(date_obj),)
    )

    if cursor.fetchone():
        conn.close()
        return f"Already exists in DB: {date_obj} ({day_name})"

    conn.close()

    file_type, url = build_bse_url(date_obj)

    try:

        print("Downloading:", url)

        response = session.get(url, timeout=(3, 40))

        if response.status_code != 200:

            file_name = f"EQ{date_obj.strftime('%d%m%y')}.CSV"

            save_log(
                file_name,
                str(date_obj),
                day_name,
                "Not Available"
            )

            return f"File not available: {date_obj} ({day_name})"

        # OLD FORMAT ZIP
        if file_type == "ZIP":

            with zipfile.ZipFile(io.BytesIO(response.content)) as z:

                for file in z.namelist():

                    raw_bytes = z.read(file)

                    processed = process_csv_before_2024(raw_bytes)

                    save_file_to_db(
                        file,
                        processed,
                        date_obj
                    )

                    save_log(
                        file,
                        str(date_obj),
                        day_name,
                        "Downloaded"
                    )

            return f"Downloaded (ZIP): {date_obj} ({day_name})"

        # NEW FORMAT CSV
        else:

            file_name = f"EQ{date_obj.strftime('%d%m%y')}.CSV"

            processed = process_bhavcopy_after_2024(response.content)

            save_file_to_db(
                file_name,
                processed,
                date_obj
            )

            save_log(
                file_name,
                str(date_obj),
                day_name,
                "Downloaded"
            )

            return f"Downloaded: {date_obj} ({day_name})"

    except Exception as e:

        file_name = f"EQ{date_obj.strftime('%d%m%y')}.CSV"

        save_log(
            file_name,
            str(date_obj),
            day_name,
            "Error"
        )

        return f"Error {date_obj}: {e}"


def download_year_data(year):

    start_date = datetime.date(year, 1, 1)
    end_date = datetime.date(year, 12, 31)

    current = start_date

    while current <= end_date:

        result = download_bhavcopy(current)

        print(result)

        current += datetime.timedelta(days=1)

    return f"Completed year {year}"


# def download_all_data():

#     start_year = 2017
#     current_year = datetime.date.today().year

#     for year in range(start_year, current_year + 1):

#         print(f"Starting year {year}")

#         download_year_data(year)

#         print(f"Completed year {year}")

#     return "Download started for all years"
def download_all_data():
    start_year = 2017
    current_year = datetime.date.today().year
    for year in range(start_year, current_year + 1):
        download_year_data(year)
    return "Download started..."