from flask import Blueprint, render_template, Response, redirect
import datetime
import sqlite3

from config import DOWNLOAD_STATE, DB_NAME
from services.downloader import download_bhavcopy

log_routes = Blueprint("log_routes", __name__)


# ===============================
# LOGS PAGE (ALL YEARS)
# ===============================
@log_routes.route("/logs-all")
def logs_all():
    return render_template("logs_all.html")


# ===============================
# LOGS PAGE FOR SPECIFIC YEAR
# ===============================
@log_routes.route("/logs/<int:year>")
def logs_page(year):
    return render_template("logs.html", year=year)


# ===============================
# REDIRECT TO ACTIVE LOG
# ===============================
@log_routes.route("/logs")
def logs_redirect():

    for year, state in DOWNLOAD_STATE.items():
        if state["running"]:
            return redirect(f"/logs/{year}")

    if DOWNLOAD_STATE:
        last_year = list(DOWNLOAD_STATE.keys())[-1]
        return redirect(f"/logs/{last_year}")

    return "No logs available"


# ===============================
# STREAM ALL YEARS DOWNLOAD
# ===============================
@log_routes.route("/stream-all-logs")
def stream_all_logs():

    key = "all"

    if key not in DOWNLOAD_STATE:
        DOWNLOAD_STATE[key] = {
            "running": False,
            "logs": []
        }

    def generate():

        state = DOWNLOAD_STATE[key]

        if state["running"]:
            for log in state["logs"]:
                yield f"data: {log}\n\n"
            return

        state["running"] = True
        state["logs"].clear()

        start_year = 2017
        current_year = datetime.date.today().year

        for year in range(start_year, current_year + 1):

            log = f"Starting download for {year}"
            state["logs"].append(log)
            yield f"data: {log}\n\n"

            start_date = datetime.date(year, 1, 1)

            if year == current_year:
                end_date = datetime.date.today()
            else:
                end_date = datetime.date(year, 12, 31)

            current = start_date

            while current <= end_date:

                log = f"Downloading: {current}"
                state["logs"].append(log)
                yield f"data: {log}\n\n"

                result = download_bhavcopy(current)

                state["logs"].append(result)
                yield f"data: {result}\n\n"

                current += datetime.timedelta(days=1)

            log = f"Completed year {year}"
            state["logs"].append(log)
            yield f"data: {log}\n\n"

        finish = "Completed all years"
        state["logs"].append(finish)
        yield f"data: {finish}\n\n"

        state["running"] = False

    return Response(generate(), mimetype="text/event-stream")


# ===============================
# STREAM YEAR DOWNLOAD
# ===============================
@log_routes.route("/stream-logs/<int:year>")
def stream_logs(year):

    if year not in DOWNLOAD_STATE:
        DOWNLOAD_STATE[year] = {
            "running": False,
            "logs": []
        }

    def generate():

        state = DOWNLOAD_STATE[year]

        if state["running"]:
            for log in state["logs"]:
                yield f"data: {log}\n\n"
            return

        state["running"] = True
        state["logs"].clear()

        start_date = datetime.date(year, 1, 1)
        end_date = datetime.date(year, 12, 31)

        current = start_date

        while current <= end_date:

            log = f"Downloading: {current}"
            state["logs"].append(log)
            yield f"data: {log}\n\n"

            result = download_bhavcopy(current)

            state["logs"].append(result)
            yield f"data: {result}\n\n"

            current += datetime.timedelta(days=1)

        finish = f"Completed year {year}"
        state["logs"].append(finish)
        yield f"data: {finish}\n\n"

        state["running"] = False

    return Response(generate(), mimetype="text/event-stream")


# ===============================
# DOWNLOAD HISTORY DASHBOARD
# ===============================
@log_routes.route("/log-dashboard")
def log_dashboard():

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT file_name, trade_date, week_day, status, download_time
        FROM download_logs
        ORDER BY trade_date DESC
    """)

    logs = cursor.fetchall()

    conn.close()

    return render_template("log_dashboard.html", logs=logs)