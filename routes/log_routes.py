from flask import Blueprint, render_template, Response
import sqlite3

from config import DB_NAME

log_routes = Blueprint("log_routes", __name__)


@log_routes.route("/logs/<int:year>")
def logs_page(year):

    return render_template("logs.html", year=year)


@log_routes.route("/logs")
def logs():

    return render_template("logs_all.html")


@log_routes.route("/log-dashboard")
def log_dashboard():

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT file_name,trade_date,week_day,status,download_time
        FROM download_logs
        ORDER BY trade_date DESC
    """)

    logs = cursor.fetchall()

    conn.close()

    return render_template("log_dashboard.html", logs=logs)