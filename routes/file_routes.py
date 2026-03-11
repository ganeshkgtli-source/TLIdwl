from flask import Blueprint, render_template, request, send_file, redirect
import sqlite3
import os
import zipfile
import io

from config import DB_NAME

file_routes = Blueprint("file_routes", __name__)


@file_routes.route("/files")
def view_files():

    selected_year = request.args.get("year")
    selected_month = request.args.get("month")

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    query = """
        SELECT id, file_name, trade_date, year, month
        FROM bhavcopy_files
        WHERE is_deleted = 0
    """

    params = []

    if selected_year:
        query += " AND year = ?"
        params.append(selected_year)

    if selected_month:
        query += " AND month = ?"
        params.append(selected_month)

    query += " ORDER BY trade_date DESC"

    cursor.execute(query, params)
    files = cursor.fetchall()

    cursor.execute("SELECT DISTINCT year FROM bhavcopy_files ORDER BY year DESC")
    years = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT DISTINCT month FROM bhavcopy_files ORDER BY month")
    months = [row[0] for row in cursor.fetchall()]

    conn.close()

    return render_template(
        "files.html",
        files=files,
        years=years,
        months=months,
        selected_year=selected_year,
        selected_month=selected_month
    )


@file_routes.route("/download-selected", methods=["POST"])
def download_selected():

    ids = request.form.getlist("file_ids")

    if not ids:
        return "No files selected"

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    placeholders = ",".join(["?"] * len(ids))

    cursor.execute(
        f"""
        SELECT file_name, year, month
        FROM bhavcopy_files
        WHERE id IN ({placeholders})
        """,
        ids
    )

    files = cursor.fetchall()
    conn.close()

    memory_file = io.BytesIO()

    with zipfile.ZipFile(memory_file, "w") as zf:

        for file_name, year, month in files:

            file_path = os.path.join(
                "Downloaded",
                str(year),
                str(month),
                file_name
            )

            if os.path.exists(file_path):
                zf.write(file_path, file_name)

    memory_file.seek(0)

    return send_file(
        memory_file,
        download_name="bhavcopy_files.zip",
        as_attachment=True
    )


@file_routes.route("/delete-selected", methods=["POST"])
def delete_selected():

    ids = request.form.getlist("file_ids")

    if not ids:
        return redirect("/files")

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    placeholders = ",".join(["?"] * len(ids))

    cursor.execute(
        f"""
        UPDATE bhavcopy_files
        SET is_deleted = 1
        WHERE id IN ({placeholders})
        """,
        ids
    )

    conn.commit()
    conn.close()

    return redirect("/files")


@file_routes.route("/delete-temp", methods=["POST"])
def delete_temp():

    ids = request.form.getlist("file_ids")

    if not ids:
        return redirect("/trash")

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    placeholders = ",".join(["?"] * len(ids))

    cursor.execute(
        f"""
        DELETE FROM bhavcopy_files
        WHERE id IN ({placeholders})
        """,
        ids
    )

    conn.commit()
    conn.close()

    return redirect("/trash")


@file_routes.route("/trash")
def view_trash():

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id, file_name, trade_date, year, month
        FROM bhavcopy_files
        WHERE is_deleted = 1
        ORDER BY trade_date DESC
    """)

    files = cursor.fetchall()

    conn.close()

    return render_template("trash.html", files=files)