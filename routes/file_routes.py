from flask import Blueprint, render_template, request, send_file, jsonify
import sqlite3
import zipfile
import io

from config import DB_NAME

file_routes = Blueprint("file_routes", __name__)


@file_routes.route("/files")
def view_files():

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id,file_name,trade_date,year,month
        FROM bhavcopy_files
        WHERE is_deleted=0
        ORDER BY trade_date DESC
    """)

    files = cursor.fetchall()

    conn.close()

    return render_template("files.html", files=files)


@file_routes.route("/download-selected", methods=["POST"])
def download_selected():

    selected_ids = request.form.getlist("file_ids")

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as z:

        for file_id in selected_ids:

            cursor.execute("""
                SELECT file_name,year,month,file_data
                FROM bhavcopy_files
                WHERE id=?
            """, (file_id,))

            row = cursor.fetchone()

            if row:

                file_name, year, month, file_data = row

                zip_path = f"Bhavcopy/{year}/{month}/{file_name}"

                z.writestr(zip_path, file_data)

    conn.close()

    zip_buffer.seek(0)

    return send_file(
        zip_buffer,
        mimetype="application/zip",
        as_attachment=True,
        download_name="Bhavcopy.zip"
    )


@file_routes.route("/delete-selected", methods=["POST"])
def delete_selected():

    selected_ids = request.form.getlist("file_ids")

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for file_id in selected_ids:

        cursor.execute(
            "DELETE FROM bhavcopy_files WHERE id=?",
            (file_id,)
        )

    conn.commit()
    conn.close()

    return jsonify({"message": "Files deleted"})


@file_routes.route("/delete-temp", methods=["POST"])
def delete_temp():

    selected_ids = request.form.getlist("file_ids")

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    for file_id in selected_ids:

        cursor.execute("""
            UPDATE bhavcopy_files
            SET is_deleted=1
            WHERE id=?
        """, (file_id,))

    conn.commit()
    conn.close()

    return jsonify({"message": "Moved to trash"})


@file_routes.route("/trash")
def trash():

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT id,file_name,trade_date,year,month
        FROM bhavcopy_files
        WHERE is_deleted=1
    """)

    files = cursor.fetchall()

    conn.close()

    return render_template("trash.html", files=files)