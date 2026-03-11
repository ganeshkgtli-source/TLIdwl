from flask import Blueprint, render_template, Response, jsonify
import sqlite3

from config import DB_NAME
from services.matrix import create_presence_matrix_from_db

matrix_routes = Blueprint("matrix_routes", __name__)


@matrix_routes.route("/matrix")
def matrix():

    return render_template("matrix.html")


@matrix_routes.route("/generate_matrix")
def generate_matrix():

    result = create_presence_matrix_from_db()

    return jsonify({"status": result})


@matrix_routes.route("/matrix_file")
def matrix_file():

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT file_data
        FROM generated_files
        ORDER BY id DESC
        LIMIT 1
    """)

    row = cursor.fetchone()

    conn.close()

    return Response(row[0], mimetype="text/csv")


@matrix_routes.route("/download_matrix")
def download_matrix():

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT file_name,file_data
        FROM generated_files
        ORDER BY id DESC
        LIMIT 1
    """)

    row = cursor.fetchone()

    conn.close()

    return Response(
        row[1],
        mimetype="text/csv",
        headers={
            "Content-Disposition":
            f'attachment; filename="{row[0]}"'
        }
    )