from flask import Blueprint, render_template, request
import datetime

from services.downloader import (
    download_bhavcopy,
    download_year_data,
    download_all_data
)

main_routes = Blueprint("main_routes", __name__)


@main_routes.route("/", methods=["GET", "POST"])
def index():

    message = ""

    if request.method == "POST":

        if "download_date" in request.form:

            date_str = request.form.get("date")

            date_obj = datetime.datetime.strptime(
                date_str,
                "%Y-%m-%d"
            ).date()

            message = download_bhavcopy(date_obj)

        elif "download_year" in request.form:

            year = int(request.form.get("year"))

            message = download_year_data(year)

        elif "download_all" in request.form:

            message = download_all_data()

    return render_template("index.html", message=message)