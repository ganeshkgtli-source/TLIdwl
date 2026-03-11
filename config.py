import requests

DB_NAME = "bhavcopy_files.db"
UNIQUE_FILE = "data/unique_security_ids_alldata.csv"

DOWNLOAD_STATE = {}

session = requests.Session()

session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx",
})

session.get("https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx")