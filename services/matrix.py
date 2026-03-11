import sqlite3
import pandas as pd
import datetime
import io

from config import DB_NAME, UNIQUE_FILE


def create_presence_matrix_from_db():

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Load master security list
    presence_df = pd.read_csv(UNIQUE_FILE)
    presence_df = presence_df.drop_duplicates(subset=["SECURITY_ID"])

    start_date = datetime.datetime(2017, 1, 1)
    end_date = datetime.datetime.today()

    current_date = start_date

    while current_date <= end_date:

        year = current_date.strftime("%Y")
        month = current_date.strftime("%b")
        day = current_date.strftime("%d")

        col_name = (year, month, day)

        date_str = current_date.strftime("%Y-%m-%d")

        print(f"Processing date: {date_str}")

        cursor.execute(
            "SELECT file_data FROM bhavcopy_files WHERE trade_date=?",
            (date_str,)
        )

        row = cursor.fetchone()

        if row:

            print(f"✔ File found for {date_str}")

            file_bytes = row[0]

            df = pd.read_csv(
                io.BytesIO(file_bytes),
                usecols=["SECURITY_ID"]
            )

            df = df.drop_duplicates(subset=["SECURITY_ID"])

            merged = presence_df[["SECURITY_ID"]].copy()

            merged["PRESENT"] = merged["SECURITY_ID"].isin(df["SECURITY_ID"])

            merged["PRESENT"] = merged["PRESENT"].map({
                True: "YES",
                False: "NO"
            })

            presence_df[col_name] = merged["PRESENT"]

            print(f"Processed {len(df)} securities")

        else:

            print(f"✖ No file in DB for {date_str} → Filling NO")

            presence_df[col_name] = "NO"

        current_date += datetime.timedelta(days=1)

    print("All dates processed. Building multi-level headers...")

    # Multi-level header
    cols = []

    for col in presence_df.columns:

        if col in ["SECURITY_ID", "SYMBOL"]:
            cols.append(("INFO", "", col))
        else:
            cols.append(col)

    presence_df.columns = pd.MultiIndex.from_tuples(cols)

    print("Saving matrix to database...")

    # Save CSV to memory
    csv_buffer = io.StringIO()

    presence_df.to_csv(csv_buffer, index=False)

    csv_bytes = csv_buffer.getvalue().encode()

    file_name = "security_presence_matrix.csv"

    created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute(
        """
        INSERT INTO generated_files
        (file_name, created_at, file_data)
        VALUES (?, ?, ?)
        """,
        (file_name, created_at, csv_bytes)
    )

    conn.commit()
    conn.close()

    print("Matrix successfully generated and stored in DB")

    return "Matrix Generated and Stored in DB"