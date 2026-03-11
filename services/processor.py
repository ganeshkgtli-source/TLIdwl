import pandas as pd
import io

symbol_df = pd.read_csv("data/BSE_CM_symbol_list.csv", header=None)
mapping_df = symbol_df.iloc[:, [12, 9]]

symbol_map = dict(zip(mapping_df.iloc[:, 0], mapping_df.iloc[:, 1]))


def process_csv_before_2024(file_bytes):

    df = pd.read_csv(io.BytesIO(file_bytes))

    df = df.rename(columns={
        "SC_CODE": "SECURITY_ID",
        "SC_NAME": "SYMBOL",
        "SC_GROUP": "SERIES",
        "SC_TYPE": "TYPE",
    })

    df["SYMBOL"] = df["SECURITY_ID"].map(symbol_map).fillna(df["SYMBOL"])

    df = df[df["SERIES"].str.startswith(("A", "B"), na=False)]

    df["TYPE"] = df["TYPE"].apply(
        lambda x: "EQUITY" if str(x).upper() == "Q" else x
    )

    cols_to_remove = ["PREVCLOSE", "NET_TURNOV", "TDCLOINDI"]
    df = df.drop(columns=[c for c in cols_to_remove if c in df.columns])

    df = df[
        [
            "SECURITY_ID",
            "SYMBOL",
            "SERIES",
            "TYPE",
            "OPEN",
            "HIGH",
            "LOW",
            "CLOSE",
            "NO_TRADES",
            "NO_OF_SHRS"
        ]
    ]

    output = io.StringIO()
    df.to_csv(output, index=False)

    return output.getvalue().encode()


def process_bhavcopy_after_2024(file_bytes):

    df = pd.read_csv(io.BytesIO(file_bytes))

    df = df.rename(columns={
        "TradDt": "DATE",
        "FinInstrmId": "SECURITY_ID",
        "TckrSymb": "SYMBOL",
        "SctySrs": "SERIES",
        "OpnPric": "OPEN",
        "HghPric": "HIGH",
        "LwPric": "LOW",
        "ClsPric": "CLOSE",
        "TtlNbOfTxsExctd": "NO_OF_TRADE",
        "TtlTradgVol": "NO_OF_SHRS"
    })

    df["SYMBOL"] = df["SECURITY_ID"].map(symbol_map).fillna(df["SYMBOL"])

    df = df[df["SERIES"].str.startswith(("A", "B"), na=False)]

    df["TYPE"] = "EQUITY"

    output = io.StringIO()
    df.to_csv(output, index=False)

    return output.getvalue().encode()