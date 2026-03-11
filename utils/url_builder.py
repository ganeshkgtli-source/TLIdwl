import datetime

def build_bse_url(date_obj):

    change_date = datetime.date(2024, 7, 8)

    if date_obj < change_date:
        date_str = date_obj.strftime("%d%m%y")
        return "ZIP", f"https://www.bseindia.com/download/BhavCopy/Equity/EQ{date_str}_CSV.ZIP"

    else:
        date_str = date_obj.strftime("%Y%m%d")
        return "CSV", f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{date_str}_F_0000.CSV"