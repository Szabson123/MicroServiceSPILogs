import pyodbc
from datetime import datetime
from dateutil.relativedelta import relativedelta
from config import settings


def fetch_new_spi_logs(database_name: str, last_fixed: int):
    conn = pyodbc.connect(
        f"DRIVER={{SQL Server}};"
        f"SERVER={settings.spi_host};"
        f"DATABASE={database_name};"
        f"UID={settings.spi_user};"
        f"PWD={settings.spi_password};"
        "Trusted_Connection=no;"
    )
    cursor = conn.cursor()

    table_now = f"Pcb{datetime.now().strftime('%Y%m')}"
    cursor.execute(f"SELECT MAX(IDNO) FROM dbo.{table_now}")
    spi_max_idno = cursor.fetchone()[0]

    if spi_max_idno is None:
        return [], 0

    if spi_max_idno < last_fixed:
        last_fixed = 0

    cursor.execute(f"""
        SELECT IDNO, PCBNAME, RESULT
        FROM dbo.{table_now}
        WHERE IDNO > ?
        ORDER BY IDNO ASC
    """, (last_fixed,))

    rows = [
        {"IDNO": r[0], "PCBNAME": r[1], "RESULT": r[2]}
        for r in cursor.fetchall()
    ]

    cursor.close()
    conn.close()

    return rows, spi_max_idno
