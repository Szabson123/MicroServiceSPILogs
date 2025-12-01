import pyodbc
import traceback
from config import settings


def fetch_new_asm_logs(database_host: str, last_fixed: int):
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={database_host};"
            f"DATABASE=master;"
            f"UID={settings.spi_asm_user};"
            f"PWD={settings.spi_asm_password};"
            "Trusted_Connection=no;",
            timeout=5
        )
        cursor = conn.cursor()

        cursor.execute("""
            SELECT TOP 1 [DBName]
            FROM [master].[dbo].[SPI_DataMapTable]
            ORDER BY endtime DESC
        """)
        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            return [], 0

        db_name = row[0]

        cursor.close()
        conn.close()

    except Exception as e:
        return [], 0

    # 2️⃣ Połączenie do właściwej bazy

    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={database_host};"
            f"DATABASE={db_name};"
            f"UID={settings.spi_asm_user};"
            f"PWD={settings.spi_asm_password};"
            "Trusted_Connection=no;",
            timeout=5
        )
        cursor = conn.cursor()


    except Exception as e:
        return [], 0

    # 3️⃣ Pobieranie max PCBIndex z tabeli dbo.PCB
    try:
        cursor.execute("SELECT MAX(PCBIndex) FROM dbo.PCB")
        max_index = cursor.fetchone()[0]


        if max_index is None:
            cursor.close()
            conn.close()
            return [], 0

    except Exception as e:
        cursor.close()
        conn.close()
        return [], 0

    # 4️⃣ Pobranie nowych rekordów
    try:
        if max_index < last_fixed:
            last_fixed = 0

        cursor.execute("""
            SELECT PCBIndex, PCBName, Result
            FROM dbo.PCB
            WHERE PCBIndex > ?
            ORDER BY PCBIndex ASC
        """, (last_fixed,))

        rows_raw = cursor.fetchall()


        rows = [
            {"IDNO": r[0], "PCBNAME": r[1], "RESULT": r[2]}
            for r in rows_raw
        ]

    except Exception as e:
        cursor.close()
        conn.close()
        return [], 0

    cursor.close()
    conn.close()

    return rows, max_index
