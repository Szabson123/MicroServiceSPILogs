import pyodbc
import traceback
from config import settings


def fetch_new_asm_logs(database_host: str, last_fixed: int):
    print("\n===== [ASM] START FETCH LOGS =====")
    print(f"[ASM] Próba połączenia z HOSTEM: {database_host}")
    print(f"[ASM] Last fixed: {last_fixed}")

    print("[ASM] Pobieram nazwę bazy (DBName) z SPI_DataMapTable...")

    try:
        # 1️⃣ Połączenie do master tylko po DBName
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
            print("❌ [ASM] Brak DBName w SPI_DataMapTable!")
            cursor.close()
            conn.close()
            return [], 0

        db_name = row[0]
        print(f"[ASM] Ustalona baza danych: {db_name}")

        cursor.close()
        conn.close()

    except Exception as e:
        print("❌ [ASM] Błąd podczas pobierania DBName!")
        print(str(e))
        print(traceback.format_exc())
        return [], 0

    # 2️⃣ Połączenie do właściwej bazy
    print(f"[ASM] Łączę z bazą: {db_name}")

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

        print("[ASM] Połączenie z bazą OK ✔")

    except Exception as e:
        print("❌ [ASM] Błąd połączenia z DBName!")
        print(str(e))
        print(traceback.format_exc())
        return [], 0

    # 3️⃣ Pobieranie max PCBIndex z tabeli dbo.PCB
    try:
        print("[ASM] Pobieram MAX(PCBIndex) z dbo.PCB...")
        cursor.execute("SELECT MAX(PCBIndex) FROM dbo.PCB")
        max_index = cursor.fetchone()[0]

        print(f"[ASM] MAX PCBIndex = {max_index}")

        if max_index is None:
            print("[ASM] Tabela dbo.PCB jest pusta ✔")
            cursor.close()
            conn.close()
            return [], 0

    except Exception as e:
        print("❌ [ASM] Błąd w SELECT MAX(PCBIndex) FROM dbo.PCB")
        print(str(e))
        print(traceback.format_exc())
        cursor.close()
        conn.close()
        return [], 0

    # 4️⃣ Pobranie nowych rekordów
    try:
        if max_index < last_fixed:
            print("[ASM] Reset fixed_id (nowy miesiąc)")
            last_fixed = 0

        print(f"[ASM] Pobieram rekordy z dbo.PCB gdzie PCBIndex > {last_fixed}...")
        cursor.execute("""
            SELECT PCBIndex, PCBName, Result
            FROM dbo.PCB
            WHERE PCBIndex > ?
            ORDER BY PCBIndex ASC
        """, (last_fixed,))

        rows_raw = cursor.fetchall()

        print(f"[ASM] Znaleziono {len(rows_raw)} rekordów")

        rows = [
            {"IDNO": r[0], "PCBNAME": r[1], "RESULT": r[2]}
            for r in rows_raw
        ]

    except Exception as e:
        print("❌ [ASM] Błąd podczas SELECT z dbo.PCB")
        print(str(e))
        print(traceback.format_exc())
        cursor.close()
        conn.close()
        return [], 0

    cursor.close()
    conn.close()
    print("===== [ASM] END FETCH LOGS =====\n")

    return rows, max_index
