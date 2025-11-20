from fastapi import FastAPI, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session
from database import engine, get_db
from pydantic import BaseModel
from spi_test import fetch_new_spi_logs

from utils.spi_connect_asm import fetch_new_asm_logs

from datetime import datetime


def set_kill_flag(db, line_id, value: bool):
    db.execute(text("""
        UPDATE checkprocess_apptokill
        SET killing_flag = :val
        WHERE line_name_id = :line_id
    """), {
        "val": value,
        "line_id": line_id
    })
    db.commit()


def fetch_databases(db: Session):
    sql = text("""
        SELECT *
        FROM public.checkprocess_databasesspimap
        WHERE active = True""")
    return db.execute(sql).mappings().all()

def fetch_ASM_databases(db: Session):
    sql = text("""
        SELECT *
        FROM public.checkprocess_databasesasmmap
        WHERE active = True""")
    return db.execute(sql).mappings().all()


def check_line_status(db: Session, line_id: int):
    sql = text("""
        SELECT id, full_sn
        FROM checkprocess_productobject
        WHERE current_place_id = :line_id AND "end" = FALSE
    """)

    result = db.execute(sql, {"line_id": line_id}).mappings().all()
    count = len(result)

    if count == 0:
        return None
    elif count == 1:
        if get_kill_flag(db, line_id) is False:
            set_kill_flag(db, line_id, False)
        return result[0]["full_sn"]
    else:
        return "Krytyczny błąd"


def working_test(db: Session):
    print("Hello")

    spi_databases = fetch_databases(db)
    response = []

    for item in spi_databases:
        line_id = item["line_name_id"]
        db_name  = item["data_base_name"]

        status = check_line_status(db, line_id)

        if status is None:
            spi_rows, spi_max_id = fetch_new_spi_logs(db_name, 0)

            decision = handle_no_product_case(db, line_id, spi_rows)

            if decision == "kill":
                set_kill_flag(db, line_id, True)

                response.append({
                    "line_name_id": line_id,
                    "data_base_name": db_name,
                    "status": "BLAD — produkcja bez produktu!",
                    "spi_count": 0,
                    "kill": True
                })
                continue

            response.append({
                "line_name_id": line_id,
                "data_base_name": db_name,
                "status": "Brak produktu — niedobitki ignoruje",
                "spi_count": 0,
                "kill": False
            })
            continue

        last_fixed = db.execute(text("""
            SELECT MAX(fixed_id) 
            FROM checkprocess_logfromspi
            WHERE machine_name_id = :line_id
        """), {"line_id": line_id}).scalar()

        if last_fixed is None:
            last_fixed = 0

        spi_rows, spi_max_id = fetch_new_spi_logs(db_name, last_fixed)

        spi_count, new_last_fixed = save_new_spi_logs(db, line_id, spi_rows, status)

        if spi_count > 0:
            add_cycles_to_sito(db, line_id, spi_count)

        response.append({
            "line_name_id": line_id,
            "data_base_name": db_name,
            "status": status,
            "spi_count": spi_count,
            "last_fixed": new_last_fixed
        })
        
    asm_databases = fetch_ASM_databases(db)

    for item in asm_databases:
        result = handle_asm_line(db, item)
        response.append(result)

    return response


def add_cycles_to_sito(db, line_id, spi_count):
    db.execute(text("""
        UPDATE checkprocess_productobject
        SET sito_cycles_count = sito_cycles_count + :count
        WHERE current_place_id = :line_id
        AND "end" = FALSE
    """), {
        "count": spi_count,
        "line_id": line_id
    })

    db.commit()


def save_new_spi_logs(db, line_id, spi_rows, full_sn):
    # 1) ostatni fixed_id z naszej bazy
    last_fixed = db.execute(text("""
        SELECT MAX(fixed_id) 
        FROM checkprocess_logfromspi
        WHERE machine_name_id = :line_id
    """), {"line_id": line_id}).scalar()

    if last_fixed is None:
        last_fixed = 0

    if not spi_rows:
        return 0, last_fixed

    # 2) czas ostatniego loga (naszego)
    last_log_time = db.execute(text("""
        SELECT MAX(time_date)
        FROM checkprocess_logfromspi
        WHERE machine_name_id = :line_id
    """), {"line_id": line_id}).scalar()

    offline_too_long = False
    if last_log_time:
        now = datetime.now(last_log_time.tzinfo)
        delta = now - last_log_time
        offline_too_long = delta.total_seconds() > 300
    else:
        # brak logów = pierwszy start
        offline_too_long = True

    # 3) wykrycie nowego miesiąca (IDNO się zresetowało)
    spi_max_id = max(row["IDNO"] for row in spi_rows)
    if spi_max_id < last_fixed:
        last_fixed = 0

    # 4) bierzemy tylko nowe rekordy po last_fixed
    new_logs = [row for row in spi_rows if row["IDNO"] > last_fixed]
    if not new_logs:
        return 0, last_fixed

    # 5) tylko NAJNOWSZY log do walidacji + ewentualnie resetu
    newest_log = max(new_logs, key=lambda r: r["IDNO"])
    newest_pcb = newest_log["PCBNAME"]

    # 6) jeśli aplikacja była OFF > 300s → traktujemy jak start:
    #    ignorujemy cały gap i logujemy tylko ostatni stan
    if offline_too_long:
        print(f"[INFO] Offline >300s na linii {line_id}. Biorę tylko ostatni log IDNO={newest_log['IDNO']}.")
        new_logs = [newest_log]

    # 7) zapis do naszej bazy
    for row in new_logs:
        db.execute(text("""
            INSERT INTO checkprocess_logfromspi
            (fixed_id, pcb_name, time_date, machine_name_id, result)
            VALUES (:fixed_id, :pcb, NOW(), :machine, :result)
        """), {
            "fixed_id": row["IDNO"],
            "pcb": row["PCBNAME"],
            "machine": line_id,
            "result": row["RESULT"]
        })

    db.commit()

    newest_fixed = max(row["IDNO"] for row in new_logs)
    return len(new_logs), newest_fixed

def handle_no_product_case(db, line_id, spi_rows):
    last_log_time = db.execute(text("""
        SELECT MAX(time_date)
        FROM checkprocess_logfromspi
        WHERE machine_name_id = :line_id
    """), {"line_id": line_id}).scalar()

    if not spi_rows:
        return "ignore"

    newest_spi = max(spi_rows, key=lambda r: r["IDNO"])

    # --- SPI time always naive (local)
    spi_time = datetime.now()

    # brak naszych logów → ignore (pierwszy raz)
    if last_log_time is None:
        return "ignore"

    # --- normalizacja obu dat ---
    # jeśli last_log_time ma timezone → dodajemy timezone do spi_time
    if last_log_time.tzinfo is not None and last_log_time.tzinfo.utcoffset(last_log_time) is not None:
        spi_time = spi_time.replace(tzinfo=last_log_time.tzinfo)
    else:
        # jeśli last_log_time jest naive → oba muszą być naive
        last_log_time = last_log_time.replace(tzinfo=None)

    delta = spi_time - last_log_time
    seconds = delta.total_seconds()

    if seconds > 90:
        return "kill"

    return "ignore"

def get_kill_flag(db, line_id):
    return db.execute(text("""
        SELECT killing_flag
        FROM checkprocess_apptokill
        WHERE line_name_id = :line_id
    """), {"line_id": line_id}).scalar()



def handle_asm_line(db: Session, item):
    line_id = item["line_name_id"]
    db_host = item["data_base_name"]   # IP\SQLEXPRESS
    
    status = check_line_status(db, line_id)

    if status is None:
        # brak produktu na linii → pobieramy ASM
        spi_rows, spi_max = fetch_new_asm_logs(db_host, 0)
        
        decision = handle_no_product_case(db, line_id, spi_rows)
        if decision == "kill":
            set_kill_flag(db, line_id, True)
            return {
                "line_name_id": line_id,
                "data_base_name": db_host,
                "status": "BLAD — produkcja bez produktu! (ASM)",
                "spi_count": 0,
                "kill": True
            }

        return {
            "line_name_id": line_id,
            "data_base_name": db_host,
            "status": "Brak produktu — niedobitki ignoruje (ASM)",
            "spi_count": 0,
            "kill": False
        }

    # jeśli produkt istnieje

    last_fixed = db.execute(text("""
        SELECT MAX(fixed_id)
        FROM checkprocess_logfromspi
        WHERE machine_name_id = :line_id
    """), {"line_id": line_id}).scalar() or 0

    spi_rows, spi_max = fetch_new_asm_logs(db_host, last_fixed)

    spi_count, new_last_fixed = save_new_spi_logs(db, line_id, spi_rows, status)

    if spi_count > 0:
        add_cycles_to_sito(db, line_id, spi_count)

    return {
        "line_name_id": line_id,
        "data_base_name": db_host,
        "status": status,
        "spi_count": spi_count,
        "last_fixed": new_last_fixed,
        "asm": True
    }
