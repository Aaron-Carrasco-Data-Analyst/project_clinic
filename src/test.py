from src.utils import get_conn_strings, get_connection

def test_oltp():
    oltp, _ = get_conn_strings()
    conn = get_connection(oltp)
    cursor = conn.cursor()
    cursor.execute("SELECT TOP 5 * FROM pacientes")
    rows = cursor.fetchall()
    conn.close()
    print("OLTP conectado ✅")
    for row in rows:
        print(row)

def test_dw():
    _, dw = get_conn_strings()
    conn = get_connection(dw)
    cursor = conn.cursor()
    cursor.execute("SELECT TOP 5 * FROM dim_pacientes")
    rows = cursor.fetchall()
    conn.close()
    print("DW conectado ✅")
    for row in rows:
        print(row)

if __name__ == "__main__":
    test_oltp()
    test_dw()