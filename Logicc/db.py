import mysql.connector


def get_server_connection():
    return mysql.connector.connect(host="localhost", port=3306, user="root", password="")


def get_connection():
    return mysql.connector.connect(
        host="localhost", port=3306, user="root", password="",
        database="ecommerce_streaming"
    )


def crear_bd():
    conn = get_server_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE DATABASE IF NOT EXISTS ecommerce_streaming
            CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci
        """)
        print("Base de datos creada correctamente.")
    finally:
        cursor.close()
        conn.close()


def crear_tabla():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS eventos_carritos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                tipo_evento VARCHAR(255) NOT NULL,
                usuario VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL,
                producto VARCHAR(255) NOT NULL,
                cantidad INT NOT NULL,
                precio DECIMAL(10,2) NOT NULL,
                fecha DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS notificaciones (
                id INT AUTO_INCREMENT PRIMARY KEY,
                usuario VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL,
                productos VARCHAR(500) NOT NULL,
                valor_carrito DOUBLE NOT NULL,
                mensaje VARCHAR(500) NOT NULL,
                fecha DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def guardar_evento(filas):
    """filas: lista de tuplas (tipo_evento, usuario, email, producto, cantidad, precio, fecha)"""
    if not filas:
        return
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany(
            "INSERT INTO eventos_carritos (tipo_evento, usuario, email, producto, cantidad, precio, fecha) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            filas
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def guardar_notificacion(filas):
    if not filas:
        return
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany(
            "INSERT INTO notificaciones (usuario, email, productos, valor_carrito, mensaje) "
            "VALUES (%s, %s, %s, %s, %s)",
            filas
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def obtener_notificacion(limite=20):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            "SELECT usuario, email, productos, valor_carrito, mensaje "
            "FROM notificaciones ORDER BY id DESC LIMIT %s", (limite,)
        )
        filas = cursor.fetchall()
        return list(reversed(filas))
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    crear_bd()
    crear_tabla()