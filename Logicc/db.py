import mysql.connector

def get_server_connection():
    return mysql.connector.connect(
        host="localhost",
        port=3306,
        user="root",
        password=""
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

def get_connection():
    return mysql.connector.connect(
        host="localhost",
        port=3306,
        user="root",
        password="",
        database="ecommerce_streaming"
    )

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
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def guardar_evento(tipo_evento, usuario, email, producto, cantidad, precio, fecha):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO eventos_carritos (tipo_evento, usuario, email, producto, cantidad, precio, fecha) " 
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (tipo_evento, usuario, email, producto, cantidad, precio, fecha)
        )
        conn.commit() 
    finally:
        cursor.close()
        conn.close()

def limpiar_eventos():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM eventos_carritos")
        conn.commit()
    finally:
        cursor.close()
        conn.close()
        
if __name__ == '__main__':
    crear_bd()
    crear_tabla()
