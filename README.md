# **EJERCICIO CARRITOS ABANDONADOS CON KAFKA Y SPARK**
Este ejercicio se basa en mostrar como funciona el streaming de carritos de compras en un ecommerce, mostrando el proceso entre consumidor y productor e identificando cuando un carrito de compras es abandonado.

### ROLES
- **Productor**: En este ejercicio existen 2 productores que se encargan de enviarle toda la información del usuario como: usuario, correo, productos que agregó al carrito y valor correspondiente. Toda esta información se genera de manera aleatoria por medio de la libreria: 

    ```python
    from faker import Faker
    ```
    Ademas, se utiliza diferentes eventos para que el consumidor determine que carrito está abandonado:
    
    ```python
    TIPO_EVENTOS = ['agregar_al_carrito', 'iniciar_pago']
    TIPO_EVENTOS = ['pago_exitoso', 'pago_fallido', 'pago_rechazado']
    ```
- **Consumidor**: La funcion del consumidor en este ejercicio es recibir la información enviada por el productor, subirla a una base de datos MySql y luego identificar por medio de ventanas de sesiones que carrito se identifica como abandonado y simular el proceso de envio de correos a cada usuario.

### TECNOLOGIAS UTILIZADAS 

- **Apache Spark**: Spark es utilizado para manejar el gran volumen de datos que se van a recibir y detectar patrones (determinar que carrito fue abandonado). Tambien, con spark podemos utilizar las ventanas de sesiones.
    ```python
    GAP_SESION = "20 seconds" 
    ```
    Las ventanas de sesiones nos permite identificar el tiempo en que el usuario dejó de realizar algun proceso o dejó inactiva su sesión. Con esto podemos determinar en que punto el usuario agregó productos a su carrito y no volvió a realizar ningun proceso.
    
- **Kafka**: Kafka nos permite la comunicación entre si (Productor y consumidor), tambien utiliza los TOPIC, en donde se organizan los mensajes o eventos de cada productor. Kafka utliza su propio servidor llamado BROKER, en donde se almacena la información que fue enviada al consumidor mientras este ocupado, esto permite que los datos enviados no se pierdan si la base de datos de cae.
- **Flask**: En este ejercicio se utiliza flask para poder enviar y actualizar toda la información en el frontend de html, las conexiones de flask se crearon en cada uno de los productores indicandoles que evento va a ser mostrado.
    ```python
    app = Flask(__name__,)
    @app.route('/')
    def web():
        return render_template('Index.html')
    ```
- **MySql**: Modelo de bases de datos relacional la cual se utilizó para guardar cada uno de los registros de los carritos, por medio de consultas como:
    ```python
    def guardar_evento(filas):
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
    ```

    # <img src="Desing/Img/pk.gif">