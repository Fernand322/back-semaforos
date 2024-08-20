import serial
import pymongo
from datetime import datetime
from pymongo.errors import PyMongoError
import time
import uuid
from flask import Flask, jsonify
import threading

# Reemplaza con tus credenciales
username = "martin97"
password = "martin123456"
cluster_address = "cluster0.mongodb.net"
database_name = "arduino_db"
nodo_collections = {
    "nodo_1": "nodo_1_collection",
    "nodo_2": "nodo_2_collection",
    "nodo_3": "nodo_3_collection"
}
fallos_collection_name = "fallos_collection"

# Crear la cadena de conexión
#connection_string = f"mongodb+srv://{username}:{password}@{cluster_address}/?retryWrites=true&w=majority&appName=Cluster0"

connection_string = f"mongodb+srv://martin97:martin123456@cluster0.ymblkgt.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
mongo_client = pymongo.MongoClient(connection_string)
db = mongo_client[database_name]
nodo_collections = {nodo: db[col] for nodo, col in nodo_collections.items()}
fallos_collection = db[fallos_collection_name]

# Configura la conexión serial
serial_port = 'COM4'  # Reemplaza 'COM4' con tu puerto serial
baud_rate = 115200

# Crear la aplicación Flask
app = Flask(__name__)

# Bandera para detectar "PAYLOAD RECIBIDO OK"
payload_received = False

# Método para validar la secuencia de encendido
def validar_secuencia():
    print("Iniciando validación de secuencia...")
    try:
        for nodo, collection in nodo_collections.items():
            documentos = list(collection.find().sort([
                ("fecha_hora", pymongo.ASCENDING),
                ("_id", pymongo.ASCENDING)  # Ordenar por ID para asegurar el orden de inserción
            ]))

            secuencia_correcta = True
            ciclo_tiempo = 133  # Duración del ciclo en segundos

            for i in range(1, len(documentos)):
                estado_anterior = documentos[i-1]['estado']
                estado_actual = documentos[i]['estado']
                tiempo_anterior = documentos[i-1]['fecha_hora']
                tiempo_actual = documentos[i]['fecha_hora']

                # Verificar secuencia de colores
                if (estado_anterior == "Rojo" and estado_actual != "Amarillo") or \
                   (estado_anterior == "Amarillo" and estado_actual != "Verde") or \
                   (estado_anterior == "Verde" and estado_actual != "Rojo"):
                    secuencia_correcta = False
                    print(f"Secuencia incorrecta detectada en {nodo} entre {estado_anterior} y {estado_actual} a las {documentos[i]['fecha_hora']}")

                    # Registrar el fallo en la colección de fallos
                    fallo = {
                        'nodo': nodo,
                        'estado_incorrecto': estado_actual,
                        'estado_anterior': estado_anterior,
                        'fecha_hora': tiempo_actual
                    }
                    fallos_collection.insert_one(fallo)

                # Verificar que el ciclo se repita correctamente cada 133 segundos
                diferencia_tiempo = (tiempo_actual - tiempo_anterior).total_seconds()
                # if i % 3 == 0:  # Cada ciclo completo debe durar 133 segundos
                #     if diferencia_tiempo > ciclo_tiempo + 5 or diferencia_tiempo < ciclo_tiempo - 5:
                #         secuencia_correcta = False
                #         print(f"Error en el ciclo de tiempo en {nodo} entre {tiempo_anterior} y {tiempo_actual}. Duración: {diferencia_tiempo} segundos")

                #         # Registrar el fallo en la colección de fallos
                #         fallo = {
                #             'nodo': nodo,
                #             'error_tiempo': diferencia_tiempo,
                #             'fecha_hora': tiempo_actual
                #         }
                #         fallos_collection.insert_one(fallo)

            if secuencia_correcta:
                print(f"La secuencia de encendido en {nodo} es correcta.")
            else:
                print(f"Se detectaron errores en la secuencia de encendido en {nodo}.")

    except PyMongoError as e:
        print(f"Error al acceder a MongoDB: {e}")

# Función para ejecutar la validación de secuencia en un bucle
def ejecutar_validacion():
    while True:
        validar_secuencia()
        time.sleep(266)

# Iniciar un hilo para la validación de la secuencia
validacion_thread = threading.Thread(target=ejecutar_validacion)
validacion_thread.start()

# Ruta API para consultar el estado de los semáforos y fallos
@app.route('/estado-semaforos', methods=['GET'])
def obtener_estado_semaforos():
    try:
        respuesta = {"semaforos": {}, "fallos": [], "analisis": []}

        # Consultar los estados actuales de los nodos
        for nodo, collection in nodo_collections.items():
            estados = list(collection.aggregate([
                {"$sort": {"nodo": 1, "fecha_hora": -1}},
                {"$group": {"_id": "$nodo", "estado": {"$first": "$estado"}, "fecha_hora": {"$first": "$fecha_hora"}}}
            ]))

            nodo_respuesta = []
            for estado in estados:
                nodo_respuesta.append({
                    "estado": estado["estado"],
                    "fecha_hora": estado["fecha_hora"].strftime('%Y-%m-%d %H:%M:%S')
                })
            respuesta["semaforos"][nodo] = nodo_respuesta

        # Consultar los fallos
        fallos = list(fallos_collection.find().sort([("fecha_hora", pymongo.DESCENDING)]))
        error_count = {}

        for fallo in fallos:
            nodo = fallo["nodo"]
            estado_incorrecto = fallo.get("estado_incorrecto", "N/A")
            key = f"{nodo}_{estado_incorrecto}"

            if key not in error_count:
                error_count[key] = 1
            else:
                error_count[key] += 1

            if error_count[key] > 3:
                respuesta["analisis"].append({
                    "mensaje": f"El color {estado_incorrecto} en el nodo {nodo} está fallando repetidamente.",
                    "nodo": nodo,
                    "estado_incorrecto": estado_incorrecto
                })

            respuesta["fallos"].append({
                "nodo": fallo["nodo"],
                "fecha_hora": fallo["fecha_hora"].strftime('%Y-%m-%d %H:%M:%S'),
                "estado_incorrecto": estado_incorrecto,
                "error_tiempo": fallo.get("error_tiempo", "N/A")
            })

        return jsonify(respuesta)

    except PyMongoError as e:
        return jsonify({"error": f"Error al acceder a MongoDB: {e}"}), 500


# Leer y guardar datos en bucle
try:
    while True:
        if ser.in_waiting > 0:
            sensor_value = ser.readline().decode('utf-8').strip()
            print(f"Valor recibido: {sensor_value}")

            if payload_received:
                try:
                    nodo, estado, fecha, hora = sensor_value.split(',')
                    fecha_hora = datetime.strptime(f"{fecha} {hora}", '%Y-%m-%d %H:%M:%S')

                    if nodo in nodo_collections:
                        # Generar un ID único
                        unique_id = str(uuid.uuid4())

                        data = {
                            '_id': unique_id,  # ID único para el registro
                            'nodo': nodo,
                            'estado': estado,
                            'fecha_hora': fecha_hora
                        }
                        nodo_collections[nodo].insert_one(data)
                        print(f"Datos guardados en MongoDB para {nodo} con ID {unique_id}")
                    else:
                        print(f"Nodo {nodo} no reconocido")

                except ValueError as e:
                    print(f"Error al parsear los datos: {e}")

                payload_received = False
            elif sensor_value == "PAYLOAD RECIBIDO OK":
                payload_received = True
                print("Mensaje de control recibido, esperando el próximo valor para subir a MongoDB")

except PyMongoError as e:
    print(f"Error de MongoDB: {e}")
except Exception as e:
    print(f"Ocurrió otro error: {e}")
finally:
    ser.close()
    print("Conexión serial cerrada")

# Iniciar la API
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)
