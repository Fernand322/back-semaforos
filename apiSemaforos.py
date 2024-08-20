from flask import Flask, jsonify
import pymongo
from pymongo.errors import PyMongoError
import os

# Crear la aplicación Flask
app = Flask(__name__)

# Configurar la conexión a MongoDB usando variables de entorno
MONGO_URI = os.getenv('MONGO_URI')
mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client["arduino_db"]

# Definir las colecciones de nodos y de fallos
nodo_collections = {
    "nodo_1": db["nodo_1_collection"],
    "nodo_2": db["nodo_2_collection"],
    "nodo_3": db["nodo_3_collection"]
}
fallos_collection = db["fallos_collection"]

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

# Iniciar la API
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
