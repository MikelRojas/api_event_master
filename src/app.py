from flask import Flask, jsonify, request
from flask_cors import CORS
import os
from psycopg2 import OperationalError, pool
from dotenv import load_dotenv

def sign_in(email:str):
    try:
        load_dotenv()

        # Get the connection string from the environment variable
        connection_string = os.getenv('DATABASE_URL')

        # Create a connection pool
        connection_pool = pool.SimpleConnectionPool(1, 10, connection_string)
        conn = connection_pool.getconn()
        cur = conn.cursor()

        cur.execute(f"SELECT * FROM users WHERE email = '{email}';")
        data_users = cur.fetchone()

        # Close the cursor and connection
        cur.close()
        connection_pool.putconn(conn)
        connection_pool.closeall()
        
        return jsonify({"data":{"email":data_users[0],"name":data_users[1],"password":data_users[2]},"state":True}), 200
    
    except OperationalError as db_error:
        print(f"Database error: {db_error}")
        return jsonify({"data": "Database Error", "state": False}), 500
    
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"data":None,"state":False}), 400
    
def sign_in_supplier(email:str):
    try:
        load_dotenv()

        # Get the connection string from the environment variable
        connection_string = os.getenv('DATABASE_URL')

        # Create a connection pool
        connection_pool = pool.SimpleConnectionPool(1, 10, connection_string)
        conn = connection_pool.getconn()
        cur = conn.cursor()

        cur.execute(f"""
        SELECT 
            u.name,
            u.email,
            u.password,
            s.description,
            s.type,
            s.url_image
        FROM users u
          INNER JOIN 
            supplier s ON s.email = u.email
        WHERE u.email = '{email}';
        """)
        data_users = cur.fetchone()

        # Close the cursor and connection
        cur.close()
        connection_pool.putconn(conn)
        connection_pool.closeall()
        
        return jsonify({"data":{"name":data_users[0],"email":data_users[1],"password":data_users[2],"description":data_users[3],"type":data_users[4],"url_image":data_users[5]},"state":True}), 200
    
    except OperationalError as db_error:
        print(f"Database error: {db_error}")
        return jsonify({"data": "Database Error", "state": False}), 500
    
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"data":None,"state":False}), 400
    
def create_client(email:str,name:str,password:str):
    try:
        load_dotenv()

        # Get the connection string from the environment variable
        connection_string = os.getenv('DATABASE_URL')

        # Create a connection pool
        connection_pool = pool.SimpleConnectionPool(1, 10, connection_string)
        conn = connection_pool.getconn()
        cur = conn.cursor()

        cur.execute(f"INSERT INTO users(email,name,password) VALUES('{email}','{name}','{password}');")
        conn.commit()
        cur.close()
        connection_pool.putconn(conn)
        connection_pool.closeall()
        
        return jsonify({"data":"Insertion Succesful","state":True}), 201
    
    except OperationalError as db_error:
        print(f"Database error: {db_error}")
        return jsonify({"data": "Database Error", "state": False}), 500
    
    except Exception as e:
        # Muestra cualquier otro error no específico de la base de datos
        print(f"Error occurred: {e}")
        return jsonify({"data":"Insertion Field","state":False}), 400
    
def create_supplier(email: str, name: str, password: str, description: str, supplier_type: str, url_image: str):
    try:
        load_dotenv()

        connection_string = os.getenv('DATABASE_URL')

        connection_pool = pool.SimpleConnectionPool(1, 10, connection_string)
        conn = connection_pool.getconn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT add_supplier(%s, %s, %s, %s, %s, %s);
            """,
            (email, name, password, description, supplier_type, url_image)
        )

        conn.commit()
        cur.close()
        connection_pool.putconn(conn)
        connection_pool.closeall()
        
        return jsonify({"data": "Supplier added successfully", "state": True}), 201
    
    except OperationalError as db_error:
        print(f"Database error: {db_error}")
        return jsonify({"data": "Database Error", "state": False}), 500
    
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"data": "Insertion Failed", "state": False}), 400
    
def create_event(email_user: str, date_event: str, details: str, start_time: str, end_time: str, ubication: str):
    try:
        load_dotenv()

        # Obtiene la cadena de conexión desde la variable de entorno
        connection_string = os.getenv('DATABASE_URL')

        # Crea un pool de conexiones
        connection_pool = pool.SimpleConnectionPool(1, 10, connection_string)
        conn = connection_pool.getconn()
        cur = conn.cursor()

        # Inserta el nuevo evento en la tabla events
        cur.execute(
            """
            INSERT INTO events (email_user, date_event, details, start_time, end_time, ubication)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (email_user, date_event, details, start_time, end_time, ubication)
        )

        # Obtiene el ID del evento recién creado
        event_id = cur.fetchone()[0]
        conn.commit()  # Asegúrate de hacer commit para guardar los cambios

        # Cierra el cursor y devuelve la conexión al pool
        cur.close()
        connection_pool.putconn(conn)  # Devuelve la conexión al pool

        return jsonify({"data": {"event_id": event_id}, "state": True}), 201
    
    except OperationalError as db_error:
        print(f"Database error: {db_error}")
        return jsonify({"data": "Database Error", "state": False}), 500
    
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"data": "Insertion Failed", "state": False}), 400

    except OperationalError as db_error:
        print(f"Database error: {db_error}")
        return jsonify({"data": "Database Error", "state": False}), 500
    
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"data": "Insertion Failed", "state": False}), 400
    
def create_contracted_service(id_event: int, email_supplier: str, state: bool):
    try:
        load_dotenv()

        # Obtiene la cadena de conexión desde la variable de entorno
        connection_string = os.getenv('DATABASE_URL')

        # Crea un pool de conexiones
        connection_pool = pool.SimpleConnectionPool(1, 10, connection_string)
        conn = connection_pool.getconn()
        cur = conn.cursor()

        # Inserta un nuevo servicio contratado en la tabla contracted_service
        cur.execute(
            """
            INSERT INTO contracted_service (id_event, email_supplier, state)
            VALUES (%s, %s, %s);
            """,
            (id_event, email_supplier, state)
        )

        conn.commit()

        # Cierra el cursor y devuelve la conexión al pool
        cur.close()
        connection_pool.putconn(conn)
        connection_pool.closeall()
        
        return jsonify({"data": "Service contracted successfully", "state": True}), 201
    
    except OperationalError as db_error:
        print(f"Database error: {db_error}")
        return jsonify({"data": "Database Error", "state": False}), 500
    
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"data": "Insertion Failed", "state": False}), 400
    
def update_contracted_service(id_event: int, email_supplier: str, new_state: bool):
    try:
        load_dotenv()

        # Obtiene la cadena de conexión desde la variable de entorno
        connection_string = os.getenv('DATABASE_URL')

        # Crea un pool de conexiones
        connection_pool = pool.SimpleConnectionPool(1, 10, connection_string)
        conn = connection_pool.getconn()
        cur = conn.cursor()

        # Actualiza el estado del servicio contratado
        cur.execute(
            """
            UPDATE contracted_service
            SET state = %s
            WHERE id_event = %s AND email_supplier = %s;
            """,
            (new_state, id_event, email_supplier)
        )

        conn.commit()

        # Cierra el cursor y devuelve la conexión al pool
        cur.close()
        connection_pool.putconn(conn)
        connection_pool.closeall()
        
        return jsonify({"data": "Service state updated successfully", "state": True}), 200
    
    except OperationalError as db_error:
        print(f"Database error: {db_error}")
        return jsonify({"data": "Database Error", "state": False}), 500
    
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"data": "Update Failed", "state": False}), 400
    
def suppliers():
    try:
        load_dotenv()

        # Get the connection string from the environment variable
        connection_string = os.getenv('DATABASE_URL')

        # Create a connection pool
        connection_pool = pool.SimpleConnectionPool(1, 10, connection_string)
        conn = connection_pool.getconn()
        cur = conn.cursor()

        # Execute the query
        cur.execute("""
        SELECT 
            s.email,
            u.name,
            s.description,
            s.url_image,
            s.type
        FROM supplier s 
            JOIN 
            users u ON u.email = s.email;
        """)
        
        # Fetch all results
        rows = cur.fetchall()
        
        # Create a list of supplier dictionaries
        suppliers_list = []
        for row in rows:
            suppliers_list.append({
                "email": row[0],
                "name": row[1],
                "description": row[2],
                "url_image": row[3],
                "type": row[4]
            })

        # Close the cursor and connection
        cur.close()
        connection_pool.putconn(conn)
        connection_pool.closeall()
        
        return jsonify({"data": suppliers_list, "state": True}), 200

    except OperationalError as db_error:
        print(f"Database error: {db_error}")
        return jsonify({"data": "Database Error", "state": False}), 500

    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"data": None, "state": False}), 400
    
def get_user_events(email_user:str):
    if not email_user:
        return jsonify({"data": "Email del usuario es requerido", "state": False}), 400
    
    try:
        load_dotenv()
        connection_string = os.getenv('DATABASE_URL')
        connection_pool = pool.SimpleConnectionPool(1, 10, connection_string)
        conn = connection_pool.getconn()
        cur = conn.cursor()
        
        # Consulta SQL para obtener eventos y proveedores
        cur.execute(f"""
            SELECT e.id, e.email_user, e.details AS type, e.date_event AS date, 
                   e.start_time, e.end_time, e.ubication,
                   cs.email_supplier, cs.state
            FROM events e
            LEFT JOIN contracted_service cs ON e.id = cs.id_event
            WHERE e.email_user = '{email_user}';
        """)
        
        # Procesa los resultados y organiza la estructura de datos
        events_dict = {}
        for row in cur.fetchall():
            event_id = row[0]
            if event_id not in events_dict:
                events_dict[event_id] = {
                    "id": str(event_id),
                    "email_user": row[1],
                    "type": row[2],
                    "date": row[3],
                    "start_time": row[4].isoformat(),
                    "end_time": row[5].isoformat(),
                    "ubication": row[6],
                    "suppliers": []
                }
            # Agrega los proveedores a cada evento
            events_dict[event_id]["suppliers"].append({
                "email_supplier": row[7],
                "state": row[8]
            })
        
        # Convierte el diccionario a una lista para devolverlo en JSON
        events_list = list(events_dict.values())

        # Cierra el cursor y la conexión
        cur.close()
        connection_pool.putconn(conn)
        
        return jsonify({"data": events_list, "state": True}), 200

    except OperationalError as db_error:
        print(f"Database error: {db_error}")
        return jsonify({"data": "Database Error", "state": False}), 500

    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"data": None, "state": False}), 400
    
def get_supplier_events(email_user: str):
    if not email_user:
        return jsonify({"data": "Email del usuario es requerido", "state": False}), 400
    
    try:
        load_dotenv()
        connection_string = os.getenv('DATABASE_URL')
        connection_pool = pool.SimpleConnectionPool(1, 10, connection_string)
        conn = connection_pool.getconn()
        cur = conn.cursor()
        
        # Consulta SQL para obtener eventos y proveedores
        cur.execute("""
            SELECT 
                e.id, 
                e.email_user, 
                e.details AS type, 
                e.date_event AS date, 
                e.start_time, 
                e.end_time, 
                e.ubication,
                cs.email_supplier,
                cs.state
            FROM contracted_service cs
            LEFT JOIN events e ON e.id = cs.id_event
            WHERE cs.email_supplier = %s;
        """, (email_user,))
        
        # Procesar resultados y construir la lista de eventos
        events_list = []
        for row in cur.fetchall():
            event = {
                "id": row[0],
                "email_user": row[1],
                "type": row[2],
                "date": row[3].strftime("%Y-%m-%d"),  # Formatear fecha
                "start_time": row[4].strftime("%H:%M:%S"),  # Formatear hora de inicio
                "end_time": row[5].strftime("%H:%M:%S"),    # Formatear hora de fin
                "ubication": row[6],
                "state": row[8]
            }
            events_list.append(event)
        
        cur.close()
        connection_pool.putconn(conn)
        
        return jsonify({"data": events_list, "state": True}), 200

    except OperationalError as db_error:
        print(f"Database error: {db_error}")
        return jsonify({"data": "Database Error", "state": False}), 500

    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({"data": None, "state": False}), 400

def create_app():
    
    app = Flask(__name__)
    CORS(app)

    # Ruta de bienvenida
    @app.route('/', methods=['GET'])
    def home():
        return jsonify({"mensaje": "Bienvenido a mi API con Flask"}), 200

    # Obtener todos los usuarios
    @app.route('/getuser', methods=['GET'])
    def get_user():
        email = request.args.get('email')
        return sign_in(email)

    @app.route('/getsupplier', methods=['GET'])
    def get_supplier():
        email = request.args.get('email')
        return sign_in_supplier(email)

    @app.route('/getsuppliers', methods=['GET'])
    def get_suppliers():
        return suppliers()

    @app.route('/get_user_events', methods=['GET'])
    def getUserEvents():
        email_user = request.args.get('email_user')
        return get_user_events(email_user)

    @app.route('/get_supplier_events', methods=['GET'])
    def getSupplierEvents():
        email_user = request.args.get('email_user')
        return get_supplier_events(email_user)

    @app.route('/create_client', methods=['POST'])
    def create_new_user():
        data_user = request.get_json()
        email = data_user.get("email")
        name = data_user.get("name")
        password = data_user.get("password")
        return create_client(email,name,password)

    @app.route('/create_supplier', methods=['POST'])
    def create_new_supplier():
        data_user = request.get_json()
        email = data_user.get("email")
        name = data_user.get("name")
        password = data_user.get("password")
        description = data_user.get("description")
        supplier_type = data_user.get("supplier_type")
        url_image = data_user.get("url_image", "")
        return create_supplier(email,name,password,description,supplier_type,url_image)
        
    @app.route('/create_event', methods=['POST'])
    def create_new_event():
        event_data = request.get_json()
        email_user = event_data.get("email_user")
        date_event = event_data.get("date_event")
        details = event_data.get("details")
        start_time = event_data.get("start_time")
        end_time = event_data.get("end_time")
        ubication = event_data.get("ubication")

        return create_event(email_user, date_event, details, start_time, end_time, ubication)

    @app.route('/create_contracted_service', methods=['POST'])
    def create_new_contracted_service():
        service_data = request.get_json()
        id_event = service_data.get("id_event")
        email_supplier = service_data.get("email_supplier")
        state = service_data.get("state")

        return create_contracted_service(id_event, email_supplier, state)

    @app.route('/update_contracted_service', methods=['POST'])
    def update_new_contracted_service():
        service_data = request.get_json()
        id_event = service_data.get("id_event")
        email_supplier = service_data.get("email_supplier")
        new_state = service_data.get("new_state")

        return update_contracted_service(id_event, email_supplier, new_state)
    
    return app


# Ejecutar la aplicación
if __name__ == '__main__':
    app =  create_app()
    app.run(host="0.0.0.0", port=5000, debug=True)

