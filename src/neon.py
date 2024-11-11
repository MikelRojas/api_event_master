import os
from psycopg2 import OperationalError, pool
from dotenv import load_dotenv
from flask import jsonify

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