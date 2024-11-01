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
        conn.commit()

        # Cierra el cursor y devuelve la conexión al pool
        cur.close()
        connection_pool.putconn(conn)
        connection_pool.closeall()
        
        return jsonify({"data": {"event_id": event_id}, "state": True}), 201
    
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