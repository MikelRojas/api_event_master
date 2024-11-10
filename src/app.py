from flask import Flask, jsonify, request
from neon import sign_in, suppliers, create_client, create_supplier, create_event, create_contracted_service, update_contracted_service
from flask_cors import CORS

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

@app.route('/getsuppliers', methods=['GET'])
def get_suppliers():
    return suppliers()

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


# Ejecutar la aplicación
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)

