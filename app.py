from flask import Flask

from routes.main_routes import main_routes
from routes.file_routes import file_routes
from routes.log_routes import log_routes
from routes.matrix_routes import matrix_routes

from database.db import init_db

app = Flask(__name__)

app.register_blueprint(main_routes)
app.register_blueprint(file_routes)
app.register_blueprint(log_routes)
app.register_blueprint(matrix_routes)

if __name__ == "__main__":
    init_db()
    app.run(debug=True)