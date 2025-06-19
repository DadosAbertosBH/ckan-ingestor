import logging
import os
from threading import Thread

import pytest
from flask import Flask, request
from flask_cors import CORS

INVALID_INPUT_JSON_ID = "e3bce367-2e62-41c2-840f-b1df6255e7e5"


@pytest.fixture(scope="session")
def ckman_mock_url():
    # Create app with shutdown hook
    app = Flask(__name__)
    CORS(app)
    app.env = "development"
    app.testing = True
    app.logger.setLevel(logging.INFO)

    @app.route("/datastore/<string:resource_id>")
    def get(resource_id: str):
        format_param = request.args.get("format")
        offset = request.args.get("offset", default="0")

        if int(offset) > 0:
            return {"fields": [{"id": "_id", "type": "int"}], "records": []}

        module_directory = os.path.dirname(os.path.abspath(__file__))
        file = os.path.join(module_directory, "data", f"{resource_id}.{format_param}")
        with open(file) as f:
            return f.read()

    thread = Thread(
        target=app.run, daemon=True, kwargs=dict(host="localhost", port=5001)
    )
    thread.start()

    yield "http://localhost:5001"
