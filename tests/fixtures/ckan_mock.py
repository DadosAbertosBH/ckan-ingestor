import logging
from threading import Thread
from uuid import uuid4

import pytest
from flask import Flask, jsonify, request
from flask_cors import CORS

INVALID_INPUT_JSON_ID = 'e3bce367-2e62-41c2-840f-b1df6255e7e5'


@pytest.fixture(scope='session')
def ckman_mock_url():
    def add_callback_response(url, callback, methods=('GET',)):
        callback.__name__ = str(uuid4())  # change name of method to mitigate flask exception
        app.add_url_rule(url, view_func=callback, methods=methods)

    def add_json_response(url, serializable, methods=('GET',)):
        def callback():
            return jsonify(serializable)

        add_callback_response(url, callback, methods=methods)

    # Create app with shutdown hook
    app = Flask(__name__)
    CORS(app)
    app.env = 'development'
    app.testing = True
    app.logger.setLevel(logging.INFO)

    @app.route('/datastore/<string:resource_id>')
    def ping(resource_id: str):

        if resource_id == INVALID_INPUT_JSON_ID:
            format_param = request.args.get('format')
            if format_param == 'json':
                return '''
                    {
                      "fields": [{"id":"_id"}],
                      "records": [
                          [416000],
                          [416001],                
                '''
                # return broken json
            return f"x\nfrom_csv"  # return CSV

        return jsonify({"fields": [{"id": "_id", "type": "string"}], "records": [resource_id]})

    thread = Thread(target=app.run, daemon=True, kwargs=dict(host='localhost', port=5001))
    thread.start()

    yield "http://localhost:5001"
