import pandas as pd
import pickle
import numpy as np
import time

from flask import Flask, jsonify, request

from bin.model.payload_processing import PayloadProcessing

from utils.api import API
from utils.var import File

app = Flask(__name__)
app.secret_key = API().key(service='app', category='price_pred')

FEATURES = ['city', 'street', 'building', 'zipcode', 'floor_number', 'rums', 'area', 'utils', 'build_year']

MODEL = pickle.load(open(File.MODEL, 'rb'))


@app.route('/api', methods=['GET'])
def api():
    """Handle request and output model score in json format."""
    # Measure execution time
    start_time = time.time()
    # Handle empty requests
    if not request.json:
        return jsonify({'error': 'no request received'})
    # Parse request args
    x_list, missing_data = parse_args(request.json)
    x_array = np.array([x_list])
    # Enrich data with variable queries and feature engineering
    x = PayloadProcessing().enrichment(pd.DataFrame(x_array, columns=FEATURES))
    # Predict on x and return JSON response
    estimate = int(np.expm1(MODEL.predict(x))[0])
    response = dict(ESTIMATED_PRICE=estimate, MISSING_DATA=missing_data, EXECUTION_TIME=time.time() - start_time)
    return jsonify(response)


def parse_args(request_dict):
    """Parse model features from incoming requests formatted in JSON."""
    # Initialize missing_data as False.
    missing_data = False
    # Parse out the features from the request_dict.
    x_list = []
    for feature in FEATURES:
        value = request_dict.get(feature, None)
        if value:
            x_list.append(value)
        else:
            # Handle missing features.
            x_list.append(0)
            missing_data = True
    return x_list, missing_data


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
