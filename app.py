import sys
sys.path.append("/Project/src/")

import pandas as pd
import pickle
import numpy as np

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
    # Handle empty requests.
    if not request.json:
        return jsonify({'error': 'no request received'})
    # Parse request args into feature array for prediction
    data = pd.DataFrame([request])
    x = PayloadProcessing().enrichment(data)
    # Predict on x and return JSON response
    estimate = int(np.expm1(MODEL.predict(x))[0])
    response = dict(ESTIMATE=estimate)
    return jsonify(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
