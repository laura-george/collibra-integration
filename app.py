import requests
from flask import Flask, request, json, jsonify
import os
from config import configs

app = Flask(__name__)

integration = open('./integration.json', 'w+')

# CORE API
@app.route('/auth', methods=['POST'])
def post_auth(): 
    auth = {"username": configs.get('collibra username'), "password": configs.get('collibra password')}
    data = requests.post('https://okera.collibra.com:443/rest/2.0/auth/sessions', json = auth).content
    return data

# IMPORT API
@app.route('/import', methods=['POST'])
def import_data():
    files = {'file': open('integration.json', 'rb')}
    params = {
        "sendNotification": True, 
        "batchSize": "10000", 
        "simulation": False,
        "field": None,
        "file": integration, 
        "fileName": "test",
        "deleteFile": False
        }
    data = requests.post('https://okera.collibra.com:443/rest/2.0/import/json-job',
     data = params, auth = (configs.get('collibra username'), configs.get('collibra password')), files = files).content

    return data

@app.route('/sync', methods=['POST'])
def sync_data():
    files = {'file': open('integration.json', 'rb')}
    params = {
        "sendNotification": True, 
        "batchSize": "10000", 
        "simulation": False,
        "field": None,
        "file": integration, 
        "fileName": "test",
        "deleteFile": False
        }
    data = requests.post('https://okera.collibra.com:443/rest/2.0/import/synchronize/okera1/json-job',
     data = params, auth = (configs.get('collibra username'), configs.get('collibra password')), files = files).content

    return data

app.run()