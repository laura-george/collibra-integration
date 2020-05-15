# test data exists in domain
# change attributes in collibra (attibutes on tables and columns)
# run export script
# get datasets where changes have been made and see if those changes are there
import json
import requests
import yaml
import thriftpy
import sys
import os
import logging
from okera import context
import pprint

with open('config.yaml') as f:
    configs = yaml.load(f, Loader=yaml.FullLoader)['configs']

with open(configs['collibra_assets']) as f:
    update_assets = yaml.load(f, Loader=yaml.FullLoader)

comm = update_assets['communities']
community = comm[0]['name']
community_id = comm[0]['id']

attribute_ids = [{'name': "Description", 'id': "00000000-0000-0000-0000-000000003114"}]

if configs['mapped_collibra_attributes'] != None:
  for attr in configs['mapped_collibra_attributes']:
    attribute_ids.append({'name': attr['attribute_name'], 'id': attr['attribute_id']})

# logging configuration
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='export_test.log', level=logging.DEBUG)

# PyOkera context
ctx = context()
ctx.enable_token_auth(token_str=configs['okera_token'])

def collibra_get(param_obj, call, method, header=None):
    if method == 'get':
        try:
            logging.info("### START: Making Collibra request ###")
            data = getattr(requests, method)(
            configs['collibra_dgc'] + "/rest/2.0/" + call,
            params=param_obj,
            auth=(configs['collibra_username'], configs['collibra_password']))
            data.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if json.loads(data.content).get('errorCode') != "mappingNotFound":
                logging.warning("COLLIBRA CORE API ERROR")
                logging.warning("Request body: " + str(param_obj))
                logging.warning("Error: " + repr(e))
                logging.warning("Response: " + str(data.content))
        logging.info("Request successful")
        logging.info("### END: Making Collibra request ###")
        return data.content
    else:
        try:
            logging.info("### START: Making Collibra request ###")
            data = getattr(requests, method)(
            configs['collibra_dgc'] + "/rest/2.0/" + call,
            headers=header,
            json=param_obj,
            auth=(configs['collibra_username'], configs['collibra_password']))
            data.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.warning("COLLIBRA CORE API ERROR")
            logging.warning("Request body: " + str(param_obj))
            logging.warning("Error: " + repr(e))
            logging.warning("Response: " + str(data.content))
        logging.info("Request successful")
        logging.info("### END: Making Collibra request ###")
        return data.content

for dom in comm[0]['domains']:
    for table in dom['tables']:
        asset_params = {
            'name': table,
            'nameMatchMode': "EXACT",
            'communityId': community_id,
            'domainId': dom['id']
        }
asset_id = json.loads(collibra_get(asset_params, "assets", 'get'))['results'][0]['id']

# sync over table attributes including status
def add_attributes():
    os.system('python3 export.py')
    type_ids = []
    for a in attribute_ids:
        type_ids.append(a['id'])
    attr_params = {
        'typeIds': type_ids,
        'assetId': asset_id
    }
    print(attr_params)
    print(collibra_get(attr_params, "attributes", "get"))
    # get status
    # return grouped attributes and status
    # take mapped namespaces from config?

add_attributes()

def add_attributes_answer():
    for dom in comm[0]['domains']:
        domain = dom['name']
        domain_id = dom['id']
        for table in dom['tables']:
            try:
                conn = ctx.connect(host = configs['okera_host'], port = planner_port)
            except thriftpy.transport.TException as e:
                logging.error("\tPYOKERA ERROR")
                logging.error("\tCould not connect to Okera!")
                logging.error("\tError: " + repr(e))
            with conn:
                tables = conn.list_datasets(db=table.split(".")[0], name=table.split(".")[1])
                # gather attributes and status in same format as main function
