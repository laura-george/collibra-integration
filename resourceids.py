# This file contains all Collibra resource 'id's required for the export Collibra --> Okera
#
# DO NOT EDIT
#
import yaml
import logging

logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='export.log',level=logging.DEBUG)

try:
    logging.info("Opening config.yaml")
    with open('config_export.yaml') as f:
        configs = yaml.load(f, Loader=yaml.FullLoader)['configs']
except yaml.YAMLError as e:
    logging.error("Error in config.yaml: " + str(e))
for c in configs:
    if configs[c] == "":
        logging.error("Empty value for key '" + c + "' in config.yaml!")
        logging.info("Could not start export, terminating script")
        print("Export failed! For more information check export.log.")
        sys.exit(1)
for d in configs['domain']:
    if configs['domain'][d] == "":
        logging.error("Empty 'domain' value for key '" + d + "' in config.yaml!")
        logging.info("Could not start export, terminating script")
        print("Export failed! For more information check export.log.")
        sys.exit(1)

attribute_ids = [
  {'name': "Description", 'id': "00000000-0000-0000-0000-000000003114"},
  {'name': "Location", 'id': "00000000-0000-0000-0000-000000000203"},
  {'name': "Technical Data Type", 'id': "00000000-0000-0000-0000-000000000219"},
  {'name': "Data Type", 'id': "00000000-0000-0000-0001-000500000005"}
]

if configs['mapped_attribute_okera_namespaces'] != None:
  for attr in configs['mapped_attribute_okera_namespaces']:
    attribute_ids.append({'name': attr['attribute_name'], 'id': attr['attribute_id']})

asset_ids = [
  {'name': "Column", 'id': "00000000-0000-0000-0000-000000031008"},
  {'name': "Database", 'id': "00000000-0000-0000-0000-000000031006"},
  {'name': "Table", 'id': "00000000-0000-0000-0000-000000031007"}
]

relation_ids = [
  {'head': "Table", 'role': "is part of", 'tail': "Database", 'id': "00000000-0000-0000-0000-000000007045"},
  {'head': "Column", 'role': "is part of", 'tail': "Table", 'id': "00000000-0000-0000-0000-000000007042"}
]