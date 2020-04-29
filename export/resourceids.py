# This file contains all Collibra resource 'id's required for the export Collibra --> Okera
#
# DO NOT EDIT
#
import yaml
import logging
import sys
import os

CONF = os.getenv('CONF')

def error_out():
    logging.info("Could not start export, terminating script")
    print("Export failed! For more information check export.log.")
    sys.exit(1)

try:
    if CONF:
        logging.info("Opening " + str(CONF))
        config = str(CONF)
    else:
        logging.info("Opening config.yaml")
        config = 'config.yaml'
    with open(config) as f:
        configs = yaml.load(f, Loader=yaml.FullLoader)['configs']
except yaml.YAMLError as e:
    logging.error("Error in '{config}': " + repr(e))
    error_out()
except FileNotFoundError as e:
    logging.error("Config file {config} not found! To set the location run $ export CONF=path/to/config.yaml".format(config=config))
    error_out()

log_file = os.path.join(configs['log_directory'], 'export.log')
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename=log_file, level=logging.DEBUG)

attribute_ids = [
  {'name': "Description", 'id': "00000000-0000-0000-0000-000000003114"},
  {'name': "Location", 'id': "00000000-0000-0000-0000-000000000203"},
  {'name': "Technical Data Type", 'id': "00000000-0000-0000-0000-000000000219"},
  {'name': "Data Type", 'id': "00000000-0000-0000-0001-000500000005"}
]

if configs['mapped_collibra_attributes'] != None:
  for attr in configs['mapped_collibra_attributes']:
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

status_ids = [
  {'name': "Accepted", 'id': "00000000-0000-0000-0000-000000005009"},
  {'name': "Access Granted", 'id': "00000000-0000-0000-0000-000000005024"},
  {'name': "Approval Pending", 'id': "00000000-0000-0000-0000-000000005023"},
  {'name': "Approved", 'id': "00000000-0000-0000-0000-000000005025"},
  {'name': "Candidate", 'id': "00000000-0000-0000-0000-000000005008"},
  {'name': "Deployed", 'id': "00000000-0000-0000-0000-000000005053"},
  {'name': "Disabled", 'id': "00000000-0000-0000-0000-000000005052"},
  {'name': "Enabled", 'id': "00000000-0000-0000-0000-000000005051"},
  {'name': "Implemented", 'id': "00000000-0000-0000-0000-000000005055"},
  {'name': "In Progress", 'id': "00000000-0000-0000-0000-000000005019"},
  {'name': "Invalid", 'id': "00000000-0000-0000-0000-000000005022"},
  {'name': "Monitored", 'id': "00000000-0000-0000-0000-000000005054"},
  {'name': "New", 'id': "00000000-0000-0000-0000-000000005058"},
  {'name': "Obsolete", 'id': "00000000-0000-0000-0000-000000005011"},
  {'name': "Pending", 'id': "00000000-0000-0000-0000-000000005059"},
  {'name': "Rejected", 'id': "00000000-0000-0000-0000-000000005010"},
  {'name': "Resolution Pending", 'id': "00000000-0000-0000-0000-000000005056"},
  {'name': "Resolved", 'id': "00000000-0000-0000-0000-000000005057"},
  {'name': "Reviewed", 'id': "00000000-0000-0000-0000-000000005021"},
  {'name': "Submitted for Approval", 'id': "00000000-0000-0000-0000-000000005060"},
  {'name': "Under Review", 'id': "00000000-0000-0000-0000-000000005020"}
]