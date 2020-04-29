import json
import requests
import yaml
import thriftpy
import sys
import os
import logging
from okera import context
from resourceids import attribute_ids, asset_ids, relation_ids
import collections
import pprint

# Okera token passed as environment variable
TOKEN = os.getenv('TOKEN')
# location of config.yaml
CONF = os.getenv('CONF')
# Okera planner port
planner_port = 12050
# list of elements retrieved from Collibra
update_elements = []
# list of elements retrieved from Okera
elements = []
# Okera column type IDs
type_ids = ["BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING", "VARCHAR", "CHAR", "BINARY", "TIMESTAMP_NANOS", "DECIMAL", "DATE", "RECORD", "ARRAY", "MAP"]

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
    required = ['collibra_dgc', 'collibra_username', 'collibra_password', 'okera_host', 'log_directory']
    for c in configs:
        if c in required and configs[c] == "":
            logging.error("Empty value for key '{key}' in {config}!".format(key=c, config=config))
            error_out()
except yaml.YAMLError as e:
    logging.error("Error in '{config}': " + repr(e))
    error_out()
except FileNotFoundError:
    logging.error("Config file {config} not found! To set the location run $ export CONF=path/to/config.yaml".format(config=config))
    error_out()

# logging configuration
log_file = os.path.join(configs['log_directory'], 'export.log')
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename=log_file, level=logging.DEBUG)
logging.info("Logging to " + log_file)

update_assets = None
if configs['collibra_assets']:
    try:
        logging.info("Opening " + str(configs['collibra_assets']))
        with open(configs['collibra_assets']) as f:
            update_assets = yaml.load(f, Loader=yaml.FullLoader)
    except yaml.YAMLError as e:
        logging.error("Error in {assets_file}: {exception}".format(assets_file=configs['collibra_assets'], exception=repr(e)))

class Logger:
    def __init__(self, log_type, asset_name=None, asset_type=None, asset_id=None, location=None, attributes=None):
        self.log_type = log_type
        self.asset_name = asset_name
        self.asset_type = asset_type
        self.asset_id = asset_id
        self.location = location
        self.attributes = attributes
    def __str__(self):
        return "### " + str(self.log_type) + " ###\n" + str(pprint.pformat(self.__dict__))

# escapes special characters
def escape(string, remove_whitespace=False):
    if string:
        if remove_whitespace:
            return json.dumps(string.replace(" ", "_"))[1:-1]
        else: return json.dumps(string)[1:-1]

# builds and makes collibra request
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
            auth=(configs['collibra username'], configs['collibra password']))
            data.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.warning("COLLIBRA CORE API ERROR")
            logging.warning("Request body: " + str(param_obj))
            logging.warning("Error: " + repr(e))
            logging.warning("Response: " + str(data.content))
        logging.info("Request successful")
        logging.info("### END: Making Collibra request ###")
        return data.content

# PyOkera context
ctx = context()
try:
    if TOKEN:
        token = TOKEN
        logging.info("Using Okera token from environment")
    else:
        token = configs['okera_token']
        logging.info("Using Okera token from " + config)
    ctx.enable_token_auth(token_str=token)
except ValueError as e:
    logging.error("Okera token is not set! Set token in {config} or run $ export TOKEN=$TOKEN".format(config=config))
    error_out()

# returns resource ID in resourceids.yaml
def get_resource_ids(search_in, name):
    logging.info("\t### START: Get resource IDs from resourceids.py ###")
    logging.info("\t\tFetching resource ID for '" + name + "'")
    for r in search_in:
        if search_in == relation_ids:
            if r['head'] == name:
                logging.info("\t\tSuccessfully fetched resource ID {id} for '{name}'".format(id=r['id'], name=name))
                logging.info("\t### END: Get resource IDs from resourceids.py ###")
                return r['id']
        else:
            if r['name'] == name:
                logging.info("\t\tSuccessfully fetched resource ID {id} for '{name}'".format(id=r['id'], name=name))
                logging.info("\t### END: Get resource IDs from resourceids.py ###")
                return r['id']

# makes /attributes REST call and returns attribute
def get_attributes(asset_id, attr_type):
    logging.info("\t### START: Get attributes from Collibra ###")
    logging.debug("\t\tFetching attribute '{attr_type}' for asset '{id}' from Collibra".format(attr_type=attr_type, id=asset_id))
    type_id = get_resource_ids(attribute_ids, attr_type)
    params = {
        'typeIds': [type_id],
        'assetId': asset_id
        }
    data = json.loads(collibra_get(params, "attributes", 'get', None))
    if data.get('results'):
        value = data.get('results')[0].get('value')
        logging.debug("\t\tSuccessfully fetched '{attr_type}' for asset '{id}': {val}".format(attr_type=attr_type, id=asset_id, val=value))
        logging.info("\t### END: Get attributes from Collibra ###")
        return value
    else:
        logging.debug("\t\tNo attribute '{attr_type}' returned for asset '{id}'".format(attr_type=attr_type, id=asset_id))
        logging.info("\t### END: Get attributes from Collibra ###")

# creates tags as namespace.key and returns as list
def create_tags(attribute_values):
    attributes = []
    if attribute_values:
        logging.info("\t### START: Format tags ###")
        logging.info("\t\tFormatting tags for Okera as 'namespace.key'")
        for attribute in attribute_values:
            name = attribute.attribute.attribute_namespace + "." + attribute.attribute.key
            attributes.append(name)
        logging.debug("\t\tFormatted tags: " + str(attributes))
        logging.info("\t### END: Format tags ###")
        return attributes

def get_status(name, asset_type):
    status_param = {
        'name': name,
        'nameMatchMode': "EXACT",
        'domainId': domain_id,
        'communityId': community_id,
        'typeId': get_resource_ids(asset_ids, asset_type)
    }
    status = json.loads(collibra_get(status_param, 'assets', 'get'))['results']
    if status: return status[0]['status']['name']

# gets assets and their tags and attributes from Collibra
def collibra_calls(asset_name=None, asset_type=None):
    logging.info("############ START: Collibra calls ############")
    def set_elements(asset_id, asset_name, asset_type, status=None):
        logging.info("Fetching attributes for '" + str(asset_name) + "' from Collibra")
        if configs['full_name_prefixes'] != None and asset_name.split('.', 1)[0] in configs['full_name_prefixes']:
            asset_name = asset_name.split('.', 1)[1]
        update_element = ({
            'name': asset_name,
            'asset_id': asset_id,
            'description': escape(get_attributes(asset_id, "Description")),
            'type': asset_type,
            'mapped_okera_resource': json.loads(collibra_get(None, "mappings/externalSystem/okera/mappedResource/" + asset_id, 'get')).get('externalEntityId')
            })
        update_element['tags'] = []
        status_info = configs['mapped_collibra_statuses']
        if status and status_info['okera_namespace'] and status_info['okera_namespace'] != "" and status in status_info['statuses']: update_element['tags'].append(status_info['okera_namespace'] + "." + escape(status.lower(), True))
        if configs['mapped_collibra_attributes'] != None:
            for custom_attr in configs['mapped_collibra_attributes']:
                custom_attr_values = get_attributes(asset_id, custom_attr['attribute_name'])
                if custom_attr_values != None:
                    if isinstance(custom_attr_values, list):
                        for attr in custom_attr_values:
                            update_element['tags'].append(custom_attr['okera_namespace'] + "." + escape(attr, True))
                    else:
                        update_element['tags'].append(custom_attr['okera_namespace'] + "." + escape(custom_attr_values, True))
        update_elements.append(update_element)
    if asset_name:
        params = {
            'name': asset_name,
            'nameMatchMode': "EXACT",
            'domainId': domain_id,
            'communityId': community_id,
            'typeId': get_resource_ids(asset_ids, asset_type)
            }
    else:
        params = {
            'domainId': domain_id,
            'communityId': community_id
            }
    if asset_type == "Database":
        logging.info("###### COLLIBRA DATABASE '" + str(asset_name) + "' ######")
        logging.info("\tFetching Collibra database '" + str(asset_name) + "'")
        try:
            database = json.loads(collibra_get(params, "assets", 'get'))['results'][0]
            logging.info("\tSuccessfully fetched database '" + asset_name + "'")
        except IndexError:
            logging.error("\tEmpty Collibra response: Could not find Collibra database '" + str(asset_name) + "'!")
            logging.error("\tRequest body: " + str(params))
            error_out()
        set_elements(database['id'], database['name'], asset_type)
        table_params = {'relationTypeId': get_resource_ids(relation_ids, 'Table'), 'targetId': database['id']}
        logging.info("\tFetching tables for database '" + asset_name + "'")
        tables = json.loads(collibra_get(table_params, "relations", 'get'))['results']
        if tables == None:
            logging.warning("\tEmpty Collibra response: Could not find tables for Collibra database '" + str(asset_name) + "'")
            logging.warning("\tRequest body: " + str(table_params))
        else: logging.info("\tSuccessfully fetched tables for database '" + str(asset_name) + "'")
        columns = []
        for t in tables:
            logging.info("###### COLLIBRA TABLE '" + str(t['source']['name']) + "' ######")
            logging.debug("\tFetching table '" + str(t['source']['name']) + "'")
            set_elements(t['source']['id'], t['source']['name'], 'Table', get_status(t['source']['name'], 'Table'))
            column_params = {'relationTypeId': get_resource_ids(relation_ids, 'Column'), 'targetId': t['source']['id']}
            logging.info("\tFetching columns for table '" + str(t['source']['name']) + "'")
            for c in json.loads(collibra_get(column_params, 'relations', 'get'))['results']:
                logging.info("###### COLLIBRA COLUMN '" + str(c['source']['name']) + "' ######")
                logging.debug("\tFetching column '" + str(c['source']['name']) + "'")
                set_elements(c['source']['id'], c['source']['name'], 'Column')
            logging.debug("\tSuccessfully fetched columns for table '" + str(t['source']['name']) + "'")
    elif asset_type == "Table":
        logging.info("###### COLLIBRA TABLE '" + str(asset_name) + "' ######")
        logging.info("\tFetching Collibra table '" + str(asset_name) + "'")
        try:
            table = json.loads(collibra_get(params, "assets", "get"))['results'][0]
            logging.info("\tSuccessfully fetched table '" + str(asset_name) + "'")
        except IndexError:
            logging.error("\tEmpty Collibra response: Could not find Collibra table '" + str(asset_name) + "'!")
            logging.error("\tRequest body: " + str(params))
            error_out()
        set_elements(table['id'], table['name'], asset_type, get_status(table['name'], asset_type))
        column_params = {'relationTypeId': get_resource_ids(relation_ids, 'Column'), 'targetId': table['id']}
        logging.debug("Fetching columns for table '" + table['name'] + "'")
        for c in json.loads(collibra_get(column_params, 'relations', 'get'))['results']:
            logging.info("###### COLLIBRA COLUMN '" + str(c['source']['name']) + "' ######")
            logging.debug("\tFetching column '" + str(c['source']['name']) + "'")
            set_elements(c['source']['id'], c['source']['name'], 'Column')
        logging.debug("\tSuccessfully fetched columns for table '" + table['name'] + "'")
    logging.info("############ END: Collibra calls ############")

def pyokera_calls(asset_name=None, asset_type=None):
    logging.info("############ START: PyOkera calls ############")
    try:
        conn = ctx.connect(host = configs['okera_host'], port = planner_port)
    except thriftpy.transport.TException as e:
        logging.error("\tPYOKERA ERROR")
        logging.error("\tCould not connect to Okera!")
        logging.error("\tError: " + repr(e))
        error_out()
    with conn:
        databases = conn.list_databases()
        if asset_name:
            if asset_type == "Database":
                logging.info("\tFetching tables for database '" + asset_name + "'")
                tables = conn.list_datasets(asset_name)
                if tables:
                    element = {'database': asset_name, 'tables': tables}
                else:
                    element = {'database': asset_name}
                elements.append(element)
            elif asset_type == "Table":
                # look for asset ids here
                db_name = asset_name.split(".")[0]
                tables = conn.list_datasets(db_name)
                logging.info("\tFetching table '" + asset_name + "'")
                for t in tables:
                    asset_id = find_info(asset_name, 'asset_id')
                    name_match = t.db[0] + "." + t.name == asset_name
                    id_match = asset_id == t.metadata.get('collibra_asset_id')
                    if name_match or id_match: elements.append({'database': db_name, 'tables': t})
        else:
            for database in databases:
                logging.info("\tFetching tables for database '" + asset_name + "'")
                tables = conn.list_datasets(database)
                if tables:
                    element = {'database': database, 'tables': tables}
                else:
                    element = {'database': database}
                elements.append(element)
    logging.info("############ END: PyOkera calls ############")

# returns Collibra asset information based on the mapped Okera resource name
def find_info(name=None, info=None, asset_id=None):
    for ue in update_elements:
        if name and not asset_id:
            if ue['mapped_okera_resource'] and ue['mapped_okera_resource'] == name:
                return ue.get(info)
                break
            elif ue['name'] and ue['name'] == name:
                return ue.get(info)
                break
        elif asset_id:
            if ue['asset_id'] == asset_id:
                return ue.get(info)
                break

# makes assign_attribute() or unassign_attribute() call for either table or column
def tag_actions(action, db, name, type, tags):
    logging.info("### START: Okera tag operations ###")
    def define_tags(tag):
        try:
            conn = ctx.connect(host = configs['okera_host'], port = planner_port)
        except thriftpy.transport.TException as e:
            logging.error("\tPYOKERA ERROR")
            logging.error("\tCould not connect to Okera!")
            logging.error("\tError: " + repr(e))
            error_out()
        with conn:
            nmspc_key = tag.split(".")
            try:
                list_namespaces = conn.list_attribute_namespaces()
            except thriftpy.thrift.TException as e:
                logging.warning("\tPYOKERA ERROR")
                logging.warning("\tCould not list attribute namespaces!")
                logging.warning("\tError: " + repr(e))
            try:
                list_attributes = conn.list_attributes(nmspc_key[0])
            except thriftpy.thrift.TException as e:
                logging.warning("\tPYOKERA ERROR")
                logging.warning("\tCould not list attributes for namespace '" + nmspc_key[0] + "'!")
                logging.warning("\tError: " + repr(e))
            keys = []
            for l in list_attributes:
                keys.append(l.key)
            if not list_attributes or nmspc_key[0] not in list_namespaces or nmspc_key[1] not in keys:
                try:
                    logging.info("\tCreating new Okera tag and namespace '" + tag + "'")
                    conn.create_attribute(nmspc_key[0], nmspc_key[1], True)
                    logging.info("\tSuccessfully created new Okera tag and namespace '" + tag + "'")
                except thriftpy.thrift.TException as e:
                    logging.warning("\tPYOKERA ERROR")
                    logging.warning("\tCould not create tag '" + tag + "'!")
                    logging.warning("\tError: " + repr(e))
            if action == "assign":
                if type == "Column":
                    tab_col = name.split(".")
                    try:
                        logging.info("\tAssigning tag '" + tag + "' to column '" + name + "'")
                        conn.assign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=tab_col[1], column=tab_col[2], if_not_exists=True)
                        logging.info("\tSuccessfully assigned tag '" + tag + "' to column '" + name + "'")
                    except thriftpy.thrift.TException as e:
                        logging.warning("\tPYOKERA ERROR")
                        logging.warning("\tCould not assign tag '" + tag + "' to column '" + name + "'!")
                        logging.warning("\tError: " + repr(e))
                elif type == "Table":
                    try:
                        logging.info("\tAssigning tag '" + tag + "' to table '" + name + "'")
                        conn.assign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=name, if_not_exists=True)
                        logging.info("\tSuccessfully assigned tag '" + tag + "' to table '" + name + "'")
                    except thriftpy.thrift.TException as e:
                        logging.warning("\tPYOKERA ERROR")
                        logging.warning("\tCould not assign tag '" + tag + "' to table '" + name + "'!")
                        logging.warning("\tError: " + repr(e))
            elif action == "unassign":
                if type == "Column":
                    tab_col = name.split(".")
                    try:
                        logging.info("\tRemoving tag '" + tag + "' from column '" + name + "'")
                        conn.unassign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=tab_col[1], column=tab_col[2], if_not_exists=True)
                        logging.info("\tSuccessfully removed tag '" + tag + "' from column '" + name + "'")
                    except thriftpy.thrift.TException as e:
                        logging.warning("\tPYOKERA ERROR")
                        logging.warning("\tCould not remove tag '" + tag + "' from column '" + name + "'!")
                        logging.warning("\tError: " + repr(e))
                elif type == "Table":
                    try:
                        logging.info("\tRemoving tag '" + tag + "' from table '" + name + "'")
                        conn.unassign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=name, if_not_exists=True)
                        logging.info("\tSuccessfully removed tag '" + tag + "' from table '" + name + "'")
                    except thriftpy.thrift.TException as e:
                        logging.warning("\tPYOKERA ERROR")
                        logging.warning("\tCould not remove tag '" + tag + "' from table '" + name + "'!")
                        logging.warning("\tError: " + repr(e))
    if isinstance(tags, list):
        for tag in tags:
            define_tags(tag)
    else:
        define_tags(tags)
    logging.info("### END: Okera tag operations ###")

# alters description for either table, view or column
def desc_actions(name, type, col_type, description, tab_type=None):
    logging.info("### START: Okera description operations ###")
    description = '' if not description else description
    try:
        conn = ctx.connect(host = configs['okera_host'], port = planner_port)
    except thriftpy.transport.TException as e:
        logging.error("\tPYOKERA ERROR")
        logging.error("\tCould not connect to Okera!")
        logging.error("\tError: " + repr(e))
        error_out()
    with conn:
        if type == "Column" and tab_type == "Table":
            tab_col = name.rsplit('.', 1)
            try:
                logging.info("\tChanging description of column '" + name + "' to '" + description + "'")
                conn.execute_ddl("ALTER TABLE " + tab_col[0] + " CHANGE " + tab_col[1] + " " + tab_col[1] + " " + col_type + " COMMENT '" + description + "'")
                logging.info("\tSuccessfully changed description of column '" + name + "' to '" + description + "'")
            except thriftpy.thrift.TException as e:
                logging.warning("\tPYOKERA ERROR")
                logging.warning("\tCould not change description for column '" + name + "'!")
                logging.warning("\tError: " + repr(e))
        elif type == "Column" and tab_type == "View":
            tab_col = name.rsplit('.', 1)
            try:
                logging.info("\tChanging description of column '" + name + "' to '" + description + "'")
                conn.execute_ddl("ALTER TABLE " + tab_col[0] + " CHANGE COLUMN COMMENT " + tab_col[1] + " '" + description + "'")
                logging.info("\tSuccessfully changed description of column '" + name + "' to '" + description + "'")
            except thriftpy.thrift.TException as e:
                logging.warning("\tPYOKERA ERROR")
                logging.warning("\tCould not change description for column '" + name + "'!")
                logging.warning("\tError: " + repr(e))
        elif type == "Table":
            try:
                logging.info("\tChanging description of table '" + name + "' to '" + description + "'")
                conn.execute_ddl("ALTER TABLE " + name + " SET TBLPROPERTIES ('comment' = '" + description + "')")
                logging.info("\tSuccessfully changed description of table '" + name + "' to '" + description + "'")
            except thriftpy.thrift.TException as e:
                logging.warning("\tPYOKERA ERROR")
                logging.warning("\tCould not change description for table '" + name + "'!")
                logging.warning("\tError: " + repr(e))
        elif type == "View":
            try:
                logging.info("\tChanging description of view '" + name + "' to '" + description + "'")
                conn.execute_ddl("ALTER TABLE " + name + " SET TBLPROPERTIES ('comment' = '" + description + "')")
                logging.info("\tSuccessfully changed description of view '" + name + "' to '" + description + "'")
            except thriftpy.thrift.TException as e:
                logging.warning("\tPYOKERA ERROR")
                logging.warning("\tCould not change description for view '" + name + "'!")
                logging.warning("\tError: " + repr(e))
    logging.info("### END: Okera description operations ###")

loggers = []
# diffs attributes from Collibra with attributes from Okera and makes necessary changes in Okera
def log_summary():
    logging.info("############ SUMMARY: ASSETS FETCHED AND COMPARED ############")
    for logger in loggers: logging.debug(logger)

def export(asset_name=None, asset_type=None):
    collibra_calls(asset_name, asset_type)
    if configs['full_name_prefixes'] != None and asset_name.split('.', 1)[0] in configs['full_name_prefixes']:
        asset_name = asset_name.split('.', 1)[1]
    pyokera_calls(asset_name, asset_type)
    def diff(t, okera_tab_logger=None, collibra_tab_logger=None):
        desc_id = get_resource_ids(attribute_ids, 'Description')
        custom_attr_ids = []
        if configs['mapped_collibra_attributes'] != None:
            for custom_attr in configs['mapped_collibra_attributes']:
                custom_attr_ids.append({'name': custom_attr['attribute_name'], 'id': get_resource_ids(attribute_ids, custom_attr['attribute_name'])})
        asset_id = t.metadata.get('collibra_asset_id')
        tab_name = t.db[0] + "." + t.name
        collibra_tab_tags = find_info(info='tags', asset_id=asset_id) if asset_id else find_info(name=tab_name, info='tags')
        okera_tab_tags = create_tags(t.attribute_values)
        logging.debug("###### Comparing Collibra attributes of table (name: '{tab_name}', asset ID: '{id}') to Okera tags of table '{tab_name}': ######\nCollibra attributes: {collibra_tags}\nOkera tags: {okera_tags}".format(tab_name=tab_name, id=asset_id, collibra_tags=collibra_tab_tags, okera_tags=okera_tab_tags))
        if okera_tab_tags and collibra_tab_tags:
            if collections.Counter(okera_tab_tags) != collections.Counter(collibra_tab_tags):
                logging.debug("\t Differences found, starting Okera tag operations")
                tag_actions("unassign", t.db[0], t.name, "Table", okera_tab_tags)
                tag_actions("assign", t.db[0], t.name, "Table", collibra_tab_tags)
            else: logging.debug("\tNo differences found")
        elif collibra_tab_tags and not okera_tab_tags:
            logging.debug("\t Differences found, starting Okera tag operations")
            tag_actions("assign", t.db[0], t.name, "Table", collibra_tab_tags)
        elif okera_tab_tags and not collibra_tab_tags:
            logging.debug("\t Differences found, starting Okera tag operations")
            tag_actions("unassign", t.db[0], t.name, "Table", okera_tab_tags)
        else: logging.debug("\tNo differences found")
        collibra_tab_desc = find_info(info="description", asset_id=asset_id) if asset_id else find_info(name=tab_name, info="description")
        okera_tab_desc = t.description
        tab_type = "View" if t.primary_storage == "View" else "Table"
        logging.debug("###### Comparing Collibra description of table (name: '{tab_name}', asset ID: '{id}') to Okera description of table '{tab_name}': ######\nCollibra description: {collibra_desc}\nOkera description: {okera_desc}".format(tab_name=tab_name, id=asset_id, collibra_desc=collibra_tab_desc, okera_desc=okera_tab_desc))
        if okera_tab_desc and not collibra_tab_desc or collibra_tab_desc and not okera_tab_desc or (okera_tab_desc and collibra_tab_desc and okera_tab_desc != collibra_tab_desc):
            logging.debug("\tDifferences found, starting Okera description operations")
            desc_actions(tab_name, tab_type, None, collibra_tab_desc)
        else:
            logging.debug("\tNo differences found")
        # begin of column loop: same functionality as table loop
        find_name_by_id = find_info(info="name", asset_id=asset_id)
        collibra_tab_name = str(find_info(info="name", asset_id=asset_id)) if asset_id else tab_name
        collibra_tab_logger = Logger(
            "COLLIBRA TABLE '" + collibra_tab_name + "'",
            find_info(info="name", asset_id=asset_id) if asset_id else tab_name,
            {'name': "Table", 'type_id': get_resource_ids(asset_ids, "Table")},
            str(asset_id) if asset_id else find_info(name=tab_name, info="asset_id"),
            {'system': "Collibra", 'community': community, 'domain': domain, 'database': str(t.db[0])},
            {'description': {'value': str(collibra_tab_desc), 'resource_id': str(desc_id)}, 'tags': {'value': collibra_tab_tags, 'resource_ids': custom_attr_ids}}
        )
        loggers.append(collibra_tab_logger)
        for col in t.schema.cols:
            col_name = tab_name + "." + col.name
            okera_col_tags = create_tags(col.attribute_values)
            collibra_col_name = find_info(asset_id=asset_id, info="name") + "." + col.name if find_info(asset_id=asset_id, info="name") else col_name
            collibra_col_tags = find_info(collibra_col_name, "tags")
            logging.debug("###### Comparing Collibra attributes of column '{collibra_col_name}' to Okera tags of column '{col_name}': ######\nCollibra attributes: {collibra_tags}\nOkera tags: {okera_tags}".format(collibra_col_name=collibra_col_name, col_name=col_name, collibra_tags=collibra_col_tags, okera_tags=okera_col_tags))
            if okera_col_tags and collibra_col_tags:
                if collections.Counter(okera_col_tags) != collections.Counter(collibra_col_tags):
                    logging.debug("\t Differences found, starting Okera tag operations")
                    tag_actions("unassign", t.db[0], col_name, "Column", okera_col_tags)
                    tag_actions("assign", t.db[0], col_name, "Column", collibra_col_tags)
                else: logging.debug("\tNo differences found")
            elif collibra_col_tags and not okera_col_tags:
                logging.debug("\t Differences found, starting Okera tag operations")
                tag_actions("assign", t.db[0], col_name, "Column", collibra_col_tags)
            elif okera_col_tags and not collibra_col_tags:
                logging.debug("\t Differences found, starting Okera tag operations")
                tag_actions("unassign", t.db[0], col_name, "Column", okera_col_tags)
            else: logging.debug("\tNo differences found")
            collibra_col_desc = find_info(collibra_col_name, "description")
            okera_col_desc = col.comment
            logging.debug("###### Comparing Collibra description of column '{collibra_col_name}' to Okera description of column '{col_name}': ######\nCollibra description: {collibra_desc}\nOkera description: {okera_desc}".format(collibra_col_name=collibra_col_name, col_name=col_name, collibra_desc=collibra_col_desc, okera_desc=okera_col_desc))
            if okera_col_desc and not collibra_col_desc or collibra_col_desc and not okera_col_desc or (okera_col_desc and collibra_col_desc and okera_col_desc != collibra_col_desc):
                logging.debug("\tDifferences found, starting Okera description operations")
                desc_actions(col_name, "Column", type_ids[col.type.type_id], collibra_col_desc, tab_type)
            else: logging.debug("\tNo differences found")
            okera_col_logger = Logger(
                "OKERA COLUMN '" + col_name + "'",
                col_name,
                "Column",
                "None",
                {'system': "Okera", 'table': tab_name},
                {'description': str(okera_col_desc), 'tags': str(okera_col_tags)}
            )
            collibra_tab_name = collibra_col_name.rsplit('.',1)[0]
            collibra_col_logger = Logger(
                "COLLIBRA COLUMN '" + collibra_col_name + "'",
                collibra_col_name,
                {'name': "Column", 'type_id': get_resource_ids(asset_ids, "Column")},
                find_info(collibra_col_name, 'asset_id'),
                {'system': "Collibra", 'community': community, 'domain': domain, 'table': {'name': str(collibra_tab_name), 'asset_id': str(asset_id) if asset_id else str(find_info(str(collibra_tab_name), "asset_id"))}},
                {'description': {'value': str(collibra_col_desc), 'resource_id': str(desc_id)}, 'tags': {'value': collibra_col_tags, 'resource_ids': custom_attr_ids}}
            )
            loggers.append(okera_col_logger)
            loggers.append(collibra_col_logger)

    logging.info("############ START: Compare Collibra assets to Okera assets ############")
    for element in elements:
        if asset_type == "Database" and element.get('database') == asset_name:
            for table in element.get('tables'):
                okera_tab_logger = Logger(
                    "OKERA TABLE '" + table.db[0] + "." + table.name + "'",
                    table.db[0] + "." + table.name,
                    "Table",
                    str(table.metadata.get('collibra_asset_id')),
                    {'system': "Okera", 'database': table.db[0]},
                    {'description': str(table.description), 'tags': create_tags(table.attribute_values)}
                )
                loggers.append(okera_tab_logger)
                diff(table, okera_tab_logger)
        elif asset_type == "Table":
            table = element.get('tables')
            okera_tab_logger = Logger(
                "OKERA TABLE '" + table.db[0] + "." + table.name + "'",
                table.db[0] + "." + table.name,
                "Table",
                str(table.metadata.get('collibra_asset_id')),
                {'system': "Okera", 'database': table.db[0]},
                {'description': str(table.description), 'tags': create_tags(table.attribute_values)}
            )
            loggers.append(okera_tab_logger)
            diff(table, okera_tab_logger)
    logging.info("############ END: Compare Collibra assets to Okera assets ############")

if update_assets:
    logging.info("Using assets in '" + configs['collibra_assets'] + "' to run script...")
    for comm in update_assets['communities']:
        community = comm['name']
        community_id = comm['id']
        for dom in comm['domains']:
            domain = dom['name']
            domain_id = dom['id']
            if dom['databases']:
                for db in dom['databases']:
                    update_elements = []
                    elements = []
                    export(db, 'Database')
            if dom['tables']:
                for table in dom['tables']:
                    update_elements = []
                    elements = []
                    export(table, 'Table')
    log_summary()
    logging.info("Export complete!")
    print("Export complete!")
else:
    # names and IDs of Collibra community and domain
    community = configs['community']
    try:
        community_id = json.loads(collibra_get({'name': community}, "communities", 'get')).get('results')[0].get('id')
    except IndexError:
        logging.error("Empty Collibra response: Could not find Collibra community " + community + "!")
        error_out()
    domain = configs['domain']
    try:
        domain_id = json.loads(collibra_get({'name': domain['name'], 'communityId': community_id}, "domains", 'get')).get('results')[0].get('id')
    except IndexError:
        logging.error("Empty Collibra response: Could not find Collibra domain " + domain['name'] + " in community " + community + "!")
        error_out()

    which_type = input("Would you like to update a database or a table? ")
    valid = ["table", "database"]
    while which_type not in valid:
        which_type = input("Please enter a valid asset type: ")
    which_asset = input("Please enter the full name of the " + which_type + ": ")
    if which_asset and which_type:
        export(which_asset, which_type.capitalize())
        log_summary()
        logging.info("Export complete!")
        print("Export complete!")
    else:
        while not which_asset:
            which_asset = input("The full name of the asset is required: ")
        export(which_asset, which_type)
        log_summary()
        logging.info("Export complete!")
        print("Export complete!")