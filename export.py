import json
import requests
import yaml
import thriftpy
import sys
import logging
from okera import context
import collections
from pprint import pformat
#pp = pprint.PrettyPrinter(indent=4)

logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='export.log',level=logging.DEBUG)
# Okera planner port
planner_port = 12050
# list of elements retrieved from Collibra
update_elements = []
# list of elements retrieved from Okera
elements = []
# Okera column type IDs
type_ids = ["BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING", "VARCHAR", "CHAR", "BINARY", "TIMESTAMP_NANOS", "DECIMAL", "DATE", "RECORD", "ARRAY", "MAP"]

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

# opens resourceids.yaml
try:
    logging.info("Opening resourceids.yaml")
    with open('resourceids.yaml') as f:
        resource_ids = yaml.load(f, Loader=yaml.FullLoader)
except yaml.YAMLError as e:
    logging.error("Error in resourceids.yaml: " + str(e))
for r in resource_ids:
    for line in resource_ids[r]:
        for key in line:
            if line[key] == "":
                logging.error("Empty value in line " + str(line) + " in resourceids.yaml!")
                logging.info("Could not start export, terminating script")
                print("Export failed! For more information check export.log.")
                sys.exit(1)

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
            logging.info("Making Collibra request")
            data = getattr(requests, method)(
            configs['collibra_dgc'] + "/rest/2.0/" + call, 
            params=param_obj, 
            auth=(configs['collibra_username'], configs['collibra_password']))
            data.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if json.loads(data.content).get('errorCode') != "mappingNotFound":
                logging.warning("COLLIBRA CORE API ERROR") 
                logging.warning("Request body: " + str(param_obj))
                logging.warning("Error: " + str(e))
                logging.warning("Response: " + str(data.content))
        logging.info("Request successful")
        return data.content
    else:
        try: 
            logging.info("Making Collibra request")
            data = getattr(requests, method)(
            configs['collibra_dgc'] + "/rest/2.0/" + call,
            headers=header, 
            json=param_obj, 
            auth=(configs['collibra username'], configs['collibra password']))    
            data.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.warning("COLLIBRA CORE API ERROR")
            logging.warning("Request body: " + str(param_obj))
            logging.warning("Error: " + str(e))
            logging.warning("Response: " + str(data.content))
        logging.info("Request successful")
        return data.content

class Logger:
    def __init__(self, log_type, asset_name=None, asset_type=None, asset_id=None, location=None, attributes=None):
        self.log_type = log_type
        self.asset_name = asset_name
        self.asset_type = asset_type
        self.asset_id = asset_id
        self.location = location
        self.attributes = attributes
    def __str__(self):
        return "### " + str(self.log_type) + " ###\n" + str(pformat(self.__dict__)) + "\n----------"

# names and IDs of Collibra community and domain
community = configs['community']
try:
    community_id = json.loads(collibra_get({'name': community}, "communities", 'get')).get('results')[0].get('id')
except IndexError:
    logging.error("Empty Collibra response: Could not find Collibra community " + community + "!")
    logging.info("Could not start export, terminating script")
    print("Export failed! For more information check export.log.")
    sys.exit(1)
domain = configs['domain']
try:
    domain_id = json.loads(collibra_get({'name': domain['name'], 'communityId': community_id}, "domains", 'get')).get('results')[0].get('id')
except IndexError:
    logging.error("Empty Collibra response: Could not find Collibra domain " + domain['name'] + " in community " + community + "!")
    logging.info("Could not start export, terminating script")
    print("Export failed! For more information check export.log.")
    sys.exit(1)

# PyOkera context
ctx = context()
ctx.enable_token_auth(token_str=configs['token'])

# returns resource ID in resourceids.yaml
def get_resource_ids(search_in, name):
    logging.info("### Get resource IDs from resourceids.yaml ###")
    logging.info("Fetching resource ID for '" + name + "' in '" + search_in + "'")
    for r in resource_ids[search_in]:
        if search_in == 'relations':
            if r['head'] == name:
                logging.info("Successfully fetched resource ID " + r['id'] + " for '" + name + "'")
                logging.info("###")
                return r['id']
        else:
            if r['name'] == name:
                logging.info("Successfully fetched " + r['id'] + " for '" + name + "'")
                logging.info("###")
                return r['id']

# makes /attributes REST call and returns attribute
def get_attributes(asset_id, attr_type):
    logging.info("### Get attributes from Collibra ###")
    logging.info("Fetching attribute '" + attr_type + "' for asset '" + str(asset_id) + "' from Collibra")
    type_id = get_resource_ids('attributes', attr_type)
    params = {
        'typeIds': [type_id],
        'assetId': asset_id
        }
    data = json.loads(collibra_get(params, "attributes", 'get', None))
    if data.get('results'):
        value = data.get('results')[0].get('value')
        logging.info("Successfully fetched '" + attr_type + "' for asset '" + str(asset_id) + "': '" + str(value) + "'")
        logging.info("###")
        return value
    else: 
        logging.debug("No attribute '" + attr_type + "' returned for asset '" + str(asset_id) + "'")
        logging.info("###")

# creates tags as namespace.key and returns as list
def create_tags(attribute_values):
    attributes = []
    if attribute_values:
        logging.info("### Format tags ###")
        logging.info("Formatting tags for Okera as 'namespace.key'")
        for attribute in attribute_values:
            name = attribute.attribute.attribute_namespace + "." + attribute.attribute.key 
            attributes.append(name)
        logging.info("Formatted tags: " + str(attributes))
        logging.info("###")
        return attributes

# pyokera calls
def pyokera_calls(asset_name=None, asset_type=None):
    logging.info("###### PyOkera calls ######")
    try:
        conn = ctx.connect(host = configs['host'], port = planner_port)
    except thriftpy.transport.TException as e:
        logging.error("PYOKERA ERROR")
        logging.error("Could not connect to Okera!")
        logging.error("Error: " + str(e))
        logging.info("Could not start export, terminating script")
        print("Export failed! For more information check export.log.")
        sys.exit(1)
    with conn:
        databases = conn.list_databases()
        if asset_name:
            if asset_type == "Database":
                logging.info("Fetching tables for database " + asset_name)
                tables = conn.list_datasets(asset_name)
                if tables:
                    element = {'database': asset_name, 'tables': tables}
                else:
                    element = {'database': asset_name}
                elements.append(element)
            elif asset_type == "Table":
                db_name = asset_name.split(".")[0]
                tables = conn.list_datasets(db_name)
                logging.info("Fetching table " + asset_name)
                for t in tables:
                    if t.db[0] + "." + t.name == asset_name:
                        elements.append({'database': db_name, 'tables': t})
                        break
        else:
            for database in databases:
                logging.info("Fetching tables for database " + asset_name)
                tables = conn.list_datasets(database)
                if tables:
                    element = {'database': database, 'tables': tables}
                else:
                    element = {'database': database}
                elements.append(element)
    logging.info("###### PyOkera calls completed ######") 

# gets assets and their tags and attributes from Collibra
def collibra_calls(asset_name=None, asset_type=None):
    logging.info("###### Collibra calls ######")
    def set_elements(asset_id, asset_name, asset_type):
        info_classif = escape(get_attributes(asset_id, "Information Classification"), True)
        logging.info("Fetching attributes for '" + str(asset_name) + "' from Collibra")
        update_elements.append({
            'name': asset_name, 
            'asset_id': asset_id, 
            'description': escape(get_attributes(asset_id, "Description")),
            'info_classif': configs['mapped_attribute_okera_namespace'] + "." + info_classif if info_classif else None, 
            'type': asset_type, 
            'mapped_okera_resource': json.loads(collibra_get(None, "mappings/externalSystem/okera/mappedResource/" + asset_id, 'get')).get('externalEntityId')
            })

    if asset_name:
        params = {
            'name': asset_name,
            'nameMatchMode': "EXACT",
            'simulation': False,
            'domainId': domain_id,
            'communityId': community_id,
            'typeId': get_resource_ids('assets', asset_type)
            }
    else:
        params = {
            'simulation': False,
            'domainId': domain_id,
            'communityId': community_id
            }
    logging.info("Fetching Collibra asset '" + str(asset_name) + "'")
    if asset_type == "Database":
        try:
            database = json.loads(collibra_get(params, "assets", 'get'))['results'][0]
            logging.info("Successfully fetched database '" + asset_name + "'")
        except IndexError:
            logging.error("Empty Collibra response: Could not find Collibra database '" + str(asset_name) + "'!")
            logging.error("Request body: " + str(params))
            logging.info("Could not start export, terminating script")
            print("Export failed! For more information check export.log.")
            sys.exit(1)
        set_elements(database['id'], database['name'], asset_type)
        table_params = {'relationTypeId': get_resource_ids('relations', 'Table'), 'targetId': database['id']}
        logging.info("Fetching tables for database '" + asset_name + "'")
        tables = json.loads(collibra_get(table_params, "relations", 'get'))['results']
        if tables == None:
            logging.warning("Empty Collibra response: Could not find tables for Collibra database '" + str(asset_name) + "'")
            logging.warning("Request body: " + str(table_params))
        else: logging.info("Successfully fetched tables for database '" + str(asset_name) + "'")
        columns = []
        for t in tables:
            logging.debug("Fetching table '" + str(t['source']['name']) + "'")
            set_elements(t['source']['id'], t['source']['name'], 'Table')
            column_params = {'relationTypeId': get_resource_ids('relations', 'Column'), 'targetId': t['source']['id']}
            logging.info("Fetching columns for table '" + str(t['source']['name']) + "'")
            for c in json.loads(collibra_get(column_params, 'relations', 'get'))['results']:
                logging.debug("Fetching column '" + str(c['source']['name']) + "'")
                set_elements(c['source']['id'], c['source']['name'], 'Column')
            logging.debug("Successfully fetched columns for table '" + str(t['source']['name']) + "'")
    elif asset_type == "Table":
        try:
            table = json.loads(collibra_get(params, "assets", "get"))['results'][0]
            logging.info("Successfully fetched table '" + str(asset_name) + "'")
        except IndexError:
            logging.error("Empty Collibra response: Could not find Collibra table '" + str(asset_name) + "'!")
            logging.error("Request body: " + str(params))
            logging.info("Could not start export, terminating script")
            print("Export failed! For more information check export.log.")
            sys.exit(1)
        set_elements(table['id'], table['name'], asset_type)
        column_params = {'relationTypeId': get_resource_ids('relations', 'Column'), 'targetId': table['id']}
        logging.debug("Fetching columns for table '" + table['name'] + "'")
        for c in json.loads(collibra_get(column_params, 'relations', 'get'))['results']:
            logging.debug("Fetching column '" + c['source']['name'] + "'")
            set_elements(c['source']['id'], c['source']['name'], 'Column')
        logging.debug("Successfully fetched columns for table '" + table['name'] + "'")
    logging.info("###### Collibra calls completed ######")
        
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
    logging.info("### Okera tag operations ###")
    def define_tags(tag):
        try:
            conn = ctx.connect(host = configs['host'], port = planner_port)
        except thriftpy.transport.TException as e:
            logging.error("PYOKERA ERROR")
            logging.error("Could not connect to Okera!")
            logging.error("Error: " + str(e))
        with conn:
            nmspc_key = tag.split(".")
            try:
                list_namespaces = conn.list_attribute_namespaces()
            except thriftpy.thrift.TException as e:
                logging.warning("PYOKERA ERROR")
                logging.warning("Could not list attribute namespaces!")
                logging.warning("Error: " + str(e))
            try:
                list_attributes = conn.list_attributes(nmspc_key[0])
            except thriftpy.thrift.TException as e:
                logging.warning("PYOKERA ERROR")
                logging.warning("Could not list attributes for namespace '" + nmspc_key[0] + "'!")
                logging.warning("Error: " + str(e))
            keys = []
            for l in list_attributes:
                keys.append(l.key)
            if not list_attributes or nmspc_key[0] not in list_namespaces or nmspc_key[1] not in keys:
                try:
                    logging.info("Creating new Okera tag and namespace '" + tag + "'")
                    conn.create_attribute(nmspc_key[0], nmspc_key[1], True)
                    logging.info("Successfully created new Okera tag and namespace '" + tag + "'")
                except thriftpy.thrift.TException as e:
                    logging.warning("PYOKERA ERROR")
                    logging.warning("Could not create tag '" + tag + "'!")
                    logging.warning("Error: " + str(e))
            if action == "assign":
                if type == "Column":
                    tab_col = name.split(".")
                    try:
                        logging.info("Assigning tag '" + tag + "' to column '" + name + "'")
                        conn.assign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=tab_col[1], column=tab_col[2], if_not_exists=True)
                        logging.info("Successfully assigned tag '" + tag + "' to column '" + name + "'")
                    except thriftpy.thrift.TException as e:
                        logging.warning("PYOKERA ERROR")
                        logging.warning("Could not assign tag '" + tag + "' to column '" + name + "'!")
                        logging.warning("Error: " + str(e))
                elif type == "Table":
                    try:
                        logging.info("Assigning tag '" + tag + "' to table '" + name + "'")
                        conn.assign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=name, if_not_exists=True)
                        logging.info("Successfully assigned tag '" + tag + "' to table '" + name + "'")
                    except thriftpy.thrift.TException as e:
                        logging.warning("PYOKERA ERROR")
                        logging.warning("Could not assign tag '" + tag + "' to table '" + name + "'!")
                        logging.warning("Error: " + str(e))
            elif action == "unassign":
                if type == "Column":
                    tab_col = name.split(".")
                    try:
                        logging.info("Removing tag '" + tag + "' from column '" + name + "'")
                        conn.unassign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=tab_col[1], column=tab_col[2], if_not_exists=True)
                        logging.info("Successfully removed tag '" + tag + "' from column '" + name + "'")
                    except thriftpy.thrift.TException as e:
                        logging.warning("PYOKERA ERROR")
                        logging.warning("Could not remove tag '" + tag + "' from column '" + name + "'!")
                        logging.warning("Error: " + str(e))
                elif type == "Table":
                    try:
                        logging.info("Removing tag '" + tag + "' from table '" + name + "'")
                        conn.unassign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=name, if_not_exists=True)
                        logging.info("Successfully removed tag '" + tag + "' from table '" + name + "'")
                    except thriftpy.thrift.TException as e:
                        logging.warning("PYOKERA ERROR")
                        logging.warning("Could not remove tag '" + tag + "' from table '" + name + "'!")
                        logging.warning("Error: " + str(e))
    if isinstance(tags, list):
        for tag in tags:
            define_tags(tag)
    else: 
        define_tags(tags)
    logging.info("### Okera tag operations completed ###")
        
# alters description for either table, view or column
def desc_actions(name, type, col_type, description, tab_type=None):
    logging.info("### Okera description operations ###")
    description = '' if not description else description
    try:
        conn = ctx.connect(host = configs['host'], port = planner_port)
    except thriftpy.transport.TException as e:
        logging.error("PYOKERA ERROR")
        logging.error("Could not connect to Okera!")
        logging.error("Error: " + str(e))
        logging.info("Could not start export, terminating script")
        print("Export failed! For more information check export.log.")
        sys.exit(1)
    with conn:
        if type == "Column" and tab_type == "Table":
            tab_col = name.rsplit('.', 1)
            try:
                logging.info("Changing description of column '" + name + "' to '" + description + "'")
                conn.execute_ddl("ALTER TABLE " + tab_col[0] + " CHANGE " + tab_col[1] + " " + tab_col[1] + " " + col_type + " COMMENT '" + description + "'")
                logging.info("Successfully changed description of column '" + name + "' to '" + description + "'")
            except thriftpy.thrift.TException as e:
                logging.warning("PYOKERA ERROR")
                logging.warning("Could not change description for column '" + name + "'!")
                logging.warning("Error: " + str(e))
        elif type == "Column" and tab_type == "View":
            logging.warning("PYOKERA ERROR")
            logging.warning("Could not change description for column in view '" + name + "'!")
        elif type == "Table":
            try:
                logging.info("Changing description of table '" + name + "' to '" + description + "'")
                conn.execute_ddl("ALTER TABLE " + name + " SET TBLPROPERTIES ('comment' = '" + description + "')")
                logging.info("Successfully changed description of table '" + name + "' to '" + description + "'")
            except thriftpy.thrift.TException as e:
                logging.warning("PYOKERA ERROR")
                logging.warning("Could not change description for table '" + name + "'!")
                logging.warning("Error: " + str(e))
        elif type == "View":
            try:
                logging.info("Changing description of view '" + name + "' to '" + description + "'")
                conn.execute_ddl("ALTER TABLE " + name + " SET TBLPROPERTIES ('comment' = '" + description + "')")
                logging.info("Successfully changed description of view '" + name + "' to '" + description + "'")
            except thriftpy.thrift.TException as e:
                logging.warning("PYOKERA ERROR")
                logging.warning("Could not change description for view '" + name + "'!")
                logging.warning("Error: " + str(e))
    logging.info("### Okera description operations completed ###")

# diffs attributes from Collibra with attributes from Okera and makes necessary changes in Okera
loggers = []
def log_summary():
    logging.info("############ ASSETS FETCHED AND COMPARED ############")
    for logger in loggers: logging.debug(logger)

def export(asset_name=None, asset_type=None):
    collibra_calls(asset_name, asset_type)
    pyokera_calls(asset_name, asset_type)
    def diff(t, okera_tab_logger=None, collibra_tab_logger=None):
        desc_id = get_resource_ids('attributes', 'Description')
        info_classif_id = get_resource_ids('attributes', 'Information Classification')
        asset_id = t.metadata.get('collibra_asset_id')
        tab_name = t.db[0] + "." + t.name
        collibra_tab_tags = find_info(info="info_classif", asset_id=asset_id) if asset_id else find_info(name=tab_name, info="info_classif")
        okera_tab_tags = create_tags(t.attribute_values)
        logging.debug("Comparing Collibra attributes of table (name: '" + tab_name + "', asset ID: '" + str(asset_id) + "') to Okera tags of table '" + tab_name + "'")
        logging.debug("Collibra attributes: " + str(collibra_tab_tags))
        logging.debug("Okera tags: " + str(okera_tab_tags))
        if okera_tab_tags and collibra_tab_tags:
            if collections.Counter(okera_tab_tags) != collections.Counter([collibra_tab_tags]):
                tag_actions("unassign", t.db[0], t.name, "Table", okera_tab_tags)
                tag_actions("assign", t.db[0], t.name, "Table", collibra_tab_tags)
        elif collibra_tab_tags and not okera_tab_tags:
            tag_actions("assign", t.db[0], t.name, "Table", collibra_tab_tags)
        elif okera_tab_tags and not collibra_tab_tags:
            tag_actions("unassign", t.db[0], t.name, "Table", okera_tab_tags)
        collibra_tab_desc = find_info(info="description", asset_id=asset_id) if asset_id else find_info(name=tab_name, info="description")
        okera_tab_desc = t.description
        tab_type = "View" if t.primary_storage == "View" else "Table"
        logging.debug("Comparing Collibra description of table (name: '" + tab_name + "', asset ID: '" + str(asset_id) + "') to Okera description of table '" + tab_name + "'")
        logging.debug("Collibra description: " + str(collibra_tab_desc))
        logging.debug("Okera description: " + str(okera_tab_desc))
        if okera_tab_desc and not collibra_tab_desc or collibra_tab_desc and not okera_tab_desc or (okera_tab_desc and collibra_tab_desc and okera_tab_desc != collibra_tab_desc):
            desc_actions(tab_name, tab_type, None, collibra_tab_desc)        
        # begin of column loop: same functionality as table loop
        find_name_by_id = find_info(info="name", asset_id=asset_id)
        collibra_tab_name = str(find_info(info="name", asset_id=asset_id)) if asset_id else tab_name
        collibra_tab_logger = Logger(
            "Fetched Collibra information for table '" + collibra_tab_name + "'",
            find_info(info="name", asset_id=asset_id) if asset_id else tab_name,
            {'name': "Table", 'type_id': get_resource_ids('assets', "Table")},
            str(asset_id) if asset_id else find_info(name=tab_name, info="asset_id"),
            {'system': "Collibra", 'community': community, 'domain': domain, 'database': str(t.db[0])},
            {'description': {'value': str(collibra_tab_desc), 'resource_id': str(desc_id)}, 'info_classif': {'value': str(collibra_tab_tags), 'resource_id': str(info_classif_id)}}
        )
        loggers.append(collibra_tab_logger)
        for col in t.schema.cols:
            col_name = tab_name + "." + col.name
            okera_col_tags = create_tags(col.attribute_values)
            collibra_col_name = find_info(asset_id=asset_id, info="name") + "." + col.name if find_info(asset_id=asset_id, info="name") else col_name
            collibra_col_tags = find_info(collibra_col_name, "info_classif")
            logging.debug("Comparing Collibra attributes of column '" + collibra_col_name + "' to Okera tags of column '" + col_name + "':\nCollibra attributes: '" + str(collibra_col_tags) + "'\nOkera tags: " + str(okera_col_tags))
            #logging.debug("Collibra attributes: '" + str(collibra_col_tags) + "'")
            #logging.debug("Okera tags: " + str(okera_col_tags))
            if okera_col_tags and collibra_col_tags:
                if collections.Counter(okera_col_tags) != collections.Counter([collibra_col_tags]):
                    tag_actions("unassign", t.db[0], col_name, "Column", okera_col_tags)
                    tag_actions("assign", t.db[0], col_name, "Column", collibra_col_tags)
            elif collibra_col_tags and not okera_col_tags:
                tag_actions("assign", t.db[0], col_name, "Column", collibra_col_tags)
            elif okera_col_tags and not collibra_col_tags:
                tag_actions("unassign", t.db[0], col_name, "Column", okera_col_tags)
            collibra_col_desc = find_info(collibra_col_name, "description")
            okera_col_desc = col.comment
            logging.debug("Comparing Collibra description of column '" + collibra_col_name + "' to Okera description of column '" + col_name + "'")
            logging.debug("Collibra description: " + str(collibra_col_desc))
            logging.debug("Okera description: " + str(okera_col_desc))
            if okera_col_desc and not collibra_col_desc or collibra_col_desc and not okera_col_desc or (okera_col_desc and collibra_col_desc and okera_col_desc != collibra_col_desc):
                desc_actions(col_name, "Column", type_ids[col.type.type_id], collibra_col_desc, tab_type)
            okera_col_logger = Logger(
                "Fetched Okera information for column '" + col_name + "'",
                col_name, 
                "Column", 
                "None", 
                {'system': "Okera", 'table': tab_name},
                {'description': str(okera_col_desc), 'tags': str(okera_col_tags)}
            )
            collibra_tab_name = collibra_col_name.rsplit('.',1)[0]
            collibra_col_logger = Logger(
                "Fetched Collibra information for column '" + collibra_col_name + "'",
                collibra_col_name,
                {'name': "Column", 'type_id': get_resource_ids('assets', "Column")},
                find_info(collibra_col_name, 'asset_id'),
                {'system': "Collibra", 'community': community, 'domain': domain, 'table': {'name': str(collibra_tab_name), 'asset_id': str(asset_id) if asset_id else str(find_info(str(collibra_tab_name), "asset_id"))}},
                {'description': {'value': str(okera_col_desc), 'resource_id': str(desc_id)}, 'info_classif': {'value': str(okera_col_tags), 'resource_id': str(info_classif_id)}}
            )
            loggers.append(okera_col_logger)
            loggers.append(collibra_col_logger)
    
    for element in elements:
        if asset_type == "Database" and element.get('database') == asset_name:
            for table in element.get('tables'):
                okera_tab_logger = Logger(
                    "Fetched Okera information for table '" + table.db[0] + "." + table.name + "'",
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
            if table.db[0] + "." + table.name == asset_name:
                okera_tab_logger = Logger(
                    "Fetched Okera information for table '" + table.db[0] + "." + table.name + "'",
                    table.db[0] + "." + table.name, 
                    "Table", 
                    str(table.metadata.get('collibra_asset_id')), 
                    {'system': "Okera", 'database': table.db[0]},
                    {'description': str(table.description), 'tags': create_tags(table.attribute_values)}
                )
                loggers.append(okera_tab_logger)
                diff(table, okera_tab_logger)

which_asset = input("Please enter the full name the asset you wish to update: ")
which_type = input("Is this asset of the type Database or Table? ")
if which_asset and which_type:
    if which_type.capitalize() == "Table":
        print("WARNING: If a table in Collibra in mapped to one or multiple tables in with different names Okera, the entire database must be updated.")
        export(which_asset, which_type.capitalize())
        log_summary()
        logging.info("Export complete!")
        print("Export complete!")
    else:
        export(which_asset, which_type.capitalize())
        log_summary()
        logging.info("Export complete!")
        print("Export complete!")
else:
    while not which_asset:
        which_asset = input("The full name of the asset is required: ")
    while not which_type:
        which_type = input("The asset type is required: ")
    export(which_asset, which_type)
    log_summary()
    logging.info("Export complete!")
    print("Export complete!")
