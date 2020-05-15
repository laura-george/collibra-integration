import json
import requests
import thriftpy
import datetime
import yaml
import sys
import os
import hashlib
import copy
from okera import context
from pymongo import MongoClient
import pprint
import logging
import collections

# Okera token passed as environment variable
TOKEN = os.getenv('TOKEN')
# location of config.yaml
CONF = os.getenv('CONF')
# Collibra environment credentials
env_un = os.getenv('collibra_username')
env_pw = os.getenv('collibra_password')
# Okera planner port
planner_port = 12050
# list of all Okera dbs, datasets and columns (not nested)
assets = []
# list of all Okera tables with columns nested
okera_tables = []
# list of all Okera databases
okera_dbs = []
# Okera column type IDs
type_ids = ["BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING", "VARCHAR",
    "CHAR", "DECIMAL", "TIMESTAMP_NANOS", "RECORD", "ARRAY", "MAP", "BINARY", "DATE"]
# List of collibra attribute types that is imported and updated
collibra_attributes = ["Description", "Location", "Column Position", "Technical Data Type"]

def error_out():
    logging.info("Could not start import, terminating script")
    print("Import failed! For more information check import.log.")
    sys.exit(1)

def pyokera_error(e):
    logging.error("\tPYOKERA ERROR")
    logging.error("\tCould not connect to Okera!")
    logging.error("\tError: " + repr(e))

class Asset:
    def __init__(self, name, asset_type, asset_type_id=None, asset_id=None, displayName=None,
                relation=None, children=None, attributes=None, attribute_ids=None,
                tags=None, last_collibra_sync_time=None, table_hash=None):
        self.name = name
        self.asset_type = asset_type
        self.asset_type_id = asset_type_id
        self.asset_id = asset_id
        self.displayName = displayName
        self.relation = relation
        self.children = children
        self.attributes = attributes
        self.attribute_ids = attribute_ids
        self.tags = tags
        self.last_collibra_sync_time = last_collibra_sync_time
        self.table_hash = table_hash
    def __eq__(self, other):
        if self.asset_id and other.asset_id:
            return self.asset_id == other.asset_id
        else : return self.name == other.name
    def __str__(self):
        child_names = []
        ret = self.__dict__.copy()
        if ret['children'] is not None:
            for c in ret['children']:
                child_names.append(c.name)
            ret['children'] = child_names
        return pprint.pformat(ret)

class Logger:
    def __init__(self, log_type, asset):
        self.log_type = log_type
        self.asset = asset
    def __str__(self):
        return "### " + str(self.log_type) + " ###\n" + self.asset

try:
    if CONF:
        logging.info("Opening " + str(CONF))
        config = str(CONF)
    else:
        logging.info("Opening config.yaml")
        config = 'config.yaml'
    with open(config) as f:
        configs = yaml.load(f, Loader=yaml.FullLoader)['configs']
    required = ['collibra_dgc', 'okera_host', 'log_directory']
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
log_file = os.path.join(configs['log_directory'], 'import.log')
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename=log_file, level=logging.DEBUG)
logging.info("Logging to " + log_file)

def get_credentials(cred_type):
    if not env_un and not configs['collibra_username']:
        logging.error("Collibra username is not set! To set username run $ export collibra_username=$username or set in " + str(config))
        error_out()
    if not env_pw and not configs['collibra_password']:
        logging.error("Collibra password is not set! To set username run $ export collibra_password=$password or set in " + str(config))
        error_out()
    collibra_username = env_un if env_un else configs['collibra_username']
    collibra_password = env_pw if env_pw else configs['collibra_password']
    return collibra_username if cred_type == "username" else collibra_password

update_assets = None
if configs['collibra_assets']:
    try:
        logging.info("Opening " + str(configs['collibra_assets']))
        with open(configs['collibra_assets']) as f:
            update_assets = yaml.load(f, Loader=yaml.FullLoader)
    except yaml.YAMLError as e:
        logging.error("Error in {assets_file}: {exception}".format(assets_file=configs['collibra_assets'], exception=repr(e)))

try:
    logging.info("Opening resourceids.yaml")
    with open('resource_ids.yaml') as f:
        resource_ids = yaml.load(f, Loader=yaml.FullLoader)
except yaml.YAMLError as e:
    logging.error("Error in resourceids.yaml: " + repr(e))
for r in resource_ids:
    for line in resource_ids[r]:
        for key in line:
            if line[key] == "":
                logging.error("Empty value in line " + str(line) + " in resourceids.yaml!")
                error_out()

# builds and makes collibra request
def collibra_get(param_obj, call, method, header=None):
    def api_error():
        logging.warning("COLLIBRA CORE API ERROR")
        logging.warning("Request body: " + str(param_obj))
        logging.warning("Error: " + repr(e))
        logging.warning("Response: " + str(data.content))

    if method == 'get':
        try:
            logging.info("### START: Making Collibra request ###")
            data = getattr(requests, method)(
            configs['collibra_dgc'] + "/rest/2.0/" + call,
            params=param_obj,
            auth=(get_credentials("username"), get_credentials("password")))
            data.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if json.loads(data.content).get('errorCode') != "mappingNotFound":
                api_error()
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
            api_error()
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

# escapes special characters
def escape(string, remove_whitespace=False):
    if string:
        if remove_whitespace:
            return json.dumps(string.replace(" ", "_"))[1:-1]
        else: return json.dumps(string)[1:-1]

# creates list of tags of one asset as namespace.key
def create_tags(attribute_values):
    attributes = []
    no_duplicates = []
    if attribute_values:
        logging.info("\t### START: Format tags ###")
        logging.info("\t\tFormatting tags for Okera as 'namespace.key'")
        for attribute in attribute_values:
            name = attribute.attribute.attribute_namespace + "." + attribute.attribute.key
            attributes.append(name)
        for attr in list(dict.fromkeys(attributes)):
            no_duplicates.append(attr)
        logging.debug("\t\tFormatted tags: " + str(attributes))
        logging.info("\t### END: Format tags ###")
        return no_duplicates

# finds assetID in list of assets from Okera (for finding assetID of relations)
def find_okera_info(asset_id=None, name=None, info=None):
    for a in assets:
        if asset_id and asset_id == a.asset_id:
            return getattr(a, info)
        elif name and a.name == name:
            return getattr(a, info)

# resourceids.yaml search functions
def get_resource_ids(search_in, name, name2=None):
    logging.info("\t### START: Get resource IDs from resourceids.yaml ###")
    logging.info("\t\tFetching resource ID for '" + name + "' in '" + search_in + "'")
    for r in resource_ids[search_in]:
        if search_in == 'relations':
            if r['head'] == name or r['head'] == name2 and r['tail'] == name or r['tail'] == name2:
                logging.info("\t\tSuccessfully fetched resource ID " + r['id'] + " for '" + name + "'")
                logging.info("\t### END: Get resource IDs from resourceids.yaml ###")
                return r
        else:
            if r['name'] == name:
                logging.info("\t\tSuccessfully fetched " + r['id'] + " for '" + name + "'")
                logging.info("\t### END: Get resource IDs from resourceids.yaml ###")
                return r['id']

def find_relation_id(asset1, asset2):
    for r in resource_ids['relations']:
        if r['head'] == asset1 or r['head'] == asset2 and r['tail'] == asset1 or r['tail'] == asset2:
            return r

def pyokera_calls(asset_name=None, asset_type=None):
    logging.info("############ START: PyOkera calls ############")
    complex_type_ids = [type_ids.index("ARRAY"), type_ids.index("MAP"), type_ids.index("RECORD")]
    try:
        conn = ctx.connect(host = configs['okera_host'], port = planner_port)
    except thriftpy.transport.TException as e:
        pyokera_error(e)
        error_out()
    # creates an Asset object for each database, table and column in Okera and adds it to the list assets[]
    with conn:
        databases = conn.list_databases()
        def set_tables():
            okera_columns = []
            table = Asset(
                name=escape(t.db[0] + "." + t.name),
                asset_type="Table",
                asset_id=t.metadata.get('collibra_asset_id') if t.metadata.get('collibra_asset_id') else None,
                displayName=escape(t.name),
                relation={'Name': escape(t.db[0]), 'Type': "Database"},
                attributes={'Description': escape(t.description) if t.description else None,
                'Location': escape(t.location) if t.location else ""},
                tags=create_tags(t.attribute_values),
                last_collibra_sync_time=t.metadata.get('last_collibra_sync_time'),
                table_hash=t.metadata.get('table_hash') if t.metadata.get('table_hash') else None
                )

            def add_col(col, col_position, name=None, data_type=None):
                col_name = name if name else col.name
                column = Asset(
                    name=escape(t.db[0] + "." + t.name + "." + col_name),
                    asset_type="Column",
                    displayName=escape(col_name),
                    relation={'Name': escape(t.db[0] + "." + t.name), 'id': t.metadata.get('collibra_asset_id'), 'Type': "Table"},
                    attributes={'Description': escape(col.comment) if col.comment else None,
                                'Column Position': float(col_position),
                                'Technical Data Type': str(data_type) if data_type else type_ids[col.type.type_id]},
                    tags=create_tags(col.attribute_values)
                )
                assets.append(column)
                okera_columns.append(column)

            def set_cols(col, col_position, parent):
                parent += col.name
                # complex types
                if col.type.num_children is not None:
                    if col.type.type_id == type_ids.index("ARRAY"):
                        technical_data_type = type_ids[col.type.type_id] + "&lt;" + type_ids[t.schema.cols[col_position + 1].type.type_id] + "&gt;"
                        add_col(col, col_position, name=parent, data_type=technical_data_type)
                        col_position += 1
                        item_col = t.schema.cols[col_position]
                        col = item_col
                    if col.type.type_id == type_ids.index("MAP"):
                        technical_data_type = type_ids[col.type.type_id] + "&lt;" + type_ids[t.schema.cols[col_position + 1].type.type_id] + ", " + type_ids[t.schema.cols[col_position + 2].type.type_id] + "&gt;"
                        add_col(col, col_position, name=parent, data_type=technical_data_type)
                        col_position += 2
                        item_col = t.schema.cols[col_position]
                        col = item_col
                    else:
                        add_col(col, col_position, name=parent)
                    j = col_position + 1
                    num_children = col.type.num_children if col.type.num_children else 0
                    while j < col_position + 1 + num_children:
                        j = set_cols(t.schema.cols[j], j, parent=parent + ".")
                    col_position = j
                # non complex types
                else:
                    add_col(col, col_position, name=parent)
                    col_position += 1
                return col_position

            i = 0
            while i < len(t.schema.cols):
                col = t.schema.cols[i]
                i = set_cols(col, i, "")
            assets.append(table)
            table.children = okera_columns
            okera_tables.append(table)

        if asset_name and asset_type == "Database":
            logging.info("Fetching tables for database '" + asset_name + "'")
            okera_dbs.append(Asset(name=asset_name, asset_type="Database"))
            tables = conn.list_datasets(asset_name)
            assets.append(Asset(name=asset_name, asset_type="Database"))
            if tables:
                for t in tables: set_tables()
        elif asset_name and asset_type == "Table":
            logging.info("\tFetching table '" + asset_name + "'")
            assets.append(Asset(asset_name.rsplit('.', 1)[0], "Database"))
            tables = conn.list_datasets(db=asset_name.rsplit('.', 1)[0], name=asset_name.rsplit('.', 1)[1])
            t = tables[0]
            set_tables()
        elif not asset_name and not asset_type:
            logging.info("\tFetching all databases")
            for d in databases:
                okera_dbs.append(Asset(name=d, asset_type="Database"))
                tables = conn.list_datasets(d)
                assets.append(Asset(d, "Database"))
                set_tables()
    logging.info("############ END: PyOkera calls ############")

# finds Asset objects from Okera in assets[], adds asset type id to object
def get_okera_assets(name=None, asset_type=None, asset_id=None):
    # if name, then column or database, if id then table
    okera_assets = []
    asset = None
    for a in assets:
        if name and not asset_id or asset_type=="Column":
            if a.name == name and a.asset_type == asset_type:
                a.asset_type_id = get_resource_ids('assets', a.asset_type)
                asset = a
                okera_assets.append(a)
        elif asset_id:
            if a.asset_id == asset_id and a.asset_type == asset_type:
                a.asset_type_id = get_resource_ids('assets', a.asset_type)
                asset = a
                okera_assets.append(a)
        else:
            a.asset_type_id = get_resource_ids('assets', a.asset_type)
            asset = a
            okera_assets.append(a)
    if len(okera_assets) > 1:
        return okera_assets
    else:
        return asset

def set_tblproperties(name=None, asset_id=None, asset_type=None, key=None, value=None):
    logging.info("### START: Set Okera table properties ###")
    if name and not asset_id:
        asset = get_okera_assets(name=name, asset_type="Table")
    elif asset_id:
        asset = get_okera_assets(asset_id=asset_id, asset_type="Table")
    try:
        conn = ctx.connect(host = configs['okera_host'], port = planner_port)
    except thriftpy.transport.TTransportException as e:
        pyokera_error(e)
        error_out()
    with conn:
        if asset_type == "Table":
            logging.debug("\tAdding table properties for table '" + str(asset.name) + "'")
            try:
                conn.execute_ddl("ALTER TABLE " + asset.name + " SET TBLPROPERTIES ('" + key + "' = '" + str(value) + "')")
                logging.info("\tSuccessfully added table property ('{key}' = '{value}') to table '{name}'".format(
                        key=key,
                        value=value,
                        name=asset.name
                    ))
            except thriftpy.thrift.TException as e:
                logging.error("\tPYOKERA ERROR")
                logging.error("\tCould not set table property {key} = {value} for table {name}!".format(
                        key=key,
                        value=value,
                        name=asset.name
                    ))
                logging.error("\tError: ", repr(e))
    logging.info("### END: Set Okera table properties ###")

def set_sync_time(name, asset_id, asset_type):
    set_tblproperties(name, asset_id, asset_type, "last_collibra_sync_time", value=int(datetime.datetime.utcnow().timestamp()))

# gets assets and their children from Collibra
def collibra_calls(asset_id=None, asset_name=None, asset_type=None):
    logging.info("############ START: Collibra calls ############")
    asset_param = {
            'name': asset_name,
            'nameMatchMode': "EXACT",
            'domainId': domain_id,
            'communityId': community_id,
            'typeId': get_resource_ids('assets', asset_type)
            }
    if asset_id:
        #finds name of asset from asset id
        asset_param['name'] = json.loads(collibra_get(None, "assets/" + asset_id, "get")).get('name')
        logging.info("\tFetching Collibra " + str(asset_type) + " '" + str(asset_param['name']) + ", asset ID: " + str(asset_id))
        logging.info("############ END: Collibra calls ############")
        return json.loads(collibra_get(asset_param, "assets", "get")).get('results')

    elif asset_name and not asset_id:
        logging.info("\tFetching Collibra " + str(asset_type) + " '" + str(asset_name) + "'")
        logging.info("############ END: Collibra calls ############")
        return json.loads(collibra_get(asset_param, "assets", "get")).get('results')

    else:
        get_all_param = {
            'domainId': domain_id,
            'communityId': community_id,
            }
        logging.info("############ END: Collibra calls ############")
        return json.loads(collibra_get(get_all_param, "assets", "get")).get('results')

# sets the parameters for /assets Collibra call
def set_assets(asset):
    return ({
    'name': asset.name,
    'displayName': asset.displayName,
    'domainId': domain_id,
    'typeId': asset.asset_type_id,
    'statusId': get_resource_ids('statuses', "Candidate"),
    'excludedFromAutoHyperlinking': "true"
    })

# sets the parameters for /attributes Collibra call
def set_attributes(asset, attr_name=None):
    attribute_params = []
    if asset.attributes:
        def group_attributes(attr):
            attribute_params.append({
                    'assetId': asset.asset_id,
                    'typeId': get_resource_ids('attributes', attr),
                    'value': asset.attributes.get(attr)
                    })
        if attr_name:
            group_attributes(attr_name)
        else:
            for attr in collibra_attributes:
                if attr in asset.attributes and asset.attributes.get(attr) != None: group_attributes(attr)
        return attribute_params

# sets the parameters for /relations Collibra call
def set_relations(asset):
    if asset.relation:
        relation_info = get_resource_ids('relations', asset.asset_type, asset.relation.get('Type'))
        return {
            'sourceId': asset.asset_id if asset.asset_type == relation_info.get('head') else collibra_calls(asset_name=asset.relation.get('Name'), asset_type=asset.relation.get('Type'))[0].get('id'),
            'targetId': asset.asset_id if asset.asset_type == relation_info.get('tail') else collibra_calls(asset_name=asset.relation.get('Name'), asset_type=asset.relation.get('Type'))[0].get('id'),
            'typeId': relation_info.get('id')
        }

loggers = []
def update(asset_name=None, asset_id=None, asset_type=None):
    collibra_assets = []
    import_params = []
    deleted_assets = []
    deleted_params = []
    relation_params = []
    mapping_params = []
    for a in collibra_calls(asset_id, asset_name, asset_type):
        asset = Asset(
            a.get('name'),
            a.get('type').get('name'),
            a.get('type').get('id'),
            a.get('id'),
            a.get('displayName')
        )
        collibra_assets.append(asset)
    # IMPORTS
    new_asset = get_okera_assets(name=asset_name, asset_type=asset_type, asset_id=asset_id)
    if new_asset not in collibra_assets:
        logging.info("############ START: Import new asset '" + new_asset.name + "' ############")
        set_tblproperties(name=new_asset.name, asset_type=new_asset.asset_type, key="collibra_asset_id", value="")
        import_param = set_assets(new_asset)
        import_response = json.loads(collibra_get(import_param, "assets", "post"))
        if not import_response:
            logging.warning("Empty Collibra import response: Asset '" + new_asset.name + "' not imported into Collibra!")
        else:
            logging.info("Successfully imported asset '" + new_asset.name + "', information imported: ")
            logging.debug(new_asset)
        if import_response.get('name') == new_asset.name:
            new_asset.asset_id = import_response.get('id')
            set_sync_time(new_asset.name, new_asset.asset_id, new_asset.asset_type)
        new_relations = set_relations(new_asset)
        if new_relations:
            logging.info("\tSetting relations for asset '" + new_asset.name + "'")
            collibra_get(new_relations, "relations", "post")
            logging.info("\tRelations set for asset '" + new_asset.name + "': " + str(new_relations))
        new_attributes = set_attributes(new_asset)
        if new_attributes:
            logging.info("\tSetting attributes for asset '" + new_asset.name + "'")
            for attr in new_attributes:
                collibra_get(attr, "attributes", "post")
            logging.info("\tAttributes set for asset '" + new_asset.name + "': " + str(new_attributes))
        set_sync_time(new_asset.name, new_asset.asset_id, new_asset.asset_type)
        # commenting out Collibra mappings for now, don't have much use
        """ mapping_param = {
            "externalSystemId": "okera_import2",
            "externalEntityId": new_asset.name,
            "mappedResourceId": new_asset.asset_id,
            "lastSyncDate": 0,
            "syncAction": "ADD"
        }
        collibra_get(mapping_param, "mappings", "post") """
        # set column order here
        if new_asset.tags:
            logging.info("\tSetting tags for asset '" + new_asset.name + "'")
            tag_params = {'tagNames': new_asset.tags}
            collibra_get(tag_params, "assets/" + new_asset.asset_id + "/tags", "post")
        set_tblproperties(name=new_asset.name, asset_type=new_asset.asset_type, key="collibra_asset_id", value=new_asset.asset_id)

    for not_found in [obj for obj in collibra_assets if obj not in assets]:
        logging.debug("### Asset '{name}' not found in Collibra domain '{domain}' in community '{community}' ###".format(
            name=not_found.name,
            domain=domain,
            community=community))
        print("Asset '{name}' not found in Collibra domain '{domain}' in community '{community}'".format(
            name=not_found.name,
            domain=domain,
            community=community))

    # UPDATES (asset already exists in Collibra)
    for c in collibra_assets:
        collibra_tags = []
        attributes = {}
        attribute_ids = {}
        okera_tags = None
        loggers.append(Logger("COLLIBRA {asset_type} '{name}'".format(asset_type=c.asset_type.upper(), name=c.name), str(c)))
        #unsure about this
        if asset_name and c.name == asset_name or asset_id and c.asset_id == asset_id:
            logging.info("############ START: Update asset '" + c.name + "' ############")
            for tag in json.loads(collibra_get(None, "assets/" + c.asset_id + "/tags", "get")):
                collibra_tags.append(tag.get('name'))
            if asset_type == "Table":
                okera_tags = find_okera_info(c.asset_id, None, "tags")
            else: okera_tags = find_okera_info(None, c.name, "tags")
            tag_params = {'tagNames': okera_tags}
            okera_name = find_okera_info(c.asset_id, None, "name") if asset_id else c.name
            logging.info(
                "###### Comparing Collibra tags of {asset_type} '{collibra_name}' to Okera tags of {asset_type} '{okera_name}': ######"
                "\nCollibra tags: {collibra_tags}\nOkera tags: {okera_tags}".format(
                    asset_type=c.asset_type,
                    collibra_name=c.name,
                    okera_name=okera_name,
                    collibra_tags=collibra_tags,
                    okera_tags=okera_tags))
            if okera_tags and not collibra_tags:
                collibra_get(tag_params, "assets/" + c.asset_id + "/tags", "post")
                set_sync_time(c.name, c.asset_id, c.asset_type)
            elif collibra_tags and okera_tags and collections.Counter(collibra_tags) != collections.Counter(okera_tags):
                new_tags = []
                for new_tag in [tag for tag in okera_tags if tag not in collibra_tags]:
                    new_tags.append(new_tag)
                new_tag_params = {'tagNames': new_tags}
                collibra_get(new_tag_params, "assets/" + asset.asset_id + "/tags", "post")
                set_sync_time(c.name, c.asset_id, c.asset_type)
                # commenting out deletion for now, tags should not be overwritten
            else:
                logging.debug("\tNo differences found")

            attribute = json.loads(collibra_get({'assetId': c.asset_id}, "attributes", "get")).get('results')
            matched_attr = find_okera_info(asset_id=c.asset_id, info="attributes") if asset_id else find_okera_info(name=c.name, info="attributes")
            for attr in attribute:
                attributes.update({attr.get('type').get('name'): attr.get('value')})
                attribute_ids.update({attr.get('type').get('name'): attr.get('id')})
            c.attributes = attributes
            c.attribute_ids = attribute_ids
            update_attr = []
            import_attr = []
            delete_attr = []
            logging.info(
                "###### Comparing Collibra attributes of {asset_type} '{collibra_name}' to Okera attributes of {asset_type} '{okera_name}': ######"
                "\nCollibra attributes: {collibra_attr}\nOkera attributes: {okera_attr}".format(
                    asset_type=c.asset_type,
                    collibra_name=c.name,
                    okera_name=okera_name,
                    collibra_attr=c.attributes,
                    okera_attr=matched_attr))
            if c.attributes and matched_attr:
                for key in matched_attr:
                    if key in c.attributes:
                        if matched_attr[key] != None and c.attributes[key] != matched_attr[key]:
                            update_attr.append({'id': c.attribute_ids[key], 'value': matched_attr[key]})
                        elif matched_attr[key] == None:
                            delete_attr.append(c.attribute_ids[key])
                    elif matched_attr[key] != None:
                        a = get_okera_assets(name=c.name, asset_type=c.asset_type, asset_id=c.asset_id)
                        a.asset_id = c.asset_id
                        import_attr.append(set_attributes(a, key)[0])
                    else:
                        logging.debug("\tNo differences found")
            elif matched_attr and not c.attributes:
                for key in matched_attr:
                    if matched_attr[key] != None and matched_attr[key] != '':
                            a = get_okera_assets(name=c.name, asset_type=c.asset_type)
                            a.asset_id = c.asset_id
                            import_attr.append(set_attributes(a, key)[0])
            else:
                logging.debug("\tNo differences found")
            # Not using PATCH for new attribute if one attr already exists
            if update_attr:
                logging.info("\tUpdating attributes for asset '" + c.name + "'")
                logging.debug("\tAttributes: " + str(update_attr))
                collibra_get(update_attr, "attributes/bulk", "post", {'X-HTTP-Method-Override': "PATCH"})
                set_sync_time(c.name, c.asset_id, c.asset_type)
            if delete_attr:
                logging.info("\tDeleting attributes for asset '" + c.name + "'")
                logging.debug("\tAttributes: " + str(delete_attr))
                collibra_get(delete_attr, "attributes/bulk", "delete")
                set_sync_time(c.name, c.asset_id, c.asset_type)
            if import_attr:
                logging.info("\tSetting attributes for asset '" + c.name + "'")
                logging.debug("\tAttributes: " + str(import_attr))
                collibra_get(import_attr, "attributes/bulk", "post")
                set_sync_time(c.name, c.asset_id, c.asset_type)

def replace_none(s):
    return '' if s is None else str(s)

def create_table_hash(table):
    logging.info("### START: Create table hash ###")
    column_values = []
    if table.children:
        for c in table.children:
            column_values.append(c.name + c.asset_type + replace_none(json.dumps(c.relation)) + replace_none(json.dumps(c.attributes)) + replace_none(json.dumps(c.tags)))
    table_hash_values = (table.name + table.asset_type + replace_none(table.asset_id) +
    replace_none(json.dumps(table.relation)) + replace_none(json.dumps(column_values)) +
    replace_none(json.dumps(table.attributes)) + replace_none(json.dumps(table.tags)))
    try:
        table_hash = hashlib.md5(table_hash_values.encode()).hexdigest()
        logging.info("\tSuccessfully created hash for table '" + str(table.name) + "': " + str(table_hash))
    except Exception as e:
        logging.warning("\tCould not create hash for table '" + str(table.name) + "'!")
        logging.warning("\tError: " + repr(e))
    logging.info("### END: Create table hash ###")
    return table_hash

def log_summary():
    logging.info("############ SUMMARY: ASSETS FETCHED AND COMPARED ############")
    for logger in loggers: logging.debug(logger)

#group together tables and their columns and iterate over list and call update()
def update_all(mode, name=None, asset_type=None):
    pyokera_calls(name, asset_type)
    def update_ops(table, tab_hash):
        loggers.append(Logger("OKERA TABLE '" + table.name + "'", str(table)))
        if table.asset_id:
            update(asset_id=table.asset_id, asset_type="Table")
            set_tblproperties(table.name, table.asset_id, asset_type="Table", key="table_hash", value=tab_hash)
            for c in table.children:
                loggers.append(Logger("OKERA COLUMN '" + c.name + "'", str(c)))
                update(asset_name=c.name, asset_type="Column")
        else:
            update(asset_name=table.name, asset_type="Table")
            set_tblproperties(table.name, table.asset_id, asset_type="Table", key="table_hash", value=tab_hash)
            for c in table.children:
                loggers.append(Logger("OKERA COLUMN '" + c.name + "'", str(c)))
                update(asset_name=c.name, asset_type="Column")
    if asset_type == "Database":
        for d in okera_dbs: update(asset_name=d.name, asset_type="Database")
    for t in okera_tables:
        new_table_hash = create_table_hash(t)
        if mode == "default":
            # check for last sync time will happen here
            if (t.table_hash == None or (t.table_hash and t.table_hash != new_table_hash)):
                update_ops(t, new_table_hash)
            else:
                loggers.append(Logger("OKERA TABLE '" + t.name + "'", str(t)))
                logging.info("\tNo changes found: Table '" + t.name + "' is up to date!")
        elif mode == "force": update_ops(t, new_table_hash)

mode = 'force' if 'f' in sys.argv else 'default'

this = False
if this == True:
    raise Exception()
if update_assets:
    logging.info("Using assets in '" + configs['collibra_assets'] + "' to run script...")
    this = True
    for comm in update_assets['communities']:
        community = comm['name']
        community_id = comm['id']
        for dom in comm['domains']:
            domain = dom['name']
            domain_id = dom['id']
            if dom['databases']:
                for db in dom['databases']:
                    assets = []
                    # list of all Okera tables with columns nested
                    okera_tables = []
                    # list of all Okera databases
                    okera_dbs = []
                    update_all(mode, db, 'Database')
            if dom['tables']:
                for table in dom['tables']:
                    assets = []
                    # list of all Okera tables with columns nested
                    okera_tables = []
                    # list of all Okera databases
                    okera_dbs = []
                    update_all(mode, table, 'Table')
    log_summary()
    logging.info("Import complete!")
    print("Import complete!")
else:
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
    valid = ["table", "database", ""]
    while which_type not in valid:
        which_type = input("Please enter a valid asset type: ")
    which_asset = input("Please enter the full name the " + which_type + ": ")
    if which_asset and which_type:
        update_all(mode, which_asset, which_type.capitalize())
        logging.info("Import complete!")
        print("Import complete!")
    else:
        update_all(mode)
        logging.info("Import complete!")
        print("Import complete!")