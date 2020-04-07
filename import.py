# CURRENTLY TESTING WITH COLLIBRA_IMPORT
import json
import requests
import thriftpy
import datetime
import yaml
import sys
import hashlib
from okera import context
from pymongo import MongoClient
import logging
import collections

logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename='import.log',level=logging.DEBUG)
# Okera planner port
planner_port = 12050
# list of all Okera dbs, datasets and columns (not nested)
assets = []
# list of all Okera tables with columns nested
okera_tables = []
# list of all Okera databases
okera_dbs = []
# Okera column type IDs
type_ids = ["BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING", "VARCHAR", "CHAR", "BINARY", "TIMESTAMP_NANOS", "DECIMAL", "DATE", "RECORD", "ARRAY", "MAP"]

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

class Asset:
    def __init__(self, name, asset_type, asset_type_id=None, asset_id=None, displayName=None, relation=None, children=None, attributes=None, attribute_ids=None, tags=None, last_collibra_sync_time=None, table_hash=None):
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
        return str(self.__class__) + ": " + str(self.__dict__)

try:
    logging.info("Opening config.yaml")
    with open('config.yaml') as f:
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

try:
    logging.info("Opening resourceids.yaml")
    with open('resource_ids.yaml') as f:
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
                logging.warning("Error: " + str(e))
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
            logging.warning("Error: " + str(e))
            logging.warning("Response: " + str(data.content))
        logging.info("Request successful")
        logging.info("### END: Making Collibra request ###")
        return data.content

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

ctx = context()
ctx.enable_token_auth(token_str=configs['token'])

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
            print("find relation id", r)
            return r

def pyokera_calls(asset_name=None, asset_type=None):
    logging.info("############ START: PyOkera calls ############")
    try:
        conn = ctx.connect(host = configs['host'], port = planner_port)
    except thriftpy.transport.TException as e:
        logging.error("PYOKERA ERROR")
        logging.error("Could not connect to Okera!")
        logging.error("Error: " + str(e))
        logging.info("Could not start export, terminating script")
        print("Export failed! For more information check export.log.")
        sys.exit(1)
    # creates an Asset object for each database, table and column in Okera and adds it to the list assets[]
    with conn:
        databases = conn.list_databases()
        def set_tables():
            if tables:
                for t in tables:
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
                    for col in t.schema.cols:
                        column = Asset(
                            name=escape(t.db[0] + "." + t.name + "." + col.name),
                            asset_type="Column",
                            displayName=escape(col.name),
                            relation={'Name': escape(t.db[0] + "." + t.name), 'id': t.metadata.get('collibra_asset_id'), 'Type': "Table"},
                            attributes={'Description': escape(col.comment) if col.comment else None}, # 'Technical Data Type': type_ids[col.type.type_id]},
                            tags=create_tags(col.attribute_values)
                        )
                        assets.append(column)
                        okera_columns.append(column)
                    table.children = okera_columns
                    assets.append(table)
                    table.children = okera_columns
                    okera_tables.append(table)
        if asset_name and asset_type == "Database":
            okera_dbs.append(Asset(name=asset_name, asset_type="Database"))
            tables = conn.list_datasets(asset_name)
            db = Asset(name=asset_name, asset_type="Database")
            set_tables()
            assets.append(db)
        elif asset_name and asset_type == "Table":
            tables = conn.list_datasets(asset_name.rsplit('.', 1)[0])
            assets.append(Asset(asset_name.rsplit('.', 1)[0], "Database"))
            set_tables()
        elif not asset_name and not asset_type:
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
        if name and not asset_id:
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
    if name:
        asset = get_okera_assets(name=name, asset_type="Table")
    elif asset_id:
        # why is this being calles 2 times..?
        print("set_tblproperties: ", asset_id)
        asset = get_okera_assets(asset_id=asset_id, asset_type="Table")
    try:
        conn = ctx.connect(host = configs['host'], port = planner_port)
    except thriftpy.transport.TTransportException as e:
        print("PYOKERA ERROR")
        print("Could not connect to Okera!")
        print("Error: ", e)
        sys.exit(1)
    with conn:
        if asset_type == "Table":
            try:
                conn.execute_ddl("ALTER TABLE " + asset.name + " SET TBLPROPERTIES ('" + key + "' = '" + str(value) + "')")
            except thriftpy.thrift.TException as e:
                print("PYOKERA ERROR")
                print("Could not set table property " + key + " = " + str(value) + " for table " + asset.name + "!")
                print("Error: ", e)

def set_sync_time(asset_id, asset_type):
    set_tblproperties(asset_id=asset_id, asset_type=asset_type, key="last_collibra_sync_time", value=int(datetime.datetime.utcnow().timestamp()))

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
def set_attributes(asset):
    attribute_params = []
    if asset.attributes:
        def group_attributes(attr):
            attribute_params.append({
                    'assetId': asset.asset_id,
                    'typeId': get_resource_ids('attributes', attr),
                    'value': asset.attributes.get(attr)
                    })

        if asset.attributes.get('Description'): group_attributes('Description')
        if asset.attributes.get('Location'): group_attributes('Location')
        #if asset.attributes.get('Technical Data Type'): group_attributes('Technical Data Type')
        print("attr params: ", attribute_params)
        return attribute_params

# sets the parameters for /relations Collibra call
def set_relations(asset):
    if asset.relation:
        relation_info = get_resource_ids('relations', asset.asset_type, asset.relation.get('Type'))
        return {
            'sourceId': asset.asset_id if asset.asset_type == relation_info.get('head') else collibra_calls(None, asset.relation.get('Name'), asset.relation.get('Type'))[0].get('id'),
            'targetId': asset.asset_id if asset.asset_type == relation_info.get('tail') else collibra_calls(None, asset.relation.get('Name'), asset.relation.get('Type'))[0].get('id'),
            'typeId': relation_info.get('id')
        }

def update(asset_name=None, asset_type=None, asset_id=None):
    collibra_assets = []
    import_params = []
    deleted_assets = []
    deleted_params = []
    relation_params = []
    mapping_params = []
    for a in collibra_calls(None, asset_name, asset_type):
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
        set_tblproperties(name=new_asset.name, asset_type=new_asset.asset_type, key="collibra_asset_id", value="")
        import_param = set_assets(new_asset)
        import_response = json.loads(collibra_get(import_param, "assets", "post"))
        print("Importing " + new_asset.name + "...")
        if import_response.get('name') == new_asset.name:
            new_asset.asset_id = import_response.get('id')
            set_sync_time(new_asset.asset_id, new_asset.asset_type)
        if set_relations(new_asset):
            collibra_get(set_relations(new_asset), "relations", "post")
        new_attributes = set_attributes(new_asset)
        if new_attributes:
            print("new attributes: ", new_attributes)
            for attr in new_attributes:
                collibra_get(attr, "attributes", "post")
        set_sync_time(new_asset.asset_id, new_asset.asset_type)
        mapping_param = {
            "externalSystemId": "okera_import",
            "externalEntityId": new_asset.name,
            "mappedResourceId": new_asset.asset_id,
            "lastSyncDate": 0,
            "syncAction": "ADD"
        }
        collibra_get(mapping_param, "mappings", "post")
        if new_asset.tags:
            tag_params = {'tagNames': new_asset.tags}
            collibra_get(tag_params, "assets/" + new_asset.asset_id + "/tags", "post")
        set_tblproperties(name=new_asset.name, asset_type=new_asset.asset_type, key="collibra_asset_id", value=new_asset.asset_id)

    for deleted_asset in [obj for obj in collibra_assets if obj not in assets]:
        hello = []
        print("deleted asset found: ", deleted_asset.name)
        collibra_get(None, "assets/" + deleted_asset.asset_id, "delete")

    for c in collibra_assets:
        collibra_tags = []
        attributes = {}
        attribute_ids = {}
        okera_tags = None
        if c.name and c.name == asset_name or c.asset_id and c.asset_id == asset_id:
            for tag in json.loads(collibra_get(None, "assets/" + c.asset_id + "/tags", "get")):
                collibra_tags.append(tag.get('name'))
            if asset_type == "Table":
                okera_tags = find_okera_info(c.asset_id, None, "tags")
            else: okera_tags = find_okera_info(None, c.name, "tags")
            tag_params = {'tagNames': okera_tags}
            if okera_tags and not collibra_tags:
                collibra_get(tag_params, "assets/" + c.asset_id + "/tags", "post")
                set_sync_time(c.asset_id, c.asset_type)
            elif collibra_tags and okera_tags and collections.Counter(collibra_tags) != collections.Counter(okera_tags):
                collibra_get(tag_params, "assets/" + asset.asset_id + "/tags", "put")
                set_sync_time(c.asset_id, c.asset_type)
            elif collibra_tags and not okera_tags:
                tag_params = {'tagNames': collibra_tags}
                collibra_get(tag_params, "assets/" + c.asset_id + "/tags", "delete")
                set_sync_time(c.asset_id, c.asset_type)

            attribute = json.loads(collibra_get({'assetId': c.asset_id}, "attributes", "get")).get('results')
            matched_attr = find_okera_info(asset_id=c.asset_id, info="attributes") if asset_type == "Table" else find_okera_info(name=c.name, info="attributes")
            for attr in attribute:
                print("attr  in attribute: ", attr)
                attributes.update({attr.get('type').get('name'): attr.get('value')})
                attribute_ids.update({attr.get('type').get('name'): attr.get('id')})
            c.attributes = attributes
            c.attribute_ids = attribute_ids
            update_attr = []
            import_attr = []
            delete_attr = []
            if c.attributes and matched_attr:
                for key in matched_attr:
                    if key in c.attributes:
                        if matched_attr[key] != None and c.attributes[key] != matched_attr[key]:  
                            update_attr.append({'id': c.attribute_ids[key], 'value': matched_attr[key]})
                        elif matched_attr[key] == None:
                            delete_attr.append(c.attribute_ids[key])
                    else:
                        if matched_attr[key] != None:
                            a = get_okera_assets(name=c.name, asset_type=c.asset_type, asset_id=c.asset_id)
                            print(a)
                            print(c)
                            a.asset_id = c.asset_id
                            import_attr.append(set_attributes(a)[0])

            elif matched_attr and not c.attributes:
                for key in matched_attr:
                    if matched_attr[key] != None and matched_attr[key] != '':
                            a = get_okera_assets(name=c.name, asset_type=c.asset_type)
                            a.asset_id = c.asset_id
                            import_attr.append(set_attributes(a)[0])

            if update_attr:
                collibra_get(update_attr, "attributes/bulk", "post", {'X-HTTP-Method-Override': "PATCH"})
                set_sync_time(c.asset_id, c.asset_type)
            if delete_attr:
                collibra_get(delete_attr, "attributes/bulk", "delete")
                set_sync_time(c.asset_id, c.asset_type)
            if import_attr:
                collibra_get(import_attr, "attributes/bulk", "post")
                set_sync_time(c.asset_id, c.asset_type)

def replace_none(s):
    return '' if s is None else str(s)

def create_table_hash(table):
    column_values = []
    if table.children:
        for c in table.children:
            column_values.append(c.name + c.asset_type + replace_none(json.dumps(c.relation)) + replace_none(json.dumps(c.attributes)) + replace_none(json.dumps(c.tags)))
    table_hash_values = table.name + table.asset_type + replace_none(table.asset_id) + replace_none(json.dumps(table.relation)) + replace_none(json.dumps(column_values)) + replace_none(json.dumps(table.attributes)) + replace_none(json.dumps(table.tags))
    return hashlib.md5(table_hash_values.encode()).hexdigest()

#group together tables and their columns and iterate over list and call update()
def update_all():
    pyokera_calls("collibra_import", "Database")
    for d in okera_dbs:
        print(d.name)
        update(d.name, "Database")
    for t in okera_tables:
        print("----TABLE----\n", t.name)
        # check for last sync time will happen here
        new_table_hash = create_table_hash(t)
        if (t.table_hash == None or (t.table_hash and t.table_hash != new_table_hash)):
            #set table properties
            if t.asset_id:
                print('-asset_id-')
                print("t.asset_id:", t.asset_id)
                print(t)
                update(asset_type="Table", asset_id=t.asset_id)
                set_tblproperties(t.name, t.asset_id, asset_type="Table", key="table_hash", value=new_table_hash)
                for c in t.children:
                    print("----COLUMN----\n", c.name)
                    print(c)
                    update(c.name, "Column")
            else:
                print("-name only-")
                print("t.name:", t.name)
                update(t.name, "Table")
                set_tblproperties(t.name, t.asset_id, asset_type="Table", key="table_hash", value=new_table_hash)
                for c in t.children:
                    print("----COLUMN----\n", c.name)
                    print(c)
                    update(c.name, "Column")
update_all()

# TODO add try except block
""" which_asset = input("Please enter the full name the asset you wish to update: ")
which_type = input("Is this asset of the type Database or Table? ")
if which_asset and which_type:
    update(which_asset, which_type)
    print("Update complete!")
elif which_asset and not which_type:
    while not which_type:
        which_type = input("The asset type is required: ")
    update(which_asset, which_type)
    print("Update complete!")
elif not which_asset and not which_type:
    update()
    print("Update complete!") """