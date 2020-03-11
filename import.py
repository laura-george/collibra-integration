import json
import requests
import thriftpy
import datetime
import sys
from okera import context
from pymongo import MongoClient
from config import configs
import collections

client = MongoClient(port=27017)
db = client.collibra_ids
ctx = context()
ctx.enable_token_auth(token_str=configs.get('token'))

community = configs.get('community')
community_id = json.loads(requests.get(
    configs.get('collibra dgc') + "/rest/2.0/communities", 
    params = {'name': community}, 
    auth = (configs.get('collibra username'), configs.get('collibra password'))).content).get('results')[0].get('id')
data_dict_domain = configs.get('data_dict_domain')
domain_id = json.loads(requests.get(
    configs.get('collibra dgc') + "/rest/2.0/domains", 
    params = {'name': data_dict_domain.get('name'), 'communityId': community_id}, 
    auth = (configs.get('collibra username'), configs.get('collibra password'))).content).get('results')[0].get('id')
tech_asset_domain = configs.get('tech_asset_domain')
domain_info = [data_dict_domain, tech_asset_domain]
type_ids = ["BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING", "VARCHAR", "CHAR", "BINARY", "TIMESTAMP_NANOS", "DECIMAL", "DATE", "RECORD", "ARRAY", "MAP"]

class Asset:
    def __init__(self, name, asset_type, asset_type_id=None, asset_id=None, displayName=None, relation=None, children=None, attributes=None, attribute_ids=None, tags=None):
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
    def __eq__(self, other):
        return self.name == other.name
    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

# escapes special characters
def escape(string): return(json.dumps(string)[1:-1])

# creates list of tags of one asset as namespace.key
def create_tags(attribute_values):
    attributes = []
    no_duplicates = []
    if attribute_values:
        for attribute in attribute_values:
            name = attribute.attribute.attribute_namespace + "." + attribute.attribute.key 
            attributes.append(name)
        for attr in list(dict.fromkeys(attributes)):
            no_duplicates.append(attr)
        return no_duplicates

# finds assetID in list of assets from Okera (for finding assetID of relations)
def find_okera_info(asset_id=None, name=None, info=None):
    print("Info: ", info)
    for a in assets:
        if asset_id and asset_id == a.asset_id:
            return getattr(a, info)
        elif name and a.name == name:
            return getattr(a, info)

# builds collibra request
def collibra_get(param_obj, call, method, header=None):
    if method == 'get':
        try: 
            data = getattr(requests, method)(
            configs.get('collibra dgc') + "/rest/2.0/" + call, 
            params=param_obj, 
            auth=(configs.get('collibra username'), configs.get('collibra password')))
            data.raise_for_status()
        except requests.exceptions.HTTPError as e: 
            print("Failed request: " + str(param_obj))
            print("Error: ", e)
            print("Response: " + str(data.content))
        print(data.content)
        return data.content
    else:
        try: 
            data = getattr(requests, method)(
            configs.get('collibra dgc') + "/rest/2.0/" + call,
            headers=header, 
            json=param_obj, 
            auth=(configs.get('collibra username'), configs.get('collibra password')))    
            data.raise_for_status()
        except requests.exceptions.RequestException as e:
            print("Failed request: " + str(param_obj))
            print("Error: ", e)
            print("Response: " + str(data.content))
        print(data.content)
        return data.content

# MongoDB find functions
def find_relation_id(asset1, asset2):
    for x in db.relation_ids.find({'$and': [{'$or': [{'head': asset1}, {'head': asset2}]}, {'$or': [{'tail': asset1}, {'tail': asset2}]}]}):
        return x

def find_attribute_id(name):
    for x in db.attribute_ids.find({'name': name}):
        return x.get('id')

def find_asset_type_id(asset_type):
    for x in db.asset_ids.find({'name': asset_type}):
        return x.get('id')

def find_status_id(name):
    for x in db.status_ids.find({'name': name}):
        return x.get('id')


assets = []
okera_tables = []
okera_dbs = []
# pyokera calls
def pyokera_calls(asset_name=None, asset_type=None):
    try:
        conn = ctx.connect(host = configs.get('host'), port = configs.get('port'))
    except thriftpy.transport.TTransportException as e:
        print("Could not connect to Okera!")
        print("Error: ", e)
        sys.exit(1)
    # creates an Asset object for each database, table and column in Okera and adds it to the list assets[]
    with conn:
        databases = conn.list_databases()
        def set_tables():
            if tables:
                for t in tables:
                    okera_columns = []
                    table = Asset(
                        escape(t.db[0] + "." + t.name),
                        "Table",
                        None,
                        t.metadata.get('collibra_asset_id') if t.metadata.get('collibra_asset_id') else None, 
                        escape(t.name), 
                        {'Name': escape(t.db[0]), 'Type': "Database"},
                        None,
                        {'Description': escape(t.description) if t.description else None, 
                        'Location': escape(t.location) if t.location else None},
                        None, 
                        create_tags(t.attribute_values)
                        )
                    for col in t.schema.cols:
                        column = Asset(
                            escape(t.db[0] + "." + t.name + "." + col.name),
                            "Column",
                            None,
                            None,
                            escape(col.name),
                            {'Name': escape(t.db[0] + "." + t.name), 'id': t.metadata.get('collibra_asset_id'), 'Type': "Table"},
                            None,
                            {'Description': escape(col.comment) if col.comment else None}, # 'Technical Data Type': type_ids[col.type.type_id]},
                            None,
                            create_tags(col.attribute_values)
                        )
                        assets.append(column)
                        okera_columns.append(column)
                    table.children = okera_columns
                    assets.append(table)
                    table.children = okera_columns
                    okera_tables.append(table)
                #db.children = tab_children

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

# ENTERING TABLE HERE DOESNT RETURN RIGHT TABLES??
#pyokera_calls("default", "Database")

# finds Asset objects from Okera in assets[], adds asset type id to object
# used to find assets and their relations, e.g. get_okera_assets("default", "Database") returns the Asset objects for default and all it's tables (default.okera_sample, default. ...)
def get_okera_assets(name=None, asset_type=None, asset_id=None):
    # if name, then column or database, if id then table
    okera_assets = []
    for a in assets:
        if name:
            if a.name == name and a.asset_type == asset_type:
                a.asset_type_id = find_asset_type_id(a.asset_type)
                okera_assets.append(a)
        elif asset_id:
            if a.asset_id == asset_id and a.asset_type == asset_type:
                a.asset_type_id = find_asset_type_id(a.asset_type)
                okera_assets.append(a)
        else:
            a.asset_type_id = find_asset_type_id(a.asset_type)
            okera_assets.append(a)

    if len(okera_assets) > 1:
        return okera_assets
    else:
        for a in okera_assets:
            return a

def set_tblproperties(name=None, asset_id=None, asset_type=None, key=None, value=None):
    if name:
        asset = get_okera_assets(asset_type="Table", name=name)
    elif asset_id:
        asset = get_okera_assets(asset_type="Table", asset_id=asset_id)
    print("set_tblproperties: ", asset)
    with ctx.connect(host = configs.get('host'), port = configs.get('port')) as conn:
        if asset_type == "Table":
            conn.execute_ddl("ALTER TABLE " + asset.name + " SET TBLPROPERTIES ('" + key + "' = '" + str(value) + "')")

def set_sync_time(asset_id, asset_type):
    set_tblproperties(asset_id=asset_id, asset_type=asset_type, key="last_collibra_sync_time", value=int(datetime.datetime.utcnow().timestamp()))


# gets assets and their children from Collibra

#
# USE ASSET ID INSTEAD OF NAME HERE!!! differentiate between Tables and columns ///// done but untested
def get_assets(asset_id=None, asset_name=None, asset_type=None, with_children=True):
    def find_children(name, child_type):
        child_param = {
            'name': name + ".",
            'nameMatchMode': "START",
            'domainId': domain_id,
            'communityId': community_id,
            'typeId': find_asset_type_id(child_type)
            }
        return json.loads(collibra_get(child_param, "assets", "get")).get('results')

    if asset_id and asset_type == "Table":
        #finds name of asset from asset id
        asset_name = collibra_get(None, "assets/" + asset_id, "get").get('name')
        parent_param = {
            'name': asset_name,
            'nameMatchMode': "EXACT",
            'domainId': domain_id,
            'communityId': community_id,
            'typeId': find_asset_type_id(asset_type)
            }
        parent = json.loads(collibra_get(parent_param, "assets", "get")).get('results')

        if with_children: return parent + find_children(asset_name, 'Column')
        else: return parent

    elif asset_name and asset_type == "Column" or asset_type == "Database":
        parent_param = {
            'name': asset_name,
            'nameMatchMode': "EXACT",
            'domainId': domain_id,
            'communityId': community_id,
            'typeId': find_asset_type_id(asset_type)
            }
        parent = json.loads(collibra_get(parent_param, "assets", "get")).get('results')
        
        if with_children: return parent + find_children(asset_name, 'Table') + find_children(asset_name, 'Column')
        else: return parent

    else:
        get_all_param = {
            'domainId': domain_id,
            'communityId': community_id,
            }
        return json.loads(collibra_get(get_all_param, "assets", "get")).get('results')

# sets the parameters for /assets Collibra call
def set_assets(asset):
    return ({
    'name': asset.name,
    'displayName': asset.displayName,
    'domainId': domain_id,
    'typeId': asset.asset_type_id,
    'statusId': find_status_id("Candidate"),
    'excludedFromAutoHyperlinking': "true"
    })

# sets the parameters for /attributes Collibra call
def set_attributes(asset):
    if asset.attributes:
        attribute_params = []
        def group_attributes(attr):
            attribute_params.append({
                    'assetId': asset.asset_id,
                    'typeId': find_attribute_id(attr),
                    'value': asset.attributes.get(attr)
                    })

        if asset.attributes.get('Description'): group_attributes('Description')
        if asset.attributes.get('Location'): group_attributes('Location')
        #if asset.attributes.get('Technical Data Type'): group_attributes('Technical Data Type')
        return attribute_params

# sets the parameters for /relations Collibra call
def set_relations(asset):
    print("set_relations: ", asset)
    if asset.relation:
        relation_info = find_relation_id(asset.asset_type, asset.relation.get('Type'))
        return {
            'sourceId': asset.asset_id if asset.asset_type == relation_info.get('head') else get_assets(None, asset.relation.get('Name'), asset.relation.get('Type'), False)[0].get('id'),
            'targetId': asset.asset_id if asset.asset_type == relation_info.get('tail') else get_assets(None, asset.relation.get('Name'), asset.relation.get('Type'), False)[0].get('id'),
            'typeId': relation_info.get('id')
        }

#group together tables and their columns and iterate over list and call update()
def update(asset_name=None, asset_type=None, asset_id=None):
    #pyokera_calls(asset_name, asset_type)
    collibra_assets = []
    import_params = []
    deleted_assets = []
    deleted_params = []
    relation_params = []
    mapping_params = []
    attr_params = []
    # TODO switch out name for id
    for ua in get_assets(asset_name=asset_name, asset_type=asset_type, with_children=False):
        asset = Asset(
            ua.get('name'),
            ua.get('type').get('name'),
            ua.get('type').get('id'),
            ua.get('id'),
            ua.get('displayName')
        )
        collibra_assets.append(asset)
        #print("Updating " + asset.name + "...")
        attributes = {}
        attribute_ids = {}
        attribute = json.loads(collibra_get({'assetId': asset.asset_id}, "attributes", "get")).get('results')
        # TODO change to asset id not asset name !!
        matched_attr = find_okera_info(asset_id=asset.asset_id, info="attributes")
        for attr in attribute:
            attributes.update({attr.get('type').get('name'): attr.get('value')})
            attribute_ids.update({attr.get('type').get('name'): attr.get('id')})
        asset.attributes = attributes
        asset.attribute_ids = attribute_ids
        update_attr = []
        import_attr = []
        delete_attr = []
        if asset.attributes and matched_attr:
            for key in asset.attributes:
                if key in matched_attr:
                    if matched_attr[key] != None and asset.attributes[key] != matched_attr[key]:
                        print("mismatch")
                        update_attr.append({'id': asset.attribute_ids[key], 'value': matched_attr[key]})
                    elif matched_attr[key] == None:
                        delete_attr.append(asset.attribute_ids[key])
                else:
                    #TODO create function here
                    for key in matched_attr:
                        if matched_attr[key] != None:
                            a = get_okera_assets(asset.name, asset.asset_type)
                            a.asset_id = asset.asset_id
                            import_attr.append(set_attributes(a)[0])
        elif matched_attr and not asset.attributes:
            for key in matched_attr:
                if matched_attr[key] != None:
                    for a in get_okera_assets(asset.name, asset.asset_type):
                        a.asset_id = asset.asset_id
                        import_attr.append(set_attributes(a)[0])

        if update_attr:
            collibra_get(update_attr, "attributes/bulk", "post", {'X-HTTP-Method-Override': "PATCH"})
            print("set_sync_time update attr")
            set_sync_time(asset.asset_id, asset.asset_type)
        if delete_attr:
            print("set_sync_time delete attr")
            collibra_get(delete_attr, "attributes/bulk", "delete")
            set_sync_time(asset.asset_id, asset.asset_type)
        if import_attr:
            print("set_sync_time import attr")
            collibra_get(import_attr, "attributes/bulk", "post")
            set_sync_time(asset.asset_id, asset.asset_type)

    #figure this out
    #for deleted_asset in [obj for obj in collibra_assets if obj not in get_okera_assets(None, asset_type, asset_id)]:
        #collibra_get(None, "assets/" + deleted_asset.asset_id, "delete")        
    
    # order incorrect!!! imports columns before tables, relations break    
    if get_okera_assets(asset_name, asset_type) not in collibra_assets:
        new_asset = get_okera_assets(asset_name, asset_type)
        import_param = set_assets(new_asset)
        import_response = json.loads(collibra_get(import_param, "assets", "post"))
        print("Importing " + new_asset.name + "...")
        if import_response.get('name') == new_asset.name:
            new_asset.asset_id = import_response.get('id')
            set_sync_time(new_asset.asset_id, new_asset.asset_type)
        if set_relations(new_asset):
            collibra_get(set_relations(new_asset), "relations", "post")
        if set_attributes(new_asset):
            for attr in set_attributes(new_asset):
                collibra_get(attr, "attributes", "post")
                attr_params.append(attr)
        set_sync_time(new_asset.asset_id, new_asset.asset_type)
        mapping_param = {
            "id": new_asset.asset_id,
            "externalSystemId": "okera",
            "externalEntityId": new_asset.name,
            "mappedResourceId": new_asset.asset_id,
            "lastSyncDate": 0,
            "syncAction": "ADD"
        }
        if new_asset.tags:
            tag_params = {'tagNames': new_asset.tags}
            collibra_get(tag_params, "assets/" + new_asset.asset_id + "/tags", "post")
        print("set_sync_time end of import assets")
        set_tblproperties(name=new_asset.name, asset_type=new_asset.asset_type, key="collibra_asset_id", value=new_asset.asset_id)

    # updates tags for assets that exist
    for c in collibra_assets:
        collibra_tags = []
        for tag in json.loads(collibra_get(None, "assets/" + c.asset_id + "/tags", "get")):
            collibra_tags.append(tag.get('name'))
        tag_params = {'tagNames': find_okera_info(asset_id=c.asset_id, info="tags")}
        if find_okera_info(c.asset_id, "tags") and not collibra_tags:
            collibra_get(tag_params, "assets/" + c.asset_id + "/tags", "post")
            print("set_sync_time tags1")
            set_sync_time(c.asset_id, c.asset_type)
        elif collibra_tags and find_okera_info(asset_id=c.asset_id, info="tags") and collections.Counter(collibra_tags) != collections.Counter(find_okera_info(asset_id=c.asset_id, info="tags")):
            collibra_get(tag_params, "assets/" + asset.asset_id + "/tags", "put")
            print("set_sync_time tags2")
            set_sync_time(c.asset_id, c.asset_type)
        elif collibra_tags and not find_okera_info(asset_id=c.asset_id, info="tags"):
            tag_params = {'tagNames': collibra_tags}
            collibra_get(tag_params, "assets/" + c.asset_id + "/tags", "delete")
            print("set_sync_time tags3")
            set_sync_time(c.asset_id, c.asset_type)

    # TODO use mappings to find correct asset id
# what happens if i change display name in collibra? will full name stay the same??
def update_all():
    pyokera_calls()
    for d in okera_dbs:
        print(d.name)
        update(d.name, "Database")
    for t in okera_tables:
        print(t.name)
        if t.asset_id:
            update(None, "Table", t.asset_id)
            for c in t.children:
                print(c.name)
                update(c.name, "Column")
        else:
            update(t.name, "Table")
            for c in t.children:
                print(c.name)
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


    #Databases don't need relations
    # Update by iterating over tables and updating columns as well