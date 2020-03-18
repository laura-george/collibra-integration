import json
import requests
import yaml
import thriftpy
import sys
from okera import context
from config import configs
import collections

with open('config.yaml') as f:
    configs = yaml.load(f, Loader=yaml.FullLoader)['configs']

with open('resourceids.yaml') as f:
    resource_ids = yaml.load(f, Loader=yaml.FullLoader)

# escapes special characters
def escape(string, remove_whitespace=False):
    if remove_whitespace:
        string.replace(" ", "_")
        return(json.dumps(string)[1:-1])
    else: return(json.dumps(string)[1:-1])

# builds collibra request
def collibra_get(param_obj, call, method, header=None):
    if method == 'get':
        try: 
            data = getattr(requests, method)(
            configs['collibra_dgc'] + "/rest/2.0/" + call, 
            params=param_obj, 
            auth=(configs['collibra_username'], configs['collibra_password']))
            data.raise_for_status()
        except requests.exceptions.HTTPError as e: 
            print("Failed request: " + str(param_obj))
            print("Error: ", e)
            print("Response: " + str(data.content))
        return data.content
    else:
        try: 
            data = getattr(requests, method)(
            configs['collibra_dgc'] + "/rest/2.0/" + call,
            headers=header, 
            json=param_obj, 
            auth=(configs['collibra username'], configs['collibra password']))    
            data.raise_for_status()
        except requests.exceptions.RequestException as e:
            print("Failed request: " + str(param_obj))
            print("Error: ", e)
            print("Response: " + str(data.content))
        return data.content

community = configs['community']
community_id = json.loads(collibra_get({'name': community}, "communities", 'get')).get('results')[0].get('id')
domain = configs['domain']
domain_id = json.loads(collibra_get({'name': domain['name'], 'communityId': community_id}, "domains", 'get')).get('results')[0].get('id')
ctx = context()
ctx.enable_token_auth(token_str=configs.get('token'))

def get_resource_ids(search_in, name):
    for r in resource_ids[search_in]:
        if search_in == 'relations':
            if r['head'] == name:
                return r['id']
        else:
            if r['name'] == name:
                return r['id']

# makes /tags/asset/{asset id} REST call
def get_tags(asset_id):
    data = collibra_get(None, "tags/asset/" + asset_id, 'get')
    if data:
        tags = []
        for t in data:
            tags.append(t.get('name'))
        return tags

def get_attributes(asset_id, attr_type):
    type_id = get_resource_ids('attributes', attr_type)
    params = {
        'typeIds': [type_id],
        'assetId': asset_id
        }
    data = json.loads(collibra_get(params, "attributes", 'get', None))
    if data.get('results'):
        return data.get('results')[0].get('value')

# creates tags as namespace.key, adds them to list
def create_tags(attribute_values):
    attributes = []
    if attribute_values:
        for attribute in attribute_values:
            name = attribute.attribute.attribute_namespace + "." + attribute.attribute.key 
            attributes.append(name)
        return attributes

elements = []
# pyokera calls
def pyokera_calls(asset_name=None):
    try:
        conn = ctx.connect(host = configs['host'], port = configs['port'])
    except thriftpy.transport.TException as e:
        print("Could not connect to Okera!")
        print("Error: ", e)
        sys.exit(1)
    with conn:
        databases = conn.list_databases()
        if asset_name:
            tables = conn.list_datasets(asset_name)
            if tables:
                element = {'database': asset_name, 'tables': tables}
            else:
                element = {'database': asset_name}
            elements.append(element)
        else:
            for database in databases:
                tables = conn.list_datasets(database)
                if tables:
                    element = {'database': database, 'tables': tables}
                else:
                    element = {'database': database}
                elements.append(element) 

update_elements = []
type_ids = ["BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING", "VARCHAR", "CHAR", "BINARY", "TIMESTAMP_NANOS", "DECIMAL", "DATE", "RECORD", "ARRAY", "MAP"]
# gets assets and their tags from collibra
# asset_name is always name of a database
def collibra_calls(asset_name=None):
    def set_elements(asset_id, asset_name, asset_type):
        info_classif = get_attributes(asset_id, "Information Classification")
        update_elements.append({
            'name': asset_name, 
            'asset_id': asset_id, 
            'description': get_attributes(asset_id, "Description"),
            'info_classif': configs['info_classif_namespace'] + "." + info_classif if info_classif else None, 
            'type': asset_type, 
            #'tags': get_tags(d.get('id')),
            'mapped_okera_resource': json.loads(collibra_get(None, "mappings/externalSystem/okera/mappedResource/" + asset_id, 'get')).get('externalEntityId')
            })

    if asset_name:
        params = {
            'name': asset_name,
            'nameMatchMode': "EXACT",
            'simulation': False,
            'domainId': domain_id,
            'communityId': community_id,
            'typeId': get_resource_ids('assets', 'Database')
            }
    else:
        params = {
            'simulation': False,
            'domainId': domain_id,
            'communityId': community_id
            }
    database = json.loads(collibra_get(params, "assets", 'get'))['results'][0]
    set_elements(database['id'], database['name'], 'Database')
    table_params = {'relationTypeId': get_resource_ids('relations', 'Table'), 'targetId': database['id']}
    tables = json.loads(collibra_get(table_params, "relations", 'get'))['results']
    columns = []
    for t in tables:
        set_elements(t['source']['id'], t['source']['name'], 'Table')
        column_params = {'relationTypeId': get_resource_ids('relations', 'Column'), 'targetId': t['source']['id']}
        for c in json.loads(collibra_get(column_params, 'relations', 'get'))['results']:
            print(c)
            set_elements(c['source']['id'], c['source']['name'], 'Column')

def find_info(name, info):
    for ue in update_elements:
        if ue.get('mapped_okera_resource') and ue.get('mapped_okera_resource') == name:
            return ue.get(info)
            break
        elif ue['name'] and ue['name'] == name:
            return ue.get(info)
            break

# makes assign_attribute() or unassign_attribute() call for either table or column
def tag_actions(action, db, name, type, tags):
    def define_tags(tag):
        with ctx.connect(host = configs['host'], port = configs['port']) as conn:
            #for tag in tags:
            nmspc_key = tag.split(".")
            print('nsmpc_key: ', nmspc_key)
            if nmspc_key[0] not in conn.list_attribute_namespaces() or nmspc_key[1] not in conn.list_attributes(nmspc_key[0]):
                conn.create_attribute(nmspc_key[0], nmspc_key[1], True)
            if action == "assign":
                if type == "Column":
                    tab_col = name.split(".")
                    conn.assign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=tab_col[1], column=tab_col[2], if_not_exists=True)
                elif type == "Table":
                    conn.assign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=name, if_not_exists=True)
            elif action == "unassign":
                if type == "Column":
                    tab_col = name.split(".")
                    conn.unassign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=tab_col[1], column=tab_col[2], if_not_exists=True)
                elif type == "Table":
                    conn.unassign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=name, if_not_exists=True)
    if isinstance(tags, list):
        print('tags list: ', tags)
        for tag in tags:
            define_tags(tag)
    else: 
        print('one tag: ', tags)
        define_tags(tags)
        
# alters description for either table, view or column
def desc_actions(name, type, col_type, description):
    with ctx.connect(host = configs.get('host'), port = configs.get('port')) as conn:
        if type == "Column":
            tab_col = name.rsplit('.', 1)
            conn.execute_ddl("ALTER TABLE " + tab_col[0] + " CHANGE " + tab_col[1] + " " + tab_col[1] + " " + col_type + " COMMENT '" + description + "'")
        elif type == "Table":
            conn.execute_ddl("ALTER TABLE " + name + " SET TBLPROPERTIES ('comment' = '" + description + "')")
        elif type == "View":
            conn.execute_ddl("ALTER VIEW " + name + " SET TBLPROPERTIES ('comment' = '" + description + "')")

# diffs attributes from Collibra with attributes from Okera and makes necessary changes in Okera
def export(asset_name=None):
    pyokera_calls(asset_name)
    collibra_calls(asset_name)
    for element in elements:
        if element.get('database') == asset_name:
            # begin of table loop: iterates over tables compares tags and descriptions from collibra and okera
            # tags: if only okera tags exist -> unassign tags in okera, if only collibra tags exist -> assign tags in okera, if collibra and okera tags exist -> compare tags and change (unassign and assign) if the collibra tags are different to the okera tags
            for t in element.get('tables'):
                tab_name = t.db[0] + "." + t.name
                collibra_tab_tags = find_info(tab_name, "info_classif")
                okera_tab_tags = create_tags(t.attribute_values)
                if okera_tab_tags and collibra_tab_tags:
                    if collections.Counter(okera_tab_tags) != collections.Counter(collibra_tab_tags):
                        tag_actions("unassign", t.db[0], t.name, "Table", okera_tab_tags)
                        tag_actions("assign", t.db[0], t.name, "Table", collibra_tab_tags)
                elif collibra_tab_tags and not okera_tab_tags:
                    tag_actions("assign", t.db[0], t.name, "Table", collibra_tab_tags)
                elif okera_tab_tags and not collibra_tab_tags:
                    tag_actions("unassign", t.db[0], t.name, "Table", okera_tab_tags)
                collibra_tab_desc = find_info(tab_name, "description")
                okera_tab_desc = t.description
                type = "View" if t.primary_storage == "VIEW" else "Table"
                if okera_tab_desc and not collibra_tab_desc or collibra_tab_desc and not okera_tab_desc or (okera_tab_desc and collibra_tab_desc and okera_tab_desc != collibra_tab_desc):
                    desc_actions(tab_name, type, None, collibra_tab_desc)        
                # begin of column loop: same functionality as table loop
                for col in t.schema.cols:
                    col_name = tab_name + "." + col.name
                    collibra_col_tags = find_info(col_name, "info_classif")
                    okera_col_tags = create_tags(col.attribute_values)
                    if okera_col_tags and collibra_col_tags:
                        if collections.Counter(okera_col_tags) != collections.Counter(collibra_col_tags):
                            tag_actions("unassign", t.db[0], col_name, "Column", okera_col_tags)
                            tag_actions("assign", t.db[0], col_name, "Column", collibra_col_tags)
                    elif collibra_col_tags and not okera_col_tags:
                        tag_actions("assign", t.db[0], col_name, "Column", collibra_col_tags)
                    elif okera_col_tags and not collibra_col_tags:
                        tag_actions("unassign", t.db[0], col_name, "Column", okera_col_tags)
                    collibra_col_desc = find_info(col_name, "description")
                    okera_col_desc = col.comment
                    if okera_col_desc and not collibra_col_desc or collibra_col_desc and not okera_col_desc or (okera_col_desc and collibra_col_desc and okera_col_desc != collibra_col_desc):
                        desc_actions(col_name, "Column", type_ids[col.type.type_id], collibra_col_desc)

which_asset = input("Please enter the full name of the database you wish to export: ")
if which_asset:
    print("Exporting " + which_asset + "...")
    export(which_asset)
    print("Export complete!")
elif not which_asset:
    print("All databases will now be exported...")
    export()
    print("Export complete!")