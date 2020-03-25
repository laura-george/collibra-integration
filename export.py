# TODO name entered in script should be for collibra table not okera table!!!

import json
import requests
import yaml
import thriftpy
import sys
from okera import context
import collections

# Okera planner port
planner_port = 12050
# list of elements retrieved from Collibra
update_elements = []
# list of elements retrieved from Okera
elements = []
# Okera column type IDs
type_ids = ["BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "STRING", "VARCHAR", "CHAR", "BINARY", "TIMESTAMP_NANOS", "DECIMAL", "DATE", "RECORD", "ARRAY", "MAP"]

# opens config.yaml
with open('config_export.yaml') as f:
    configs = yaml.load(f, Loader=yaml.FullLoader)['configs']

# opens resourceids.yaml
with open('resourceids.yaml') as f:
    resource_ids = yaml.load(f, Loader=yaml.FullLoader)

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
            data = getattr(requests, method)(
            configs['collibra_dgc'] + "/rest/2.0/" + call, 
            params=param_obj, 
            auth=(configs['collibra_username'], configs['collibra_password']))
            data.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if json.loads(data.content).get('errorCode') != "mappingNotFound":
                print("COLLIBRA CORE API ERROR") 
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
            print("COLLIBRA CORE API ERROR")
            print("Failed request: " + str(param_obj))
            print("Error: ", e)
            print("Response: " + str(data.content))
        return data.content

# names and IDs of Collibra community and domain
community = configs['community']
community_id = json.loads(collibra_get({'name': community}, "communities", 'get')).get('results')[0].get('id')
domain = configs['domain']
domain_id = json.loads(collibra_get({'name': domain['name'], 'communityId': community_id}, "domains", 'get')).get('results')[0].get('id')

# PyOkera context
ctx = context()
ctx.enable_token_auth(token_str=configs['token'])

# returns resource ID in resourceids.yaml
def get_resource_ids(search_in, name):
    for r in resource_ids[search_in]:
        if search_in == 'relations':
            if r['head'] == name:
                return r['id']
        else:
            if r['name'] == name:
                return r['id']

# makes /tags/asset/{asset id} REST call and returns tags in list
def get_tags(asset_id):
    data = collibra_get(None, "tags/asset/" + asset_id, 'get')
    if data:
        tags = []
        for t in data:
            tags.append(t.get('name'))
        return tags

# makes /attributes REST call and returns attribute
def get_attributes(asset_id, attr_type):
    type_id = get_resource_ids('attributes', attr_type)
    params = {
        'typeIds': [type_id],
        'assetId': asset_id
        }
    data = json.loads(collibra_get(params, "attributes", 'get', None))
    if data.get('results'):
        return data.get('results')[0].get('value')

# creates tags as namespace.key and returns as list
def create_tags(attribute_values):
    attributes = []
    if attribute_values:
        for attribute in attribute_values:
            name = attribute.attribute.attribute_namespace + "." + attribute.attribute.key 
            attributes.append(name)
        return attributes

# pyokera calls
def pyokera_calls(asset_name=None, asset_type=None):
    try:
        conn = ctx.connect(host = configs['host'], port = planner_port)
    except thriftpy.transport.TException as e:
        print("PYOKERA ERROR")
        print("Could not connect to Okera!")
        print("Error: ", e)
        sys.exit(1)
    with conn:
        databases = conn.list_databases()
        if asset_name:
            if asset_type == "Database":
                tables = conn.list_datasets(asset_name)
                if tables:
                    element = {'database': asset_name, 'tables': tables}
                else:
                    element = {'database': asset_name}
                elements.append(element)
            elif asset_type == "Table":
                db_name = asset_name.split(".")[0]
                tables = conn.list_datasets(db_name)
                for t in tables:
                    # build in asset id
                    if t.name == asset_name:
                        element = {'database': db_name, 'tables': tables}
                        break
        else:
            for database in databases:
                tables = conn.list_datasets(database)
                if tables:
                    element = {'database': database, 'tables': tables}
                else:
                    element = {'database': database}
                elements.append(element) 

# gets assets and their tags and attributes from Collibra
def collibra_calls(asset_name=None, asset_type=None):
    def set_elements(asset_id, asset_name, asset_type):
        info_classif = escape(get_attributes(asset_id, "Information Classification"), True)
        update_elements.append({
            'name': asset_name, 
            'asset_id': asset_id, 
            'description': escape(get_attributes(asset_id, "Description")),
            'info_classif': configs['mapped_attribute_okera_namespace'] + "." + info_classif if info_classif else None, 
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
            'typeId': get_resource_ids('assets', asset_type)
            }
    else:
        params = {
            'simulation': False,
            'domainId': domain_id,
            'communityId': community_id
            }
    
    if asset_type == "Database":
        database = json.loads(collibra_get(params, "assets", 'get'))['results'][0]
        set_elements(database['id'], database['name'], asset_type)
        table_params = {'relationTypeId': get_resource_ids('relations', 'Table'), 'targetId': database['id']}
        tables = json.loads(collibra_get(table_params, "relations", 'get'))['results']
        columns = []
        for t in tables:
            set_elements(t['source']['id'], t['source']['name'], 'Table')
            column_params = {'relationTypeId': get_resource_ids('relations', 'Column'), 'targetId': t['source']['id']}
            for c in json.loads(collibra_get(column_params, 'relations', 'get'))['results']:
                set_elements(c['source']['id'], c['source']['name'], 'Column')
    elif asset_type == "Table":
        print("table: ", json.loads(collibra_get(params, "assets", "get"))['results'])
        table = json.loads(collibra_get(params, "assets", "get"))['results'][0]
        set_elements(table['id'], table['name'], asset_type)
        column_params = {'relationTypeId': get_resource_ids('relations', 'Column'), 'targetId': table['id']}
        for c in json.loads(collibra_get(column_params, 'relations', 'get'))['results']:
            set_elements(c['source']['id'], c['source']['name'], 'Column')

# returns Collibra asset information based on the mapped Okera resource name
def find_info(name=None, info=None, asset_id=None):
    for ue in update_elements:
        if name:
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
    def define_tags(tag):
        try:
            conn = ctx.connect(host = configs['host'], port = planner_port)
        except thriftpy.transport.TException as e:
            print("PYOKERA ERROR")
            print("Could not connect to Okera!")
            print("Error: ", e)
        with conn:
            nmspc_key = tag.split(".")
            try:
                list_namespaces = conn.list_attribute_namespaces()
            except thriftpy.thrift.TException as e:
                print("PYOKERA ERROR")
                print("Could not list attribute namespaces!")
                print("Error: ", e)
            try:
                list_attributes = conn.list_attributes(nmspc_key[0])
            except thriftpy.thrift.TException as e:
                print("PYOKERA ERROR")
                print("Could not list attributes for namespace " + nmspc_key[0] + "!")
                print("Error: ", e)
            if nmspc_key[0] not in list_namespaces or nmspc_key[1] not in list_attributes:
                try:
                    conn.create_attribute(nmspc_key[0], nmspc_key[1], True)
                except thriftpy.thrift.TException as e:
                    print("PYOKERA ERROR")
                    print("Could not create tag " + tag + "!")
                    print("Error: ", e)
            if action == "assign":
                if type == "Column":
                    tab_col = name.split(".")
                    try:
                        conn.assign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=tab_col[1], column=tab_col[2], if_not_exists=True)
                    except thriftpy.thrift.TException as e:
                        print("PYOKERA ERROR")
                        print("Could not assign tag " + tag + " to column " + name + "!")
                        print("Error: ", e)
                elif type == "Table":
                    try:
                        conn.assign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=name, if_not_exists=True)
                    except thriftpy.thrift.TException as e:
                        print("PYOKERA ERROR")
                        print("Could not assign tag " + tag + " to table " + name + "!")
                        print("Error: ", e)
            elif action == "unassign":
                if type == "Column":
                    tab_col = name.split(".")
                    try:
                        conn.unassign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=tab_col[1], column=tab_col[2], if_not_exists=True)
                    except thriftpy.thrift.TException as e:
                        print("PYOKERA ERROR")
                        print("Could not unassign tag " + tag + " from column " + name + "!")
                        print("Error: ", e)
                elif type == "Table":
                    try:
                        conn.unassign_attribute(nmspc_key[0], nmspc_key[1], db, dataset=name, if_not_exists=True)
                    except thriftpy.thrift.TException as e:
                        print("PYOKERA ERROR")
                        print("Could not unassign tag " + tag + " from table " + name + "!")
                        print("Error: ", e)
    if isinstance(tags, list):
        for tag in tags:
            define_tags(tag)
    else: 
        define_tags(tags)
        
# alters description for either table, view or column
def desc_actions(name, type, col_type, description, tab_type=None):
    description = '' if not description else description
    try:
        conn = ctx.connect(host = configs['host'], port = planner_port)
    except thriftpy.transport.TException as e:
        print("PYOKERA ERROR")
        print("Could not connect to Okera!")
        print("Error: ", e)
        sys.exit(1)
    with conn:
        if type == "Column" and tab_type == "Table":
            tab_col = name.rsplit('.', 1)
            try:
                conn.execute_ddl("ALTER TABLE " + tab_col[0] + " CHANGE " + tab_col[1] + " " + tab_col[1] + " " + col_type + " COMMENT '" + description + "'")
            except thriftpy.thrift.TException as e:
                print("PYOKERA ERROR")
                print("Could not change description for column " + name + "!")
                print("Error: ", e)
        elif type == "Column" and tab_type == "View":
            print("PYOKERA ERROR")
            print("Could not change description for column in view " + name + "!")
        elif type == "Table":
            try:
                conn.execute_ddl("ALTER TABLE " + name + " SET TBLPROPERTIES ('comment' = '" + description + "')")
            except thriftpy.thrift.TException as e:
                print("PYOKERA ERROR")
                print("Could not change description for table " + name + "!")
                print("Error: ", e)
        elif type == "View":
            try:
                conn.execute_ddl("ALTER VIEW " + name + " SET TBLPROPERTIES ('comment' = '" + description + "')")
            except thriftpy.thrift.TException as e:
                print("PYOKERA ERROR")
                print("Could not change description for view " + name + "!")
                print("Error: ", e)

# diffs attributes from Collibra with attributes from Okera and makes necessary changes in Okera
def export(asset_name=None, asset_type=None):
    #collibra calls first to get name of collibra table or database
    collibra_calls(asset_name, asset_type)
    pyokera_calls(asset_name, asset_type)
    def diff(t):
        asset_id = t.metadata.get('collibra_asset_id')
        tab_name = t.db[0] + "." + t.name
        collibra_tab_tags = find_info(info="info_classif", asset_id=asset_id) if asset_id else find_info(name=tab_name, info="info_classif")
        okera_tab_tags = create_tags(t.attribute_values)
        if okera_tab_tags and collibra_tab_tags:
            if collections.Counter(okera_tab_tags) != collections.Counter(collibra_tab_tags):
                tag_actions("unassign", t.db[0], t.name, "Table", okera_tab_tags)
                tag_actions("assign", t.db[0], t.name, "Table", collibra_tab_tags)
        elif collibra_tab_tags and not okera_tab_tags:
            tag_actions("assign", t.db[0], t.name, "Table", collibra_tab_tags)
        elif okera_tab_tags and not collibra_tab_tags:
            tag_actions("unassign", t.db[0], t.name, "Table", okera_tab_tags)
        collibra_tab_desc = find_info(info="description", asset_id=asset_id) if asset_id else find_info(name=tab_name, info="description")
        okera_tab_desc = t.description
        tab_type = "View" if t.primary_storage == "View" else "Table"
        if okera_tab_desc and not collibra_tab_desc or collibra_tab_desc and not okera_tab_desc or (okera_tab_desc and collibra_tab_desc and okera_tab_desc != collibra_tab_desc):
            desc_actions(tab_name, tab_type, None, collibra_tab_desc)        
        # begin of column loop: same functionality as table loop
        for col in t.schema.cols:
            col_name = tab_name + "." + col.name
            # switch out for okera
            print("collibra col name: ", find_info(asset_id=asset_id, info="name"))
            print("okera col name: ", col_name)
            collibra_col_name = find_info(asset_id=asset_id, info="name") + "." + col.name if find_info(asset_id=asset_id, info="name") else col_name
            collibra_col_tags = find_info(collibra_col_name, "info_classif")
            okera_col_tags = create_tags(col.attribute_values)
            if okera_col_tags and collibra_col_tags:
                if collections.Counter(okera_col_tags) != collections.Counter(collibra_col_tags):
                    tag_actions("unassign", t.db[0], col_name, "Column", okera_col_tags)
                    tag_actions("assign", t.db[0], col_name, "Column", collibra_col_tags)
            elif collibra_col_tags and not okera_col_tags:
                tag_actions("assign", t.db[0], col_name, "Column", collibra_col_tags)
            elif okera_col_tags and not collibra_col_tags:
                tag_actions("unassign", t.db[0], col_name, "Column", okera_col_tags)
            collibra_col_desc = find_info(collibra_col_name, "description")
            okera_col_desc = col.comment
            if okera_col_desc and not collibra_col_desc or collibra_col_desc and not okera_col_desc or (okera_col_desc and collibra_col_desc and okera_col_desc != collibra_col_desc):
                desc_actions(col_name, "Column", type_ids[col.type.type_id], collibra_col_desc, tab_type)

    for element in elements:
        if asset_type == "Database" and element.get('database') == asset_name:
            for t in element.get('tables'):
                diff(t)
        elif asset_type == "Table":
            for t in element.get('tables'):
                if t.name == asset_name: diff(t)

which_asset = input("Please enter the full name the asset you wish to update: ")
which_type = input("Is this asset of the type Database or Table? ")
if which_asset and which_type:
    print("Exporting " + which_asset + "...")
    export(which_asset, which_type)
    print("Export complete!")
elif which_asset and not which_type:
    while not which_type:
        which_type = input("The asset type is required: ")
    export(which_asset, which_type)
    print("Exporting " + which_asset + "...")
    print("Export complete!")