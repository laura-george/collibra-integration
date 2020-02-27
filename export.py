import json
import requests
from okera import context
from pymongo import MongoClient
from config import configs
import collections

client = MongoClient(port=27017)
db = client.collibra_ids

community = configs.get('community')
community_id = json.loads(requests.get(configs.get('collibra dgc') + "/rest/2.0/communities", params = {'name': community}, auth = (configs.get('collibra username'), configs.get('collibra password'))).content).get('results')[0].get('id')

# makes /tags/asset/{asset id} REST call
def get_tags(asset_id):
    data = json.loads(requests.get(configs.get('collibra dgc') + "/rest/2.0/tags/asset/" + asset_id, auth = (configs.get('collibra username'), configs.get('collibra password'))).content)
    if data:
        tags = []
        for t in data:
            tags.append(t.get('name'))
        return tags

def get_attributes(asset_id):
    params = {
        'typeId': "00000000-0000-0000-0000-000000003114",
        'assetId': asset_id
        }
    data = json.loads(requests.get(configs.get('collibra dgc') + "/rest/2.0/attributes/", params = params, auth = (configs.get('collibra username'), configs.get('collibra password'))).content)
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

# pyokera calls
ctx = context()
ctx.enable_token_auth(token_str=configs.get('token'))
with ctx.connect(host = configs.get('host'), port = configs.get('port')) as conn:
    databases = conn.list_databases()
    elements = []
    #conn.execute_ddl("CREATE EXTERNAL TABLE okera_sample.users_test (uid STRING COMMENT 'Unique user id', dob STRING COMMENT 'Formatted as DD-month-YY', gender STRING, ccn STRING COMMENT 'Sensitive data, should not be accessible without masking.') COMMENT 'Okera sample dataset.' STORED AS PARQUET LOCATION 'file:/opt/data/users'")
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
params = {
    'name': "marketing",
    'nameMatchMode': "ANYWHERE",
    'simulation': False,
    'communityId': community_id
    }
data = json.loads(requests.get(configs.get('collibra dgc') + "/rest/2.0/assets", params = params, auth = (configs.get('collibra username'), configs.get('collibra password'))).content)
for d in data.get('results'):
    update_elements.append({'name': d.get('name'), 'display name': d.get('displayName'), 'description': get_attributes(d.get('id')), 'type': d.get('type').get('name'), 'domain': d.get('domain').get('name'), 'status': d.get('status').get('name'), 'tags': get_tags(d.get('id'))})
def find_info(name, info):
    for ue in update_elements:
        if ue.get('name') == name:
            return ue.get(info)
            break

# makes assign_attribute() or unassign_attribute() call for either table or column
def tag_actions(action, db, name, type, tags):
    with ctx.connect(host = configs.get('host'), port = configs.get('port')) as conn:
        for tag in tags:
            nmspc_key = tag.split(".")
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

for element in elements:
    if element.get('database') == "marketing":
        # begin of table loop: iterates over tables compares tags and descriptions from collibra and okera
        # tags: if only okera tags exist -> unassign tags in okera, if only collibra tags exist -> assign tags in okera, if collibra and okera tags exist -> compare tags and change (unassign and assign) if the collibra tags are different to the okera tags
        for t in element.get('tables'):
            tab_name = t.db[0] + "." + t.name
            collibra_tab_tags = find_info(tab_name, "tags")
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
                collibra_col_tags = find_info(col_name, "tags")
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

# TODO get attributes in bulk and match them up to their asset using ID