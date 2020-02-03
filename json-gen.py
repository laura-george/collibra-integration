import json
import requests
from okera import context
from pymongo import MongoClient
from config import configs

client = MongoClient(port=27017)
db = client.collibra_ids

community = configs.get('community')
community_id = json.loads(requests.get('https://okera.collibra.com:443/rest/2.0/communities', params = {"name": community}, auth = (configs.get('collibra username'), configs.get('collibra password'))).content).get('results')[0].get('id')
data_dict_domain = configs.get('data_dict_domain')
tech_asset_domain = configs.get('tech_asset_domain')
domain_info = [data_dict_domain, tech_asset_domain]
assets = []
domains = []

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
            element = {"database": database, "tables": tables}
        else:
            element = {"database": database}
        elements.append(element)

# takes domain name (set in config.py) and retrieves its domain id
def get_ids(name):
    params = {
    "name": name,
    "communityId": community_id}
    domain_id = json.loads(requests.get('https://okera.collibra.com:443/rest/2.0/domains', params = params, auth = (configs.get('collibra username'), configs.get('collibra password'))).content)
    return domain_id.get('results')[0].get('id')

# makes /assets REST call
def get_assets(name, domain_id):
    params = {
        "name": name,
        "nameMatchMode" : "EXACT", 
        "simulation": False,
        "domainId": domain_id,
        "communityId": community_id
        }
    data =json.loads(requests.get('https://okera.collibra.com:443/rest/2.0/assets', params = params, auth = (configs.get('collibra username'), configs.get('collibra password'))).content)
    return data.get('results')[0]

# makes /tags/asset/{asset id} REST call
def get_tags(asset_id):
    data = json.loads(requests.get('https://okera.collibra.com:443/rest/2.0/tags/asset/' + asset_id, auth = (configs.get('collibra username'), configs.get('collibra password'))).content)
    if data:
        tags = []
        for t in data:
            tags.append(t.get('name'))
        return tags

def get_attributes(asset_id):
    params = {
        "typeId": "00000000-0000-0000-0000-000000003114",
        "assetId": asset_id
        }
    data = json.loads(requests.get('https://okera.collibra.com:443/rest/2.0/attributes/', params = params, auth = (configs.get('collibra username'), configs.get('collibra password'))).content)
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

# finds asset id and name in mongodb collection
def find_asset_id(asset_name):
    for x in db.asset_ids.find({"name": asset_name}):
        return {"name": x.get('name'), "id": x.get('id')}

# finds relation id and head in mongodb collection
def find_relation_id(head, tail):
    for x in db.relation_ids.find({"head": head, "tail": tail}):
        return {"head": x.get('head'), "id": x.get('id')}

# creates relation object for relations between data sets and databases, and data sets and data elements (and vice versa)
def create_relation(relations):
    # defines the type i.e. the direction of the relation
    def define_relation_type(asset_type, head):
        if asset_type == "Database":
            asset_type = "Technology Asset"
        if asset_type == head:
            return ":TARGET"
        else:
            return ":SOURCE"

    # relation between table and column
    tab_to_col_info = find_relation_id("Column", "Table")
    
    # relation between table and schema, a database is a technology asset
    tab_to_sche_info = find_relation_id("Schema", "Table")

     # relation between database and schema, a database is a technology asset
    db_to_sche_info = find_relation_id("Technology Asset", "Schema")
    
    relation_object = None
    for r in relations:
        # example: relation = 00000000-0000-0000-0000-000000007062:TARGET
        if (r.get('asset type') == "Schema" and r.get('asset relation') == "Database") or r.get('asset type') == "Database":
            relation = db_to_sche_info.get('id') + define_relation_type(r.get('asset type'), db_to_sche_info.get('head'))
        elif (r.get('asset type') == "Table" and r.get('asset relation') == "Schema") or r.get('asset type') == "Schema":
            relation = tab_to_sche_info.get('id') + define_relation_type(r.get('asset type'), tab_to_sche_info.get('head'))
        elif (r.get('asset type') == "Column" and r.get('asset relation') == "Table") or r.get('asset type') == "Table":
            relation = tab_to_col_info.get('id') + define_relation_type(r.get('asset type'), tab_to_col_info.get('head'))
        
        if relation_object == None:
            relation_object = '"' + relation + '":' + ' [{"name": "' + r.get("name") + '", "domain": {"name": "' + r.get("domain") + '", "community": {"name": "' + community + '"}}}]'
        else:
            relation_object = relation_object + ', "' + relation + '":' + ' [{"name": "' + r.get("name") + '", "domain": {"name": "' + r.get("domain") + '", "community": {"name": "' + community + '"}}}]'
    return '{' + relation_object + '}'

# combines a domains info into one object and adds it to domains list
def create_domain():
    domain_object = None
    for d in domain_info:
        if domain_object == None:
            domain_object = '{"resourceType": "Domain","identifier": {"name": "' + d.get('name') + '","community": {"name": "' + community + '"}}, "type": {"name": "' + d.get('type') + '"}}'
        else:
            domain_object = domain_object + ', {"resourceType": "Domain","identifier": {"name": "' + d.get('name') + '","community": {"name": "' + community + '"}}, "type": {"name": "' + d.get('type') + '"}}'
    domains.append(domain_object)

# combines an assets info and its relations into one object and adds it to assets list
def create_asset(asset):
    asset_object = None
    for a in asset:
        if a.get('description'):
            attribute = '"attributes": {"00000000-0000-0000-0000-000000003114": [{"value": "' + a.get('description') + '"}]},' 
        else: attribute = ""

        if a.get('tags'):
            tag_object = None
            for tag in a.get('tags'):
                if tag_object == None:
                    tag_object = '"'+ tag + '"'
                else:
                    tag_object = tag_object + ',"' + tag + '"'
            tags = ',"tags": [' + tag_object +']'
        else: tags = ""

        if asset_object == None:
            asset_object = '{"resourceType": "Asset",' + attribute + '"identifier": {"name": "' + a.get('name') + '","domain": {"name": "' + a.get('domain') + '","community": {"name": "' + community + '"}}},"displayName": "' + a.get('display name') + '","type": {"id": "' + a.get('type id') + '"},"status": {"name": "' + a.get('status') + '"},"relations": ' + a.get('relations') + tags + '}'
        else:
            asset_object = asset_object + ', {"resourceType": "Asset",' + attribute + '"identifier": {"name": "' + a.get('name') + '","domain": {"name": "' + a.get('domain') + '","community": {"name": "' + community + '"}}},"displayName": "' + a.get('display name') + '","type": {"id": "' + a.get('type id') + '"},"status": {"name": "' + a.get('status') + '"},"relations": ' + a.get('relations') + tags + '}'
    assets.append(asset_object)

# gathers table and column info from Okera, creates relations
# relations are created for table -> schema and column -> table
def create_data(element):
    tables = []
    columns = []
    tab_info = find_asset_id("Table")
    col_info = find_asset_id("Column")
    for t in element.get('tables'):
        tab_name = t.db[0] + "." + t.name
        tables.append({"description": t.description if t.description else "", "name": tab_name, "domain": data_dict_domain.get('name'), "community": community, "display name": t.name, "type id": tab_info.get('id'), "status": "Candidate", "relations": create_relation([{"name": "schema." + t.db[0], "domain": tech_asset_domain.get('name'), "asset type": tab_info.get('name'), "asset relation": "Schema"}]), "tags": create_tags(t.attribute_values)})
        for col in t.schema.cols:
            name = tab_name + "." + col.name
            columns.append({"description": col.comment if col.comment else "", "name": name, "domain": data_dict_domain.get('name'), "community": community, "display name": col.name, "type id": col_info.get('id'), "status": "Candidate", "relations": create_relation([{"name": tab_name, "domain": data_dict_domain.get('name'), "asset type": col_info.get('name'), "asset relation": "Table"}]), "tags": create_tags(col.attribute_values)})
    create_asset(tables + columns)

# gathers database info from Okera, creates databases and schemas
# a schema is created for each database, relations are created for database -> schema
def create_database(element):
    schemas = []
    databases = []
    db_info = find_asset_id("Database")
    schema_info = find_asset_id("Schema")
    db = element.get('database')
    schema_name = "schema." + db
    databases.append({"description": "", "name": db, "domain": tech_asset_domain.get('name'), "community": community, "display name": db, "type id": db_info.get('id'), "status": "Candidate", "relations": create_relation([{"name": schema_name, "domain": tech_asset_domain.get('name'), "asset type": db_info.get('name'), "asset relation": "Schema"}])})
    databases.append({"description": "", "name": schema_name, "domain": tech_asset_domain.get('name'), "community": community, "display name": schema_name, "type id": schema_info.get('id'), "status": "Candidate", "relations": create_relation([{"name": db, "domain": tech_asset_domain.get('name'), "asset type": schema_info.get('name'), "asset relation": "Database"}])})
    create_asset(databases)

# all functions are called here for now...
def create_all():
    create_domain()
    for element in elements:
        create_database(element)
        if element.get('tables'):
            create_data(element)
    final = ', '.join(domains + assets)
    integration = open('./integration.json', 'w+')
    integration.write('[' + final + ']')
    integration.close()

#create_all()

# gets assets and their tags from collibra
def update_assets():
    update_elements = []
    grouped_objects = []

    params = {
        "simulation": False,
        "communityId": community_id
        }
    data = json.loads(requests.get('https://okera.collibra.com:443/rest/2.0/assets', params = params, auth = (configs.get('collibra username'), configs.get('collibra password'))).content)
    for d in data.get('results'):
        update_elements.append({"name": d.get('name'), "display name": d.get('displayName'), "description": get_attributes(d.get('id')), "type": d.get('type').get('name'), "domain": d.get('domain').get('name'), "status": d.get('status').get('name'), "tags": get_tags(d.get('id'))})

    def find_info(name, info):
        for ue in update_elements:
            if ue.get('name') == name:
                return ue.get(info)
                break

    for element in elements:
        if(element.get('tables')):
            for t in element.get('tables'):
                if t.description != find_info(t.db[0] + "." + t.name, "description"):
                    print("------description changed!------")
                    print(t.description)                        
                    #with ctx.connect(host = configs.get('host'), port = configs.get('port')) as conn:
                        #conn.execute_ddl()
                for col in t.schema.cols:
                    if col.comment != find_info(t.db[0] + "." + t.name + "." + col.name, "description"):
                        print("------description changed!------")
                        #with ctx.connect(host = configs.get('host'), port = configs.get('port')) as conn:
                        #conn.execute_ddl()

    #print(conn.execute_ddl("ALTER TABLE okera_sample.users ADD COLUMNS (uid STRING COMMENT 'Unique user id')"))
        
#update_assets()
