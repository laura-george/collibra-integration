import json
from okera import context
from pymongo import MongoClient
from collections import defaultdict

client = MongoClient(port=27017)
db = client.collibra_ids

community = "Okera2.0"
dataset_domain = {"name": "Okera2.0 Data Dictionary", "type": "Physical Data Dictionary"}
database_domain = {"name": "Okera2.0 Technology Assets", "type": "Technology Asset Domain"}
domain_info = [dataset_domain, database_domain]
assets = []
domains = []

# Okera REST calls
ctx = context()
ctx.enable_token_auth(token_str='AARyb290AARyb290igFvx9X7p4oBb-vif6cKPA$$.JJiku3dvzDOMoq5OcUsFJtR15Rc$')
with ctx.connect(host='10.1.10.106', port=12050) as conn:
    databases = conn.list_databases()
    elements = []
    for database in databases:
        datasets = conn.list_datasets(database)
        if datasets:
            element = {"database": database, "datasets": datasets}
        else:
            element = {"database": database}
        elements.append(element)

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
        elif (r.get('asset type') == "Table" and r.get('asset relation') == "Column") or r.get('asset type') == "Column":
            relation = tab_to_col_info.get('id') + define_relation_type(r.get('asset type'), tab_to_col_info.get('head'))
        
        if relation_object == None:
            relation_object = '"' + relation + '":' + ' [{"name": "' + r.get("name") + '", "domain": {"name": "' + r.get("domain") + '", "community": {"name": "' + community + '"}}}]'
        else:
            relation_object = relation_object + ', "' + relation + '":' + ' [{"name": "' + r.get("name") + '", "domain": {"name": "' + r.get("domain") + '", "community": {"name": "' + community + '"}}}]'
    print(relation_object)
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

        if asset_object == None:
            asset_object = '{"resourceType": "Asset",' + attribute + '"identifier": {"name": "' + a.get('name') + '","domain": {"name": "' + a.get('domain') + '","community": {"name": "' + community + '"}}},"displayName": "' + a.get('display name') + '","type": {"id": "' + a.get('type id') + '"},"status": {"name": "' + a.get('status') + '"},"relations": ' + a.get('relations') + ', "tags": ["test"]}'
        else:
            asset_object = asset_object + ', {"resourceType": "Asset",' + attribute + '"identifier": {"name": "' + a.get('name') + '","domain": {"name": "' + a.get('domain') + '","community": {"name": "' + community + '"}}},"displayName": "' + a.get('display name') + '","type": {"id": "' + a.get('type id') + '"},"status": {"name": "' + a.get('status') + '"},"relations": ' + a.get('relations') + ', "tags": ["test"]}'
    print(asset_object)
    assets.append(asset_object)

# gathers data set and column info from Okera, creates relation to its database and columns
def create_data(element):
    datasets = []
    data_elements = []
    ds_info = find_asset_id("Table")
    de_info = find_asset_id("Column")
    ds = element.get('datasets')
    for d in ds:
        ds_relations = []
        dataset_name = d.db[0] + "." + d.name
        ds_relations.append({"name": d.db[0] + ".schema", "domain": database_domain.get('name'), "asset type": ds_info.get('name'), "asset relation": "Schema"})
        for col in d.schema.cols:
            name = dataset_name + "." + col.name
            ds_relations.append({"name": name, "domain": dataset_domain.get('name'), "asset type": ds_info.get('name'), "asset relation": "Column"})
            de_relations = []
            de_relations.append({"name": dataset_name, "domain": dataset_domain.get('name'), "asset type": de_info.get('name'), "asset relation": "Table"})
            data_elements.append({"description": "", "name": name, "domain": dataset_domain.get('name'), "community": community, "display name": col.name, "type id": de_info.get('id'), "status": "Candidate", "relations": create_relation(de_relations)})

        all_relations = create_relation(ds_relations)
        datasets.append({"description": "", "name": dataset_name, "domain": dataset_domain.get('name'), "community": community, "display name": d.name, "type id": ds_info.get('id'), "status": "Candidate", "relations": all_relations})
    create_asset(datasets + data_elements)

# gathers database info from Okera, creates relation to its data sets
def create_database(element):
    schemas = []
    databases = []
    db_info = find_asset_id("Database")
    schema_info = find_asset_id("Schema")
    db = element.get('database')
    db_relations = []
    schema_relations = []
    schema_name = db + ".schema"
    schema = {"description": "", "name": schema_name, "domain": database_domain.get('name'), "community": community, "display name": schema_name, "type id": schema_info.get('id'), "status": "Candidate", "relations": create_relation([{"name": db, "domain": dataset_domain.get('name'), "asset type": schema_info.get('name'), "asset relation": "Database"}])}
    schema_relations.append({"name": db, "domain": dataset_domain.get('name'), "asset type": schema_info.get('name'), "asset relation": "Database"})
    if element.get('datasets'): 
        ds = element.get('datasets')
        for d in ds:
            dataset_name = d.db[0] + "." + d.name
            schema_relations.append({"name": dataset_name, "domain": dataset_domain.get('name'), "asset type": schema_info.get('name'), "asset relation": "Table"})
    else:
        all_relations = '{}'
    
    all_relations = create_relation(schema_relations)
    databases.append({"description": "", "name": db, "domain": database_domain.get('name'), "community": community, "display name": db, "type id": db_info.get('id'), "status": "Candidate", "relations": create_relation([{"name": schema_name, "domain": dataset_domain.get('name'), "asset type": db_info.get('name'), "asset relation": "Schema"}])})
    databases.append({"description": "", "name": schema_name, "domain": database_domain.get('name'), "community": community, "display name": schema_name, "type id": schema_info.get('id'), "status": "Candidate", "relations": all_relations})
    print(databases)
    create_asset(databases)

#def create_tags():


# all functions are called here for now...
def create_all():
    create_domain()
    for element in elements:
        print(element.get('database'))
        create_database(element)
        if element.get('datasets'):
            create_data(element)
    final = ', '.join(domains + assets)
    integration = open('./integration.json', 'w+')
    integration.write('[' + final + ']')
    integration.close()

create_all()
