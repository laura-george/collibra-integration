import json
from okera import context
from pymongo import MongoClient
from collections import defaultdict

client = MongoClient(port=27017)
db = client.collibra_ids

community = "Okera Collibra Integration Test"
dataset_domain = "Test Data Dictionary"
database_domain = "Test Technology Asset Domain"

# Okera REST calls
ctx = context()
ctx.enable_token_auth(token_str='AARyb290AARyb290igFvigtzW4oBb64X91sHJQ$$.BhJrik4EcQ4YMaU5fU6hj76L5dg$')
with ctx.connect(host='10.1.10.106', port=12050) as conn:
    databases = conn.list_databases()
    elements = []
    for database in databases:
        datasets = conn.list_datasets(database)
        for dataset in datasets:
            columns = [dataset.schema.cols]
    element = {"database": 'okera_sample', "datasets": datasets}
    elements.append(element)

# final list of all assets loaded into JSON file
json_gen = None

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

    # relation between data sets and data elements
    ds_to_de_info = find_relation_id("Data Set", "Data Element")
    
    # relation between data sets and databases, a database is a technology asset
    ds_to_db_info = find_relation_id("Data Set", "Technology Asset")
    
    relation_object = None

    for r in relations:
        # example: relation = 00000000-0000-0000-0000-000000007062:TARGET
        if (r.get('asset type') == "Data Set" and r.get('asset relation') == "Database") or r.get('asset type') == "Database":
            relation = ds_to_db_info.get('id') + define_relation_type(r.get('asset type'), ds_to_db_info.get('head'))
        elif (r.get('asset type') == "Data Set" and r.get('asset relation') == "Data Element") or r.get('asset type') == "Data Element":
            relation = ds_to_de_info.get('id') + define_relation_type(r.get('asset type'), ds_to_de_info.get('head'))
        
        if relation_object == None:
            relation_object = '"' + relation + '":' + ' [{"name": "' + r.get("name") + '", "domain": {"name": "' + r.get("domain") + '", "community": {"name": "' + community + '"}}}]'
        else:
            relation_object = relation_object + ', "' + relation + '":' + ' [{"name": "' + r.get("name") + '", "domain": {"name": "' + r.get("domain") + '", "community": {"name": "' + community + '"}}}]'
    return '{' + relation_object + '}'

assets = []

# combines an assets info and its relations into one object and adds it to json_gen
def create_asset(asset):
    asset_object = None
    for a in asset:
        if a.get('description') != None:
            attribute = '{"00000000-0000-0000-0000-000000003114": [{"value": "' + a.get('description') + '"}]}'
        else: attribute = ""

        if asset_object == None:
            asset_object = '{"resourceType": "Asset","attributes": ' + attribute + ',"identifier": {"name": "' + a.get('name') + '","domain": {"name": "' + a.get('domain') + '", "community": {"name": "' + community + '"}}},"displayName": "' + a.get('display name') + '","type": {"id": "' + a.get('type id') + '"},"status": {"name": "' + a.get('status') + '"},"relations": ' + a.get('relations') + '}'
        else:
            asset_object = asset_object + ', {"resourceType": "Asset","attributes": ' + attribute + ',"identifier": {"name": "' + a.get('name') + '","domain": {"name": "' + a.get('domain') + '", "community": {"name": "' + community + '"}}},"displayName": "' + a.get('display name') + '","type": {"id": "' + a.get('type id') + '"},"status": {"name": "' + a.get('status') + '"},"relations": ' + a.get('relations') + '}'
   
    assets.append(asset_object)
    #print(assets)

def create_all():
    create_database()
    create_dataset()
    create_data_element()
    final = ', '.join(assets)
    f1 = open('./integration.json', 'w+')
    f1.write('[' + final + ']')
    f1.close()
    #with open('integration.json', 'w', encoding='utf-8') as f:
        #json.dump(final, f, ensure_ascii=False, indent=4)


######### TO DO: REWRITE ORDER OF FILE GENERATION #########

# gathers column (data element) info from Okera, creates relation to its data set
def create_data_element():
    data_elements = []
    asset_info = find_asset_id("Data Element")
    for element in elements:
    ds = element.get('datasets')
    for d in ds:
            dataset_name = d.db[0] + "." + d.name
            for col in d.schema.cols:
                relations = []
                name = dataset_name + "." + col.name
                relations.append({"name": dataset_name, "domain": dataset_domain, "asset type": asset_info.get('name'), "asset relation": "Data Set"})
                data_elements.append({"description": "", "name": col.name, "domain": dataset_domain, "community": community, "display name": name, "type id": asset_info.get('id'), "status": "Candidate", "relations": create_relation(relations)})
    create_asset(data_elements)

# gathers data set info from Okera, creates relation to its database and columns
def create_dataset():
    datasets = []
    asset_info = find_asset_id("Data Set")
    for element in elements:
        ds = element.get('datasets')
        for d in ds:
            relations = []
            dataset_name = d.db[0] + "." + d.name
            relations.append({"name": d.db[0], "domain": database_domain, "asset type": asset_info.get('name'), "asset relation": "Database"})
            for col in d.schema.cols:
                name = dataset_name + "." + col.name
                relations.append({"name": name, "domain": dataset_domain, "asset type": asset_info.get('name'), "asset relation": "Data Element"})
            all_relations = create_relation(relations)
            datasets.append({"description": "", "name": d.name, "domain": dataset_domain, "community": community, "display name": dataset_name, "type id": asset_info.get('id'), "status": "Candidate", "relations": all_relations})

    create_asset(datasets)

# gathers database info from Okera, creates relation to its data sets
def create_database():
    databases = []
    asset_info = find_asset_id("Database")
    for element in elements:
        db = element.get('database')
        ds = element.get('datasets')
        relations = []
        #print("DATABASE:")
        #print(db)
        for d in ds:
            dataset_name = d.db[0] + "." + d.name
            relation = {"name": dataset_name, "domain": dataset_domain, "asset type": asset_info.get('name'), "asset relation": "Data Set"}
            relations.append(relation)
        if len(relations) != 0:
            all_relations = create_relation(relations)
        else:
            all_relations = '{}'
        databases.append({"description": "", "name": db, "domain": database_domain, "community": community, "display name": db, "type id": asset_info.get('id'), "status": "Candidate", "relations": all_relations})
    create_asset(databases)

# all functions are called here for now...
# create_all()
