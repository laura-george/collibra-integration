import json
from okera import context
from pymongo import MongoClient

client = MongoClient(port=27017)
db = client.collibra_ids

community = "Okera Collibra Integration Test"
dataset_domain = "Test Data Dictionary"
database_domain = "Test Technology Asset Domain"

# Okera REST calls
ctx = context()
ctx.enable_token_auth(token_str='AARyb290AARyb290igFvf7hcmooBb6PE4JoFIA$$.febOIKgyU461wWW_KAF2IP_waSA$')
with ctx.connect(host='10.1.10.106', port=12050) as conn:
    databases = conn.list_databases()
    elements = []
    for database in databases:
        datasets = conn.list_datasets(database)
        for dataset in datasets:
            columns = [dataset.schema.cols]
        element = {"database": database, "datasets": datasets}
        elements.append(element)

# final list of all assets loaded into JSON file
json_gen = []

# finds asset id and name in mongodb collection
def find_asset_id(asset_name):
    for x in db.asset_ids.find({"name": asset_name}):
        return {"name": x.get('name'), "id": x.get('id')}

# finds relation id and head in mongodb collection
def find_relation_id(head, tail):
    for x in db.relation_ids.find({"head": head, "tail": tail}):
        return {"head": x.get('head'), "id": x.get('id')}

# creates relation object for relations between data sets and databases, and data sets and data elements (and vice versa)
def create_relation(name, domain, asset_type, asset_relation):
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

    # example: relation = 00000000-0000-0000-0000-000000007062:TARGET
    if (asset_type == "Data Set" and asset_relation == "Database") or asset_type == "Database":
        relation = ds_to_db_info.get('id') + define_relation_type(asset_type, ds_to_db_info.get('head'))
    elif (asset_type == "Data Set" and asset_relation == "Data Element") or asset_type == "Data Element":
        relation = ds_to_de_info.get('id') + define_relation_type(asset_type, ds_to_de_info.get('head'))
    
    relation_object = { relation: [
            {
                "name": name,
                "domain": {
                    "name": domain,
                    "community": {"name": community},
                },
            }
        ]
    }
    return relation_object

# combines an assets info and its relations into one object and adds it to json_gen
def create_asset(description, name, domain, community, display_name, type_id, status, relations):
    if description != None:
        attribute = {"description": [
                {"value": description}
            ]
        }
    else: attribute = ""

    asset = {
        "resourceType": "Asset",
        "attributes": attribute,
        "identifier": {
            "name": name,
            "domain": {"name": domain, "community": {"name": community}},
        },
        "displayName": display_name,
        "type": {"id": type_id},
        "status": {"name": status},
        "relations": relations,
    }
    json_gen.append(asset)
    return asset

# gathers column (data element) info from Okera, creates relation to its data set
def create_data_element():
    asset_info = find_asset_id("Data Element")

    for element in elements:
        ds = element.get('datasets')
        for d in ds:
            dataset_name = d.db[0] + "." + d.name
            for col in d.schema.cols:
                name = dataset_name + "." + col.name
                relation = create_relation(dataset_name, dataset_domain, asset_info.get('name'), "Data Set")
                create_asset("", col.name, dataset_domain, community, name, asset_info.get('id'), "status", relation)

# gathers data set info from Okera, creates relation to its database and columns
def create_dataset():
    asset_info = find_asset_id("Data Set")

    for element in elements:
        ds = element.get('datasets')
        for d in ds:
            relations = []
            dataset_name = d.db[0] + "." + d.name
            print("DATASET:")
            print(dataset_name)
            db_relation = create_relation(d.db[0], database_domain, asset_info.get('name'), "Database")
            #relations.update( {db_relation.get('relation') : db_relation.get('relation object')} )
            for col in d.schema.cols:
                name = dataset_name + "." + col.name
                de_relation = create_relation(name, dataset_domain, asset_info.get('name'), "Data Element")
                #relations.update( {de_relation.get('relation') : de_relation.get('relation object')} )
                relations.append(de_relation)

            create_asset("", d.name, dataset_domain, community, dataset_name, asset_info.get('id'), "status", json.loads(relations))

# gathers database info from Okera, creates relation to its data sets
def create_database():
    asset_info = find_asset_id("Database")

    for element in elements:
        db = element.get('database')
        ds = element.get('datasets')
        relations = {}
        #print("DATABASE:")
        #print(db)
        for d in ds:
            dataset_name = d.db[0] + "." + d.name
            relation = create_relation(dataset_name, dataset_domain, asset_info.get('name'), "Data Set")
            relations.update( {relation.get('relation') : relation.get('relation object')} )

        print(relations)
        create_asset("", db, database_domain, community, db, asset_info.get('id'), "status", relations)

# all functions are called here for now...
create_database()
create_dataset()
create_data_element()

# dumps json_gen into JSON file
with open('integration.json', 'w', encoding='utf-8') as f:
    json.dump(json_gen, f, ensure_ascii=False, indent=4)