import json
import requests
from okera import context
from pymongo import MongoClient
from config import configs
import collections

client = MongoClient(port=27017)
db = client.collibra_ids

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

class Asset:
    def __init__(self, name, asset_type, displayName=None, relation=None, attributes=None, tags=None):
        self.name = name
        self.asset_type = asset_type
        self.displayName = displayName
        self.relation = relation
        self.attributes = attributes
        self.tags = tags
    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

# finds asset id and name in mongodb collection
def find_asset_type_id(asset_name):
    for x in db.asset_ids.find({'name': asset_name}, {'id': 1}):
        return x

def escape(string): return(json.dumps(string)[1:-1])

def create_tags(attribute_values):
    attributes = []
    if attribute_values:
        for attribute in attribute_values:
            name = attribute.attribute.attribute_namespace + "." + attribute.attribute.key 
            attributes.append(name)
        return attributes

def find_relation_id(asset1, asset2):
    for x in db.relation_ids.find({'$and': [{'$or': [{'head': asset1}, {'head': asset2}]}, {'$or': [{'tail': asset1}, {'tail': asset2}]}]}):
        return x

def find_asset_id(asset_name):
    params = {
    'name': asset_name,
    'nameMatchMode': "EXACT",
    'simulation': False,
    'communityId': community_id
    }
    data = json.loads(requests.get(configs.get('collibra dgc') + "/rest/2.0/assets", params=params, auth=(configs.get('collibra username'), configs.get('collibra password'))).content)
    for d in data.get('results'):
        return d.get('id')

# pyokera calls
ctx = context()
ctx.enable_token_auth(token_str=configs.get('token'))
with ctx.connect(host = configs.get('host'), port = configs.get('port')) as conn:
    databases = conn.list_databases()
    assets = []
    for d in databases:
        tables = conn.list_datasets(d)
        assets.append(Asset(d, "Database"))
        if tables:
            for t in tables:
                assets.append(Asset(
                    escape(t.db[0] + "." + t.name),
                    "Table", 
                    escape(t.name), 
                    {'Name': escape(t.db[0]), 'Type': "Database"},
                    {'Description': escape(t.description) if t.description else None, 
                    'Location': escape(t.location) if t.location else None}, 
                    create_tags(t.attribute_values)
                    ))
                for col in t.schema.cols:
                    assets.append(Asset(
                        escape(t.db[0] + t.name + "." + col.name),
                        "Column",
                        escape(col.name),
                        {'Name': t.name, 'Type': "Table"},
                        {'Description': escape(col.comment) if col.comment else None},
                        create_tags(col.attribute_values)
                    ))

#patch, post, delete assets/bulk
#patch assets/asset id
asset_params = []
attribute_params = []
relation_params = []

for a in assets:
    asset_param = {
    'name': a.name,
    'displayName': a.displayName,
    'domainId': domain_id,
    'typeId': find_asset_type_id(a.asset_type),
    'excludedFromAutoHyperlinking': "true"
    }
    asset_params.append(asset_param)

    #TODO fix relations error z{"statusCode":400,"titleMessage":"Value not allowed","helpMessage":"Please select a target that is an \'Database\' or subtype of \'Database\'.","userMessage":"The relation \'is part of Database\' could not be made to \'default.okera_sample\'. This asset was not found in the taxonomy of \'Database\'.","errorCode":"relationTgtIncompatible"}
    if a.relation:
        relation_info = find_relation_id(a.asset_type, a.relation.get('Type'))
        relation_param = {
            'sourceId': find_asset_id(a.name if a.asset_type == relation_info.get('head') else a.relation.get('Name')),
            'targetId': find_asset_id(a.name if a.asset_type == relation_info.get('tail') else a.relation.get('Name')),
            'typeId': relation_info.get('id')
        }
        relation_params.append(relation_param)

requests.post(configs.get('collibra dgc') + "/rest/2.0/assets/bulk", json=asset_params, auth=(configs.get('collibra username'), configs.get('collibra password')))
data = requests.post(configs.get('collibra dgc') + "/rest/2.0/relations/bulk", json=relation_params, auth=(configs.get('collibra username'), configs.get('collibra password')))
print(data.content)
