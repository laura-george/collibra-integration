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
    def __init__(self, name, asset_type, asset_type_id=None, asset_id=None, displayName=None, relation=None, attributes=None, tags=None):
        self.name = name
        self.asset_type = asset_type
        self.asset_type_id = asset_type_id
        self.asset_id = asset_id
        self.displayName = displayName
        self.relation = relation
        self.attributes = attributes
        self.tags = tags
    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

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

def find_asset_id(name):
    for a in assets:
        if a.name == name:
            return a.asset_id

def find_attribute_id(name):
    for x in db.attribute_ids.find({'name': name}):
        return x.get('id')

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
                    None,
                    None, 
                    escape(t.name), 
                    {'Name': escape(t.db[0]), 'Type': "Database"},
                    {'Description': escape(t.description) if t.description else None, 
                    'Location': escape(t.location) if t.location else None}, 
                    create_tags(t.attribute_values)
                    ))
                for col in t.schema.cols:
                    assets.append(Asset(
                        escape(t.db[0] + "." + t.name + "." + col.name),
                        "Column",
                        None,
                        None,
                        escape(col.name),
                        {'Name': escape(t.db[0] + "." + t.name), 'Type': "Table"},
                        {'Description': escape(col.comment) if col.comment else None},
                        create_tags(col.attribute_values)
                    ))

#patch, post, delete assets/bulk
#patch assets/asset id
asset_params = []
attribute_params = []
relation_params = []

def set_attributes(attr):
    attribute_param = {
                'assetId': a.asset_id,
                'typeId': find_attribute_id(attr),
                'value': a.attributes.get(attr)
            }
    attribute_params.append(attribute_param)

# adds all assets
for a in assets:
    for x in db.asset_ids.find({'name': a.asset_type}):
        a.asset_type_id = x.get('id')

    asset_param = {
    'name': a.name,
    'displayName': a.displayName,
    'domainId': domain_id,
    'typeId': a.asset_type_id,
    'excludedFromAutoHyperlinking': "true"
    }
    asset_params.append(asset_param)

# gets all assets ids after being added
get_all_param = {
    'domainId': domain_id,
    'communityId': community_id,
    }
all_assets = json.loads(requests.get(
    configs.get('collibra dgc') + "/rest/2.0/assets", 
    params=get_all_param, 
    auth=(configs.get('collibra username'), configs.get('collibra password')))
    .content
    )
for a in assets:
    for asset in all_assets.get('results'):
        if asset.get('name') == a.name:
            a.asset_id = asset.get('id')

# adds assets relations
    if a.relation:
        relation_info = find_relation_id(a.asset_type, a.relation.get('Type'))
        relation_param = {
            'sourceId': a.asset_id if a.asset_type == relation_info.get('head') else find_asset_id(a.relation.get('Name')),
            'targetId': a.asset_id if a.asset_type == relation_info.get('tail') else find_asset_id(a.relation.get('Name')),
            'typeId': relation_info.get('id')
        }
        relation_params.append(relation_param)
    
    if a.attributes:
        if a.attributes.get('Description'): set_attributes('Description')
        if a.attributes.get('Location'): set_attributes('Location')
     
def import_all():
    requests.post(
        configs.get('collibra dgc') + "/rest/2.0/assets/bulk", 
        json=asset_params, 
        auth=(configs.get('collibra username'), configs.get('collibra password'))
        )
    requests.post(
        configs.get('collibra dgc') + "/rest/2.0/relations/bulk", 
        json=relation_params, 
        auth=(configs.get('collibra username'), configs.get('collibra password'))
        )
    requests.post(
        configs.get('collibra dgc') + "/rest/2.0/attributes/bulk", 
        json=attribute_params, 
        auth=(configs.get('collibra username'), configs.get('collibra password'))
        )

# TODO check assets in collibra and compare to okera to see whether a new assets needs to be added - relations also need to be added!!
# update functions: PATCH endpoints of assets and attributes - if change has occured, patch assets
# unclear: what if name of dataset or column is changed?