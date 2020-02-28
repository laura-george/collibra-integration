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
    def __init__(self, name, asset_type, asset_type_id=None, asset_id=None, displayName=None, relation=None, attributes=None, attribute_ids=None, tags=None):
        self.name = name
        self.asset_type = asset_type
        self.asset_type_id = asset_type_id
        self.asset_id = asset_id
        self.displayName = displayName
        self.relation = relation
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
    if attribute_values:
        for attribute in attribute_values:
            name = attribute.attribute.attribute_namespace + "." + attribute.attribute.key 
            attributes.append(name)
        return attributes

# finds assetID in list of assets from Okera (for finding assetID of relations)
def find_okera_info(name, info):
    for a in assets:
        if a.name == name:
            return getattr(a, info)

# builds collibra request
def collibra_get(param_obj, call, method, header=None):
    if method == 'get':
        return getattr(requests, method)(
            configs.get('collibra dgc') + "/rest/2.0/" + call, 
            params=param_obj, 
            auth=(configs.get('collibra username'), configs.get('collibra password'))).content
    else:
        return getattr(requests, method)(
            configs.get('collibra dgc') + "/rest/2.0/" + call,
            headers=header, 
            json=param_obj, 
            auth=(configs.get('collibra username'), configs.get('collibra password'))).content


# MongoDB find functions
def find_relation_id(asset1, asset2):
    for x in db.relation_ids.find({'$and': [{'$or': [{'head': asset1}, {'head': asset2}]}, {'$or': [{'tail': asset1}, {'tail': asset2}]}]}):
        return x

def find_attribute_id(name):
    for x in db.attribute_ids.find({'name': name}):
        return x.get('id')

def find_asset_id(asset_type):
    for x in db.asset_ids.find({'name': asset_type}):
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
                    None, 
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
                        None,
                        create_tags(col.attribute_values)
                    ))

def get_okera_assets(name, asset_type):
    okera_assets = []
    for a in assets:
        if a.name == name and a.asset_type == asset_type or a.name.startswith(name + ".") :
            a.asset_type_id = find_asset_id(a.asset_type)
            okera_assets.append(a)
    return okera_assets

asset_params = []
attribute_params = []
relation_params = []



# gets assets from Collibra
def get_assets(asset_name=None, asset_type=None):
    parent_param = {
        'name': asset_name,
        'nameMatchMode': "EXACT",
        'domainId': domain_id,
        'communityId': community_id,
        'typeId': find_asset_id(asset_type)
        }
    parent = json.loads(collibra_get(parent_param, "assets", "get")).get('results')

    def find_children(name, child_type):
        child_param = {
            'name': name + ".",
            'nameMatchMode': "START",
            'domainId': domain_id,
            'communityId': community_id,
            'typeId': find_asset_id(child_type)
            }
        return json.loads(collibra_get(child_param, "assets", "get")).get('results')

    if asset_name:
        if asset_type == 'Table':
            return parent + find_children(asset_name, 'Column')
        elif asset_type == 'Database':
            return parent + find_children(asset_name, 'Table') + find_children(asset_name, 'Column')

    else:
        get_all_param = {
            'domainId': domain_id,
            'communityId': community_id,
            }
        return json.loads(collibra_get(get_all_param, "assets", "get")).get('results')

def set_assets(a):
    return ({
    'name': a.name,
    'displayName': a.displayName,
    'domainId': domain_id,
    'typeId': a.asset_type_id,
    'excludedFromAutoHyperlinking': "true"
    })

# WIP
def import_attributes(a):
    for asset in get_assets():
        if asset.get('name') == a.name:
                a.asset_id = asset.get('id')
        
        # adds assets attributes
        def set_attributes(attr):
            attribute_params.append({
                        'assetId': a.asset_id,
                        'typeId': find_attribute_id(attr),
                        'value': a.attributes.get(attr)
                    })

        if a.attributes:
            if a.attributes.get('Description'): set_attributes('Description')
            if a.attributes.get('Location'): set_attributes('Location')

# WIP
def import_relations(a):
    for asset in get_assets():
        if asset.get('name') == a.name:
            a.asset_id = asset.get('id')
    # adds assets relations
    if a.relation:
        relation_info = find_relation_id(a.asset_type, a.relation.get('Type'))
        relation_params.append({
            'sourceId': a.asset_id if a.asset_type == relation_info.get('head') else find_asset_id(a.relation.get('Name')),
            'targetId': a.asset_id if a.asset_type == relation_info.get('tail') else find_asset_id(a.relation.get('Name')),
            'typeId': relation_info.get('id')
        })

def bulk_import():
    # adds all assets
    for a in assets:
        a.asset_type_id = find_asset_id(a.asset_type)
        asset_params.append({
        'name': a.name,
        'displayName': a.displayName,
        'domainId': domain_id,
        'typeId': a.asset_type_id,
        'excludedFromAutoHyperlinking': "true"
        })

        all_assets = get_assets()

        # gets all assets ids
        for asset in all_assets:
            if asset.get('name') == a.name:
                a.asset_id = asset.get('id')

        # adds assets relations
        if a.relation:
            relation_info = find_relation_id(a.asset_type, a.relation.get('Type'))
            relation_params.append({
                'sourceId': a.asset_id if a.asset_type == relation_info.get('head') else find_asset_id(a.relation.get('Name')),
                'targetId': a.asset_id if a.asset_type == relation_info.get('tail') else find_asset_id(a.relation.get('Name')),
                'typeId': relation_info.get('id')
            })
        
        # adds assets attributes
        def set_attributes(attr):
            attribute_params.append({
                        'assetId': a.asset_id,
                        'typeId': find_attribute_id(attr),
                        'value': a.attributes.get(attr)
                    })

        if a.attributes:
            if a.attributes.get('Description'): set_attributes('Description')
            if a.attributes.get('Location'): set_attributes('Location')

    # requests sent to collibra
    collibra_get(asset_params, 'assets/bulk', 'post')
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


def update(asset_name=None, asset_type=None):
    collibra_assets = []
    update_attr = []
    import_assets = []
    for ua in get_assets(asset_name, asset_type):
        asset = Asset(
            ua.get('name'),
            ua.get('type').get('name'),
            ua.get('type').get('id'),
            ua.get('id'),
            ua.get('displayName')
        )
        collibra_assets.append(asset)
        attributes = {}
        attribute_ids = {}
        attribute = json.loads(collibra_get({'assetId': asset.asset_id}, "attributes", "get")).get('results')
        matched_attr = find_okera_info(asset.name, "attributes")
        for attr in attribute:
            attributes.update({attr.get('type').get('name'): attr.get('value')})
            attribute_ids.update({attr.get('type').get('name'): attr.get('id')})
        asset.attributes = attributes
        asset.attribute_ids = attribute_ids

        if asset.attributes and matched_attr:
            for key in asset.attributes:
                if asset.attributes[key] != matched_attr[key]:
                    update_attr.append({'id': asset.attribute_ids[key], 'value': matched_attr[key]})

    for new_asset in [obj for obj in get_okera_assets(asset_name, asset_type) if obj not in collibra_assets]:
        import_assets.append(set_assets(new_asset))

    if import_assets:
        print(collibra_get(import_assets, "assets/bulk", "post"))
        # TODO ADD RELATIONS HERE

    #if update_attr:
       # collibra_get(update_attr, "attributes/bulk", "post", {'X-HTTP-Method-Override': "PATCH"})

# TODO add try except block
#which_asset = input("Please enter the name the asset you wish to update: ")
#which_type = input("Is this asset of the type Database or Table? ")
update('default', 'Database')


""" for a in get_assets():
    if which_asset == a.get('name'):
        print("ASSET FOUND")
        break
    else:
        print('Asset not found!')
        print(a.get('name')) """

# TODO check assets in collibra and compare to okera to see whether a new assets needs to be added - relations also need to be added!!
# update functions: PATCH endpoints of assets and attributes - if change has occured, patch assets
# unclear: what if name of dataset or column is changed?