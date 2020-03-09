import pymongo

#MongoDB setup
client = pymongo.MongoClient("mongodb://localhost:27017/")
collibra_ids = client["collibra_ids"]

#tables/collections
relation_ids= collibra_ids["relation_ids"]
asset_ids = collibra_ids["asset_ids"]
domain_ids = collibra_ids["domain_ids"]
attribute_ids = collibra_ids["attribute_ids"]
status_ids = collibra_ids["status_ids"]

domains = [
  { "name": "Business Asset Domain", "id": "00000000-0000-0000-0000-000000030002"},
  { "name": "Business Dimensions", "id": "00000000-0000-0000-0000-000000030012"},
  { "name": "Report Catalog", "id": "00000000-0000-0000-0000-000000030022"},
  { "name": "Codelist", "id": "00000000-0000-0000-0000-000000020001"},
  { "name": "Hierarchies", "id": "00000000-0000-0000-0000-000000020011"},
  { "name": "Data Asset Domain", "id": "00000000-0000-0000-0000-000000030001"},
  { "name": "Data Usage Registry", "id": "00000000-0000-0000-0000-000000030031"},
  { "name": "Logical Data Dictionary", "id": "00000000-0000-0000-0000-000000030021"},
  { "name": "Mapping Domain", "id": "00000000-0000-0000-0000-000000030007"},
  { "name": "Physical Data Dictionary", "id": "00000000-0000-0000-0000-000000030011"},
  { "name": "Glossary", "id": "00000000-0000-0000-0000-000000010001"},
  { "name": "Governance Asset Domain", "id": "00000000-0000-0000-0000-000000030003"},
  { "name": "Policy Domain", "id": "00000000-0000-0000-0000-000000030013"},
  { "name": "Rulebook", "id": "00000000-0000-0000-0000-000000030023"},
  { "name": "S3 Catalog", "id": "00000000-0000-0000-0001-002200000000"},
  { "name": "Tableau Catalog", "id": "00000000-0000-0000-0000-000000015000"},
  { "name": "Technology Asset Domain", "id": "00000000-0000-0000-0000-000000030004"},
  { "name": "Validation Rule Domain", "id": "00000000-0000-0000-0000-000000030008"}
]

assets = [
    { "name": "Business Asset", "id": "00000000-0000-0000-0000-000000031101"},
    { "name": "Business Dimension", "id": "00000000-0000-0000-0000-000000031105"},
    { "name": "Business Process", "id": "00000000-0000-0000-0000-000000031103"},
    { "name": "Data Category", "id": "00000000-0000-0000-0000-000000031109"},
    { "name": "Line of Business", "id": "00000000-0000-0000-0000-000000031110"},
    { "name": "Tableau Project", "id": "00000000-0000-0000-0000-110000000001"},
    { "name": "Tableau Site", "id": "00000000-0000-0000-0000-110000000000"},
    { "name": "Business Term", "id": "00000000-0000-0000-0000-000000011001"},
    { "name": "Acronym", "id": "00000000-0000-0000-0000-000000011003"},
    { "name": "Measure", "id": "00000000-0000-0000-0000-000000031104"},
    { "name": "KPI", "id": "00000000-0000-0000-0000-000000011002"},
    { "name": "Report", "id": "00000000-0000-0000-0000-000000031102"},
    { "name": "Tableau View", "id": "00000000-0000-0000-0000-110000000003"},
    { "name": "Tableau Dashboard", "id": "00000000-0000-0000-0001-110000000301"},
    { "name": "Tableau Story", "id": "00000000-0000-0000-0001-110000000302"},
    { "name": "Tableau Worksheet", "id": "00000000-0000-0000-0001-110000000300"},
    { "name": "Tableau Workbook", "id": "00000000-0000-0000-0000-110000000002"},
    { "name": "Data Asset", "id": "00000000-0000-0000-0000-000000031002"},
    { "name": "Code Set", "id": "00000000-0000-0000-0000-000000021002"},
    { "name": "Code Value", "id": "00000000-0000-0000-0000-000000021001"},
    { "name": "Crosswalk", "id": "00000000-0000-0000-0000-000000031031"},
    { "name": "Data Element", "id": "00000000-0000-0000-0000-000000031026"},
    { "name": "Column", "id": "00000000-0000-0000-0000-000000031008"},
    { "name": "Data Attribute", "id": "00000000-0000-0000-0000-000000031005"},
    { "name": "Tableau Data Attribute", "id": "00000000-0000-0000-0000-110000000010"},
    { "name": "Field", "id": "00000000-0000-0000-0001-000400000008"},
    { "name": "Report Attribute", "id": "00000000-0000-0000-0001-000400000008"},
    { "name": "Tableau Report Attribute", "id": "00000000-0000-0000-0000-110000000007"},
    { "name": "Data Set", "id": "00000000-0000-0000-0001-000400000001"},
    { "name": "Data Structure", "id": "00000000-0000-0000-0000-000000031025"},
    { "name": "Data Entity", "id": "00000000-0000-0000-0000-000000031004"},
    { "name": "Tableau Data Entity", "id": "00000000-0000-0000-0000-110000000009"},
    { "name": "Data Model", "id": "00000000-0000-0000-0000-000000031003"},
    { "name": "Tableau Data Model", "id": "00000000-0000-0000-0000-110000000008"},
    { "name": "Schema", "id": "00000000-0000-0000-0001-000400000002"},
    { "name": "Table", "id": "00000000-0000-0000-0000-000000031007"},
    { "name": "Data Usage", "id": "00000000-0000-0000-0000-000000031131"},
    { "name": "Foreign Key", "id": "00000000-0000-0000-0001-000400000003"},
    { "name": "Mapping Specification", "id": "00000000-0000-0000-0000-000000031030"},
    { "name": "Governance Asset", "id": "00000000-0000-0000-0000-000000031201"},
    { "name": "Data Quality Dimension", "id": "00000000-0000-0000-0000-000000031108"},
    { "name": "Data Sharing Agreement", "id": "00000000-0000-0000-0000-000000031231"},
    { "name": "Issue Category", "id": "00000000-0000-0000-0000-000000031112"},
    { "name": "Policy", "id": "00000000-0000-0000-0000-000000031202"},
    { "name": "Standard", "id": "00000000-0000-0000-0000-000000031206"},
    { "name": "Rule", "id": "00000000-0000-0000-0000-000000031203"},
    { "name": "Business Rule", "id": "00000000-0000-0000-0000-000000031204"},
    { "name": "Data Quality Metric", "id": "00000000-0000-0000-0000-000000031107"},
    { "name": "Data Quality Rule", "id": "0000000-0000-0000-0000-000000031205"},
    { "name": "Validation Rule", "id": "00000000-0000-0000-0000-000000005119"},
    { "name": "Issue", "id": "00000000-0000-0000-0000-000000031111"},
    { "name": "Data Issue", "id": "00000000-0000-0000-0000-000000031001"},
    { "name": "Technology Asset", "id": "00000000-0000-0000-0000-000000031301"},
    { "name": "Database", "id": "00000000-0000-0000-0000-000000031006"},
    { "name": "Directory", "id": "00000000-0000-0000-0000-000000031303"},
    { "name": "File", "id": "00000000-0000-0000-0000-000000031304"},
    { "name": "File Group", "id": "00000000-0000-0000-0001-002400000002"},
    { "name": "S3 Bucket", "id": "00000000-0000-0000-0001-002400000001"},
    { "name": "Server", "id": "00000000-0000-0000-0000-110000000004"},
    { "name": "Tableau Server", "id": "00000000-0000-0000-0000-110000000005"},
    { "name": "System", "id": "00000000-0000-0000-0000-000000031302"},
    { "name": "S3 File System", "id": "00000000-0000-0000-0001-002400000000"},
    { "name": "Tableau Data Source", "id": "00000000-0000-0000-0000-110000000006"}
]

#if asset is head -> target, if asset is tail -> source
relations = [
    { "head": "Business Term", "role": "synonym", "tail": "Business Term", "id": "00000000-0000-0000-0000-000000007001"},
    { "head": "Business Term", "role": "has code", "tail": "Code Value", "id": "00000000-0000-0000-0000-000000007002"},
    { "head": "Business Term", "role": "allowed value", "tail": "Business Term", "id": "00000000-0000-0000-0000-000000007003"},
    { "head": "Asset", "role": "uses", "tail": "Asset", "id": "00000000-0000-0000-0000-000000007004"},
    { "head": "Data Set", "role": "implemented in", "tail": "Technology Asset", "id": "00000000-0000-0000-0000-000000007005"},
    { "head": "Business Process", "role": "produces", "tail": "Business Asset", "id": "00000000-0000-0000-0000-000000007006"},
    { "head": "Business Dimension", "role": "classifies", "tail": "Asset", "id": "00000000-0000-0000-0000-000000007007"},
    { "head": "Line of Business", "role": "associates", "tail": "Business Asset", "id": "00000000-0000-0000-0000-000000007008"},
    { "head": "Asset", "role": "governed by", "tail": "Governance Asset", "id": "00000000-0000-0000-0000-000000007009"},
    { "head": "Role Type", "role": "is responsible for", "tail": "Asset", "id": "00000000-0000-0000-0000-000000007010"},
    { "head": "Data Element", "role": "is part of", "tail": "Data Structure", "id": "00000000-0000-0000-0000-000000007011"},
    { "head": "Standard", "role": "is included in", "tail": "Policy", "id": "00000000-0000-0000-0000-000000007012"},
    { "head": "Policy", "role": "is enforced by", "tail": "Rule", "id": "00000000-0000-0000-0000-000000007013"},
    { "head": "Rule", "role": "is implemented by", "tail": "Business Rule", "id": "00000000-0000-0000-0000-000000007014"},
    { "head": "Asset", "role": "related to", "tail": "Asset", "id": "00000000-0000-0000-0000-000000007015"},
    { "head": "Data Quality Rule", "role": "executed by", "tail": "Data Quality Metric", "id": "00000000-0000-0000-0000-000000007016"},
    { "head": "Data Asset", "role": "groups", "tail": "Data Asset", "id": "00000000-0000-0000-0000-000000007017"},
    { "head": "Asset", "role": "complies to", "tail": "Governance Asset", "id": "00000000-0000-0000-0000-000000007018"},
    { "head": "Business Asset", "role": "groups", "tail": "Business Asset", "id": "00000000-0000-0000-0000-000000007021"},
    { "head": "Code Value", "role": "groups", "tail": "Code Value", "id": "00000000-0000-0000-0000-000000007022"},
    { "head": "File", "role": "contains", "tail": "Field", "id": "00000000-0000-0000-0000-000000007023"},
    { "head": "Technology Asset", "role": "has", "tail": "Schema", "id": "00000000-0000-0000-0000-000000007024"},
    { "head": "Issue", "role": "impacts", "tail": "Asset", "id": "00000000-0000-0000-0000-000000007025"},
    { "head": "Code Set", "role": "source of", "tail": "Crosswalk", "id": "00000000-0000-0000-0000-000000007026"},
    { "head": "Code Set", "role": "target of", "tail": "Crosswalk", "id": "0000000-0000-0000-0000-000000007027"},
    { "head": "Data Structure", "role": "source of", "tail": "Mapping Specification", "id": "00000000-0000-0000-0000-000000007028"},
    { "head": "Data Structure", "role": "target of", "tail": "Mapping Specification", "id": "00000000-0000-0000-0000-000000007029"},
    { "head": "Business Asset", "role": "has acronym", "tail": "Acronym", "id": "00000000-0000-0000-0000-000000007030"},
    { "head": "Governance Asset", "role": "violated by", "tail": "Issue", "id": "00000000-0000-0000-0000-000000007031"},
    { "head": "Governance Asset", "role": "resolves", "tail": "Issue", "id": "00000000-0000-0000-0000-000000007032"},
    { "head": "Issue", "role": "has duplicate", "tail": "Issue", "id": "00000000-0000-0000-0000-000000007033"},
    { "head": "Technology Asset", "role": "system of record for", "tail": "Business Term", "id": "00000000-0000-0000-0000-000000007034"},
    { "head": "Technology Asset", "role": "system of use for", "tail": "Business Term", "id": "00000000-0000-0000-0000-000000007035"},
    { "head": "Technology Asset", "role": "source of system for", "tail": "Business Term", "id": "00000000-0000-0000-0000-000000007036"},
    { "head": "Business Asset", "role": "represents", "tail": "Data Asset", "id": "00000000-0000-0000-0000-000000007038"},
    { "head": "Data Element", "role": "allowed value set", "tail": "Code Set", "id": "00000000-0000-0000-0000-000000007039"},
    { "head": "Data Element", "role": "allowed value", "tail": "Code Value", "id": "00000000-0000-0000-0000-000000007040"},
    { "head": "Code Value", "role": "is part of", "tail": "Code Set", "id": "00000000-0000-0000-0000-000000007041"},
    { "head": "Column", "role": "is part of", "tail": "Table", "id": "00000000-0000-0000-0000-000000007042"},
    { "head": "Schema", "role": "contains", "tail": "Table", "id": "00000000-0000-0000-0000-000000007043"},
    { "head": "Table", "role": "is part of", "tail":  "Database", "id": "00000000-0000-0000-0000-000000007045"},
    { "head": "Data Entity", "role": "is part of", "tail": "Data Model", "id": "00000000-0000-0000-0000-000000007046"},
    { "head": "Data Entity", "role": "contains", "tail": "Data Attribute", "id": "00000000-0000-0000-0000-000000007047"},
    { "head": "Technology Asset", "role": "system record for", "tail": "Data Asset", "id": "00000000-0000-0000-0000-000000007048"},
    { "head": "Technology Asset", "role": "system of use for", "tail": "Data Asset", "id": "00000000-0000-0000-0000-000000007049"},
    { "head": "Technology Asset", "role": "source system for", "tail": "Data Asset", "id": "00000000-0000-0000-0000-000000007050"},
    { "head": "Data Quality Rule", "role": "allowed value set", "tail": "Code Set", "id": "00000000-0000-0000-0000-000000007051"},
    { "head": "Data Quality Rule", "role": "allowed value", "tail": "Code Value", "id": "00000000-0000-0000-0000-000000007052"},
    { "head": "Data Quality Rule", "role": "classified by", "tail": "Data Quality Dimension", "id": "00000000-0000-0000-0000-000000007053"},
    { "head": "Technology Asset", "role": "groups", "tail": "Technology Asset", "id": "00000000-0000-0000-0000-000000007054"},
    { "head": "Data Usage", "role": "is required by", "tail": "Data Sharing Agreement", "id": "00000000-0000-0000-0000-000000007055"},
    { "head": "Governance Asset", "role": "groups", "tail": "Governance Asset", "id": "00000000-0000-0000-0000-000000007056"},
    { "head": "Data Sharing Agreement", "role": "is requested by", "tail": "Business Dimension", "id": "00000000-0000-0000-0000-000000007057"},
    { "head": "Report Attribute", "role": "contained in", "tail": "Report", "id": "00000000-0000-0000-0000-000000007058"},
    { "head": "Data Asset", "role": "is essential for", "tail": "Data Usage", "id": "00000000-0000-0000-0000-000000007059"},
    { "head": "Directory", "role": "contains", "tail": "File", "id": "00000000-0000-0000-0000-000000007060"},
    { "head": "Asset", "role": "is essential for", "tail": "Data Usage", "id": "00000000-0000-0000-0000-000000007061"},
    { "head": "Data Set", "role": "contains", "tail": "Data Element", "id": "00000000-0000-0000-0000-000000007062"},
    { "head": "Business Process", "role": "consumes", "tail": "Business Asset", "id": "00000000-0000-0000-0000-000000007063"},
    { "head": "Data Set", "role": "related to", "tail": "Business Asset", "id": "00000000-0000-0000-0000-000000007064"},
    { "head": "Asset", "role": "specializes", "tail": "Asset", "id": "00000000-0000-0000-0000-000000007065"},
    { "head": "Data Quality Rule", "role": "governs", "tail": "Data Element", "id": "00000000-0000-0000-0000-000000007066"},
    { "head": "Issue", "role": "categorized by", "tail": "Issue Category", "id": "00000000-0000-0000-0000-000000007067"},
    { "head": "Server", "role": "hosts", "tail": "Business Dimension", "id": "00000000-0000-0000-0000-120000000000"},
    { "head": "Tableau Site", "role": "assembles", "tail": "Tableau Project", "id": "00000000-0000-0000-0000-120000000001"},
    { "head": "Business Dimension", "role": "groups", "tail": "Report", "id": "00000000-0000-0000-0000-120000000002"},
    { "head": "Business Dimension", "role": "source", "tail": "System", "id": "00000000-0000-0000-0000-120000000003"},
    { "head": "Report", "role": "groups", "tail": "Report", "id": "00000000-0000-0000-0000-120000000004"},
    { "head": "System", "role": "implements", "tail": "Data Set", "id": "00000000-0000-0000-0000-120000000005"},
    { "head": "Report", "role": "related to", "tail": "Business Asset", "id": "00000000-0000-0000-0000-120000000006"},
    { "head": "Report", "role": "uses", "tail": "Report", "id": "00000000-0000-0000-0000-120000000007"},
    { "head": "Report Attribute", "role": "is source for", "tail": "Report Attribute", "id": "00000000-0000-0000-0000-120000000008"},
    { "head": "System", "role": "implements", "tail": "Data Model", "id": "00000000-0000-0000-0000-120000000009"},
    { "head": "Report Attribute", "role": "sourced from", "tail": "Data Attribute", "id": "00000000-0000-0000-0000-120000000010"},
    { "head": "Column", "role": "is source for", "tail": "Data Attribute", "id": "00000000-0000-0000-0000-120000000011"},
    { "head": "S3 File System", "role": "contains", "tail": "S3 Bucket", "id": "00000000-0000-0000-0001-002600000000"},
    { "head": "S3 Bucket", "role": "contains", "tail": "Directory", "id": "00000000-0000-0000-0001-002600000001"},
    { "head": "File", "role": "contains", "tail": "Table", "id": "00000000-0000-0000-0001-002600000002"},
    { "head": "Directory", "role": "contains", "tail": "Directory", "id": "00000000-0000-0000-0001-002600000003"},
    { "head": "Directory", "role": "contains", "tail": "File Group", "id": "00000000-0000-0000-0001-002600000004"},
    { "head": "File Group", "role": "contains", "tail": "Table", "id": "00000000-0000-0000-0001-002600000005"},
]

attributes = [
    {"name": "Description", "id": "00000000-0000-0000-0000-000000003114"},
    {"name": "Location", "id": "00000000-0000-0000-0000-000000000203"},
    {"name": "Technical Data Type", "id": "00000000-0000-0000-0000-000000000219"},
    {"name": "Data Type", "id": "00000000-0000-0000-0001-000500000005"}
]

statuses = [
    {"name": "Accepted", "id": "00000000-0000-0000-0000-000000005009"},
    {"name": "Access Granted", "id": "00000000-0000-0000-0000-000000005024"},
    {"name": "Approval Pending", "id": "00000000-0000-0000-0000-000000005023"},
    {"name": "Approved", "id": "00000000-0000-0000-0000-000000005025"},
    {"name": "Candidate", "id": "00000000-0000-0000-0000-000000005008"},
    {"name": "Deployed", "id": "00000000-0000-0000-0000-000000005053"},
    {"name": "Disabled", "id": "00000000-0000-0000-0000-000000005052"},
    {"name": "Enabled", "id": "00000000-0000-0000-0000-000000005051"},
    {"name": "Implemented", "id": "00000000-0000-0000-0000-000000005055"},
    {"name": "In Progress", "id": "00000000-0000-0000-0000-000000005019"},
    {"name": "Invalid", "id": "00000000-0000-0000-0000-000000005022"},
    {"name": "Monitored", "id": "00000000-0000-0000-0000-000000005054"},
    {"name": "New", "id": "00000000-0000-0000-0000-000000005058"},
    {"name": "Obsolete", "id": "00000000-0000-0000-0000-000000005011"},
    {"name": "Pending", "id": "00000000-0000-0000-0000-000000005059"},
    {"name": "Rejected", "id": "00000000-0000-0000-0000-000000005010"},
    {"name": "Resolution Pending", "id": "00000000-0000-0000-0000-000000005056"},
    {"name": "Resolved", "id": "00000000-0000-0000-0000-000000005057"},
    {"name": "Reviewed", "id": "00000000-0000-0000-0000-000000005021"},
    {"name": "Submitted for Approval", "id": "00000000-0000-0000-0000-000000005060"},
    {"name": "Under Review", "id": "00000000-0000-0000-0000-000000005020"}
]

domain_ids.drop()
asset_ids.drop()
relation_ids.drop()
attribute_ids.drop()
status_ids.drop()

domain_ids.insert_many(domains)
asset_ids.insert_many(assets)
relation_ids.insert_many(relations)
attribute_ids.insert_many(attributes)
status_ids.insert_many(statuses)
