# Collibra to Okera Export Script

The export script is a Python script that uses PyOkera in combination with Collibra’s Core API to compare then export, update and delete attributes from Collibra to Okera.

## Before you begin

This export script will export the following metadata from Collibra to Okera:

* descriptions
* custom attributes (will export as tags in Okera).

The script accepts the following Collibra asset types and hierarchy:

* Database
* Table
* Column

## How mapping of Collibra objects to Okera objects works

The script leverages different methods to ensure resiliency in mapping Collibra objects to Okera objects. This means that if the names of objects change later in Collibra, they will still be mapped correctly to Okera.

>**Note:** Where possible we do not encourage allowing name changes of technical metadata. This should always been in sync with the technical metadata source system.

1. **Collibra mappings feature**

    The integration from your Technical metadata store → Collibra may create asset mappings as part of Collibra’s "mappings" feature. This script will be able to leverage those existing mappings, assuming the technical metadata has the same names in both Okera and your source system.

    > **Note:** This may need some tweaking depending on how the source system integration has been set up.

2. **Collibra object “full name”**

    If no Collibra ID mappings are present as part of Collibra’s mappings feature, this script will leverage the Collibra object's full name. For example for the `dob` column in the `okera_sample.users` table the expected full name would need to be: `okera_sample.users.dob`.

    >**Warning:** This script will not be able to successfully sync attributes if the full name is not specified in the format above.

3. **Add the Collibra table asset id as a table property within Okera**

    To map an existing table in Collibra to an existing dataset in Okera, the asset ID of the table in Collibra must be added to the table properties of the corresponding dataset in Okera.

    Example for creating a table in Okera with the Collibra asset id specified:

    ```sql
    CREATE EXTERNAL TABLE okera_sample.users (
    uid STRING COMMENT 'Unique user id',
    dob STRING COMMENT 'Formatted as DD-month-YY',
    gender STRING,
    ccn STRING COMMENT 'Sensitive data, should not be accessible without masking.'
    )
    COMMENT 'Default okera table.'
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS PARQUET
    LOCATION 'file:/opt/data/users'
    TBLPROPERTIES ("collibra_asset_id" = "123-abc-123abc-123")
    ```

    Example for adding an asset ID to Okera table properties:

    ```sql
    ALTER TABLE okera_sample.users SET TBLPROPERTIES ("collibra_asset_id" = "123-abc-123abc-123")
    ```

    > **Warning:** This mapping is only valid for tables and datasets, not columns or databases. If the Collibra full name is not specified in the correct format above for columns, attributes and descriptions will not be successfully synced.

## Configuring the script

Run `bootstrap.sh` to install all Python3 packages needed to run the script. This will install these necessary dependencies:

* PyOkera
* thriftpy
* requests
* PyYaml

Next enter user-specific integration information in `config.yaml`

Example:

```yaml
# Okera related information
host: "example.okera.com"
token: "Okera access token"

# Collibra information
collibra_dgc: "https://example.collibra.com:443"
collibra_username: "username"
collibra_password: "password"
community: "Example Community"
domain:
  name: "Example Domain"
  type: "Data Asset Domain"
#
# Collibra custom attribute mapping information.
# Specify what Okera namespace you would like to map the Collibra custom attribute to.
# If the Okera namespace already exists, the values of that Collibra attribute
# will be added there as Okera tags. If it doesn't, a new namespace will be created
# in Okera with the name provided.
#
mapped_attribute_okera_namespace: "information_classification"
#
# Prefixes that will be split off the full name of the asset in Collibra.
# The prefixes must be added as a list (see example above).
# The default value (no prefixes) is:
# full_name_prefixes:
#
full_name_prefixes:
    - "Prefix One"
    - "Prefix Two"
```

### Mapping a custom Collibra attribute to Okera’s tags

The export script allows a custom Collibra attribute to be exported to Okera as an Okera tag. See the “Collibra custom attribute mapping“ section in the `config.yaml`.

Take this example if you have a custom Collibra attribute called “Information Classification”. This will create the collibra_info_classification namespace in Okera, and add any values as tags there:

```yaml
mapped_attribute_okera_namespace: "collibra_info_classification"
```

You will also need to enter the Collibra custom attribute resource ID in `resourceids.yaml`

From the above example, you would need to enter the The resource ID for ‘Information Classification’ under attributes:

```yaml
- {name: "Information Classification", id: "123-abc-123abc-123"}
```

## Running the script

To run the script, run export.py.

Running the script will prompt the inputs: Please enter the full name the asset you wish to update: and Is this asset of the type Database or Table?

To export attribute changes made within a specific asset, the full name must be entered e.g. `okera_sample` or `okera_sample.users`. Then the asset's type must be entered (Database or Table). 

> **Note:** If a table in Collibra is mapped to one or multiple tables with different names in Okera, the entire database must be updated.

If the asset is a database it will be exported with all its tables and columns. If the asset is a table it will be exported with all its columns.

> **Note:** This script will not be able to export attributes from Collibra if the corresponding technical assets do not already exist in Okera. The export script does not create any new table or column definitions inside Okera.

Any new attributes added or updated in Collibra will be transferred to Okera as tags. Also any description changes are transferred as well.

The values for the Collibra custom attribute specified in `resourceids.yaml` will be added to tables and columns in Okera as a tag with the `mapped_attribute_okera_namespace` previously defined in `config.yaml`.

## Errors

When running the script for the first time the log file export.log is created which will track the script's stack trace and errors from that point on.

Connection or transport errors that occur for requests made with PyOkera or Collibra’s Core API are printed to export.log with:
* The origin of the error (PyOkera or Collibra)
* The request that failed
* The error message