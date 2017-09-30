# Installing the Presto Manta Connector

1. Create a new directory `$PRESTO_HOME/etc/catalog`
2. Create a new file `$PRESTO_HOME/etc/manta.properties`
3. In the file, you will need the first line to be: `connector.name=manta`. You can 
   add Manta settings to the file as needed. Also, in this file you define the schemas 
   that you support and the path to the schema in Manta. An example file:
```
connector.name=manta
manta.user=user.name
manta.key_id=00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00
manta.max_connections=48
manta.schema.default=~~/stor/json-examples
manta.schema.another=~~/stor/another-schema
```
4. Then in each one of the schema directories defined, you will need to upload to 
   Manta a file named `presto-tables.json`. This file has the following format:
```json
[
  {
    "name":"logical-table-1",
    "rootPath":"~~/stor/json-examples",
    "dataFileType":"NDJSON",
    "directoryFilterRegex":"",
    "filterRegex":""
  },
  {
    "name":"logical-table-2",
    "rootPath":"~~/stor/analytics",
    "dataFileType":"NDJSON",
    "directoryFilterRegex":"",
    "filterRegex":""
  }
]
```
