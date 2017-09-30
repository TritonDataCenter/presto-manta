# Installing the Presto Manta Connector

1. Create a new directory `$PRESTO_HOME/plugin/manta`
2. Copy connector jar (named something like `presto-manta-VERSION-jar-with-dependencies.jar`) to `$PRESTO_HOME/plugin/manta`  
3. Create a new directory `$PRESTO_HOME/etc/catalog`
4. Create a new file `$PRESTO_HOME/etc/manta.properties`
5. In the file, you will need the first line to be: `connector.name=manta`. You can 
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
6. Then in each one of the schema directories defined, you will need to upload to 
   Manta a file named `presto-tables.json`. This file has the following format:
```json
[
  {
    "name":"logical-table-1",
    "rootPath":"~~/stor/json-examples",
    "dataFileType":"NDJSON",
    "directoryFilterRegex":"^.*\\/server\\/.*$",
    "filterRegex":""
  },
  {
    "name":"logical-table-2",
    "rootPath":"~~/stor/analytics",
    "dataFileType":"NDJSON",
    "directoryFilterRegex":"",
    "filterRegex":"^.*\\.log$"
  }
]
```

Notes about file format:

 * The `name` field refers to the unique table name for the logical table definition.
 * The `rootPath` field is the root path in Manta to search for files to query.
 * The `dataFileType` field defines the structure of data stored in each file.
 * The `directoryFilterRegex` field defines a Java compatible regular expression
   that can filter subdirectories and files from the `rootPath`. This allows the
   limiting of the search space for subdirectory traversal.
 * The `filterRegex` field defines a Java compatible regular expression that can
   filter all files from the `rootPath`. 
