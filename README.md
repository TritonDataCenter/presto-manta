[![Build Status](https://travis-ci.org/joyent/presto-manta.svg?branch=master)](https://travis-ci.org/joyent/presto-manta)

# Presto Manta Connector

This is a PrestoDB connector that allows you to query unstructured data from 
the open source [Manta](https://github.com/joyent/manta/) object store or 
the public cloud [Triton Object Storage service](https://www.joyent.com/triton/object-storage).

The Presto Manta Connector does not require [Hive](https://hive.apache.org) 
and is a fully stand-alone connector.

## Usage

### Installation and Setup

The Presto Manta Connector's install involves adding a `jar` file to your
plugins directory, adding a catalog configuration file and uploading table
definition file(s) to Manta. You can find details on the install process in
the [installation documentation](./INSTALL.md).

### Catalog Configuration

1. Create a new file: `$PRESTO_HOME/etc/catalog/manta.properties`
2. Within that file add the following:
```
# Required connector name to indicate that we are using the Manta plugin
connector.name=manta

# Manta configuration properties (optional if defined elsewhere).
# You can define the Manta connection properties via environment variables,
# Java system properties, or within this file below.
# See: https://github.com/joyent/java-manta/blob/master/USAGE.md#parameters 
manta.user=my.username
manta.key_id=00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00
manta.max_connections=48

# Schema definition - Each schema is defined by specifying a Manta directory
# path. Within that remote Manta directory path, the plugin will look for
# schema information files like: presto-tables.json
manta.schema.default=/my.username/stor/presto/schema1
manta.schema.test=/my.username/stor/presto/schema2
```

### Table Definition

Tables within a schema are defined in the file `presto-tables.json` which is
contained within the schema directory. The actual data files for the schema
can be located in a different directory path specified within `presto-tables.json`.

The format of table definition file `presto-tables.json` JSON with comments
supported. You can find an example file [here](src/test/resources/test-data/logical-table-definition/presto-tables-example.json). 

### Column Definition

With each table definition configuration element (see above), you can optionally
specify the column names and data types for each row. If the columns are not
explicitly specified, then the plugin will do a best effort guess about the
data types based on the very first row read.

### Partitioning

Input files can be partitioned based on the file path or directory path within
Manta. Partitioning is defined per table and uses a configurable scheme of
regular expression matching groups to allow for matching portions of a file
path.

### Supported Data Formats

 * [Streaming JSON / Newline delimited JSON with uniform structure](http://ndjson.org)
 
### Supported Compression Algorithms
 
 * [BZIP2 with the file extension `.bz2`](https://en.wikipedia.org/wiki/Bzip2)
 * [LZ4 with the file extension `.lz4`](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm))
 * [GZIP with the file extension `.gz`](https://en.wikipedia.org/wiki/Gzip)
 * [SNAPPY with the file extension `.sz`](https://en.wikipedia.org/wiki/Snappy_(compression))
 * [XZ with the file extension `.xz`](https://en.wikipedia.org/wiki/Xz)   


### Known Issues and Limitations

#### Data Format Support

Currently the only data format supported is newline delimited JSON with each
line having a JSON object that is identical in structure without missing
nodes. In future versions, parsing of JSON will become more flexible and other
data formats will be supported like CSV and [parquet](https://parquet.apache.org).

#### Filename Extension Limitations

All compressed data files must have a filename extension that matches the 
compression algorithm.

#### Dynamic Column Definition is by the First Line

Column parsing is done by reading the first line of the smallest file in the
logical table file path. If this first line differs structurally from the data
in other lines and files, you will get inconsistent results or errors.    

For non-compressed data files the connector will do a HTTP range request on the
data file in order to avoid downloading the entire file to get the first line.
The setting for the maximum number of bytes per line is configurable via the
`manta.max_bytes_per_line` parameter. The default value is `10240`.

#### Bandwidth Considerations

All queries to Manta involve downloading multiple data files off of Manta to
Presto worker(s). By the design, this is a bandwidth intensive operation. It is
best to have your Presto workers and server located geographically near your
Manta installation with a high bandwidth link between them. For example, in the 
case of the Triton Public Cloud, if you are using the Manta installation located
in the US-EAST region, then running Presto in one of the US-EAST data centers /
availability zones is ideal.

#### HTTP Pool Settings

Since queries to Manta from Presto are done concurrently per remote file, 
you may want to increase the maximum connections setting to Manta above the
default of `24`. If you see errors related to timeouts when waiting for a 
connection from the HTTP pool for Apache HTTP client, it is indicative of a
`manta.max_connections` setting too low.  

## Development

### Building the Project

To build the Presto Manta Connector you will need [Maven 3.0+](https://maven.apache.org).
Using Maven, execute:
```
mvn clean install
```

### Contributing

See our [contribution guide](./CONTRIBUTING.md) for more information on 
contributing changes to the project.

### Bugs

See <https://github.com/joyent/presto-manta/issues>.

## License
The Presto Manta Connector is licensed under the MPLv2. Please see the 
[LICENSE](./LICENSE) file for more details.
