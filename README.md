Parquet Columnar Tools
======================

An innovative new low level driver to manipulate Parquet files.

## Motivation:
* Performant migration of other columnar data formats to Parquet
* Multithreaded encoding and compression codec support
* Efficient manipulation of parquet files specifically:
  - Appending parquet files together
  - Adding a column to parquet files
  - Table reordering (join/shuffle)
  - Efficient recompress
* Easy to use high level Java interfaces
* Avoid heavy Hadoop dependencies
* First class integration with AWS S3
* Efficient "disassembly" of a Parquet file into one file per row group
* Efficient "assembly" of many Parquet files (with the same schema)

## Usage
The best way is to read the unit tests.
Start at [this example](columnar-parquet-file/src/test/java/com/earnix/parquet/columnar/file/SampleCreateParquetFileTest.java)

## Basic Design
* Integrates with the parquet-column project
* Avoids any dependencies on heavy hadoop deps
* Builds on top of parquet-java code rather than forking it

## Limitations:
* Only Uncompressed, Snappy and zstd compression codec support
* Statistics are not generated in datapage headers. Parquet cli tool has a bug where parquet files without statistics are noted as invalid stats.
* Unstructured parquet only (no dremeling). Nullable columns are supported.
* Number of rows in a row group are determined before opening a row group
* Only modest testing has been done - more is needed
* Cross compatibility testing (parquet-compat) has not been done
* Pixe
* API is NOT yet stabilized - improvements may break integrations going forward

# Disclaimers
This code is at best beta quality. It has not yet been used in production.
The author does *not* yet recommend this for production use where data corruption/loss
is unacceptable.

It has not undergone intensive compatibility testing to ensure that other Parquet libraries can read the generated files.

