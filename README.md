Parquet Columnar Tools
======================

An innovative new low level driver to manipulate Parquet files.

## Motivation:

* Performant migration of other columnar data formats to Parquet
* Multithreaded encoding and compression codec support
* Efficient manipulation of parquet files specifically:
    * Appending parquet files together
    * Adding a column to parquet files
    * Table reordering (join/shuffle)
    * Efficient recompress
* Easy to use high level Java interfaces
* Avoid heavy Hadoop dependencies
* First class integration with AWS S3
* Efficient "assembly" of many Parquet files (with the same schema)
    * Efficient implementation of parquet append
    * Supports adding columns too, but row group boundaries *must* be identical
* Efficient splitting of a Parquet file into one file per row group
    * For distributed processing of a large parquet file - send one row group files per node
    * Avoids the need for complex distributed file systems

## Features

* Create Parquet file from raw values
    * Base interface is
      [
      `ParquetColumnarWriter`](columnar-parquet-base/src/main/java/com/earnix/parquet/columnar/writer/ParquetColumnarWriter.java)
    * Creation is done column by column
    * Parallel creation of columns within the same row group is supported
    * Creation to filesystem and S3 are supported
    * S3 implementation can begin uploading data via multipart upload API before file is completely created
* Assemble Parquet file from existing parquet files
    * Base class
      is [BaseParquetAssembler](columnar-parquet-base/src/main/java/com/earnix/parquet/columnar/assembler/BaseParquetAssembler.java)
    * Binary copy, no raw value encoding supported
    * Only supports for S3 currently. Assembly of local files is not yet built
    * Parquet file creation on S3 is done without local buffering using multiple connections with multipart upload
* Split Parquet file from into one Parquet file per row group
    * Currently only supported when downloading a file from S3
    * Implementation
      in [S3ParquetFilePartDownloader](columnar-parquet-s3/src/main/java/com/earnix/parquet/columnar/s3/downloader/S3ParquetFilePartDownloader.java)
    * Zero copy - blocks are downloaded directly into their destination files

## Usage

The best way to get started is to read the unit tests.
For the most basic example of creating a parquet file by
columns, [see this unit test](columnar-parquet-file/src/test/java/com/earnix/parquet/columnar/file/SampleCreateParquetFileTest.java)

## High Level Design Principles Used

* Integrates with the parquet-column project
* Avoids any dependencies on heavy hadoop deps
* Builds on top of parquet-java code rather than forking it
* Performance oriented implementations
    * Avoid copying data when unnecessary (zero copy)
    * Allow parallelism when possible
    * Flush to backing storage (FileSystem or S3) rather than buffering in memory
* Aims for compatibility with other Parquet implementations

## Limitations:

* This is in an Alpha state
    * API is NOT yet stabilized - changes may break integrations
    * Not all features support all backing storage systems
* Limited compression codec support. Only supports:
    * Uncompressed
    * Snappy
    * zstd
* Statistics are not generated in datapage headers.
    * The thrift file in parquet-format documents that statistics are optional, so generated files should be according
      to spec
    * Pulldown predicates will fail for these parquet files
    * Parquet cli tool has a bug where parquet files without statistics are noted as invalid. They are missing, not
      invalid
* Unstructured parquet only (no dremeling). Nullable columns are supported.
* Number of rows in a row group are determined before opening a row group
* Only modest testing has been done - more is needed
* Cross compatibility testing (parquet-compat) has not been done
* Optimizing column ordering in a row is not a non-goal, but this library can be extended to reordering column order

# Disclaimers

This code is at alpha quality. It has not yet been used in production.
The author does *not* yet recommend this for production use where data corruption/loss
is unacceptable.

It has not undergone intensive compatibility testing to ensure that other Parquet libraries can read the generated
files.

