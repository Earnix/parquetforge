package com.earnix.parquet.columnar.writer;

import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.schema.MessageType;

/**
 * Information about a parquet file that was persisted.
 */
public class ParquetFileInfo
{
	private final long totalParquetFileSize;
	private final MessageType messageType;
	private final FileMetaData fileMetaData;

	public ParquetFileInfo(long totalParquetFileSize, MessageType messageType, FileMetaData fileMetaData)
	{
		this.totalParquetFileSize = totalParquetFileSize;
		this.messageType = messageType;
		this.fileMetaData = fileMetaData;
	}

	/**
	 * Get the size of the parquet file in bytes including the footer metadata and magic.
	 *
	 * @return the size of the parquet file
	 */
	public long getTotalParquetFileSize()
	{
		return totalParquetFileSize;
	}

	/**
	 * The schema of the parquet file
	 *
	 * @return the schema of the parquet file
	 */
	public MessageType getMessageType()
	{
		return messageType;
	}

	/**
	 * Get the footer metadata of the parquet file. Note that this is called {@link FileMetaData} in the parquet thrift
	 * specification
	 *
	 * @return the footer metadata of the parquet file
	 */
	public FileMetaData getFileMetaData()
	{
		return fileMetaData;
	}
}
