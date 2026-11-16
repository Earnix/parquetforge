package com.earnix.parquet.columnar.utils;

import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.Util;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.earnix.parquet.columnar.writer.ParquetWriterUtils.getFileMetaDataAllowEmpty;

public class EmptyParquetInMemWriter
{
	/**
	 * A utility method for persisting an empty parquet file with schema ONLY in memory
	 * @param messageType the message schema
	 * @param keyValues the keyvalue metadata to add
	 * @return the schema bytes for the empty parquet file
	 */
	public static byte[] inMemWriteEmptyParquetFile(MessageType messageType, List<KeyValue> keyValues) {
		FileMetaData fileMetaData = getFileMetaDataAllowEmpty(messageType, List.of(), keyValues);
		try (UnsynchronizedByteArrayOutputStream baos =
				UnsynchronizedByteArrayOutputStream.builder().get()) {
			ParquetMagicUtils.writeMagicBytes(baos);
			Util.writeFileMetaData(fileMetaData, baos);
			byte[] footerAndMagic = ParquetMagicUtils.createFooterAndMagic(baos.size() - ParquetMagicUtils.PARQUET_MAGIC.length());
			baos.write(footerAndMagic);
			return baos.toByteArray();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
