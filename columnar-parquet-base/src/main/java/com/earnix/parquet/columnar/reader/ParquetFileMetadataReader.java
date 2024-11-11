package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import org.apache.commons.io.IOUtils;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

/**
 * A class to assist in reading Parquet File metadata
 */
public class ParquetFileMetadataReader
{
	static final byte[] magicBytes = "PAR1".getBytes(StandardCharsets.US_ASCII);

	/**
	 * Print the parquet metadata for the specified path
	 *
	 * @param path the path to print the metadata for
	 */
	static void printMetadata(Path path)
	{
		try (FileChannel fc = FileChannel.open(path))
		{
			FileMetaData fileMetaData = readMetadata(fc);
			List<RowGroup> rowGroups = fileMetaData.getRow_groups();
			for (RowGroup rowGroup : rowGroups)
			{
				System.out.println("Row group with " + rowGroup.getNum_rows() + " rows.");
				List<ColumnChunk> columns = rowGroup.getColumns();
				for (ColumnChunk col : columns)
				{
					System.out.println(col);
					long dataPageOffset = col.getMeta_data().getData_page_offset();
					fc.position(dataPageOffset);
					PageHeader header = Util.readPageHeader(Channels.newInputStream(fc));
					System.out.println(header);
					System.out.println();
				}
			}
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}

	/**
	 * Read the metadata for the opened file
	 *
	 * @param fc the opened file
	 * @return the parsed metadata
	 * @throws IOException on failure to read the file
	 */
	public static FileMetaData readMetadata(FileChannel fc) throws IOException
	{
		long startPos = validateMagicAndFindFooterStartOffset(fc);
		fc.position(startPos);

		FileMetaData fileMetaData = Util.readFileMetaData(Channels.newInputStream(fc));
		return fileMetaData;
	}

	private static long validateMagicAndFindFooterStartOffset(FileChannel fc) throws IOException
	{
		// we want to read the integer length of the footer, and then the magic bytes.
		int numBytesToRead = Integer.BYTES + magicBytes.length;

		ByteBuffer buf = ByteBuffer.allocate(numBytesToRead);
		buf.order(ByteOrder.LITTLE_ENDIAN);// little endian according to spec.

		// position right before the end of the file
		fc.position(fc.size() - numBytesToRead);
		IOUtils.readFully(fc, buf);
		buf.flip();
		if (buf.remaining() != numBytesToRead)
			throw new IllegalStateException();
		int numBytesInFooters = buf.getInt();

		// validate magic at footer
		assertMagic(buf);

		buf.clear();
		buf.limit(magicBytes.length);
		fc.position(0L);
		IOUtils.readFully(fc, buf);
		fc.read(buf, 0L);
		buf.flip();
		assertMagic(buf);

		long startPos = fc.size() - numBytesInFooters - 8;
		return startPos;
	}

	private static void assertMagic(ByteBuffer buf)
	{
		if (!ParquetMagicUtils.expectMagic(buf))
			throw new IllegalStateException("Parquet file did not contain expected magic");
	}
}
