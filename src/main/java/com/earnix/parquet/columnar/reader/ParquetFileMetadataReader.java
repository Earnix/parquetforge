package com.earnix.parquet.columnar.reader;

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

public class ParquetFileMetadataReader
{
	static final byte[] magicBytes = "PAR1".getBytes(StandardCharsets.US_ASCII);

	static void printMetadata(Path path)
	{
		try (FileChannel fc = FileChannel.open(path))
		{
			FileMetaData fileMetaData = readMetadata(fc);
			// System.out.println(fileMetaData);
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

	static FileMetaData readMetadata(FileChannel fc) throws IOException
	{
		long startPos = validateMagicAndFindFooter(fc);
		fc.position(startPos);

		FileMetaData fileMetaData = Util.readFileMetaData(Channels.newInputStream(fc));
		return fileMetaData;
	}

	private static long validateMagicAndFindFooter(FileChannel fc) throws IOException
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
		System.out.println("Footers are " + numBytesInFooters + " bytes");

		// validate magic at footer
		validateMagic(buf);

		buf.clear();
		buf.limit(magicBytes.length);
		fc.position(0L);
		IOUtils.readFully(fc, buf);
		fc.read(buf, 0L);
		buf.flip();
		validateMagic(buf);

		long startPos = fc.size() - numBytesInFooters - 8;
		return startPos;
	}

	private static void validateMagic(ByteBuffer buf)
	{
		for (byte magicByte : magicBytes)
		{
			if (buf.get() != magicByte)
				throw new IllegalStateException();
		}
	}
}
