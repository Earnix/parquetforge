package com.earnix.parquet.columnar.writer.columnchunk;

import com.earnix.parquet.columnar.writer.page.InMemPageWriter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnWriteStoreV2;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class for writing column chunk values incrementally with a writer, and getting the resultant pages at the end
 */
public class ColumnChunkValuesWriter implements AutoCloseable
{
	private static final Logger LOG = LoggerFactory.getLogger(ColumnChunkValuesWriter.class);

	static final String DUMMY_COL_NAME = "dummy_col_name";

	private final ColumnDescriptor columnDescriptor;
	private final PageWriteStore pageWriteStore;

	private final ColumnWriteStore writeStore;
	private final ColumnWriter columnWriter;
	private final InMemPageWriter inMemPageWriter;
	private volatile ColumnChunkPages pages = null;

	// counter for number of values written
	private long numVals = 0;

	public ColumnChunkValuesWriter(ColumnDescriptor columnDescriptor, ParquetProperties parquetProperties,
			CompressionCodec compressionCodec)
	{
		this.inMemPageWriter = new InMemPageWriter(compressionCodec);
		this.columnDescriptor = columnDescriptor;

		pageWriteStore = descriptor -> {
			if (!columnDescriptor.equals(descriptor))
			{
				throw new IllegalArgumentException(
						"unexpected descriptor: " + descriptor + ". expected: " + columnDescriptor);
			}
			return inMemPageWriter;
		};
		MessageType dummyMessageType = new MessageType(DUMMY_COL_NAME, columnDescriptor.getPrimitiveType());
		writeStore = new ColumnWriteStoreV2(dummyMessageType, pageWriteStore, parquetProperties);

		boolean success = false;
		try
		{
			columnWriter = writeStore.getColumnWriter(columnDescriptor);
			success = true;
		}
		finally
		{
			if (!success)
				writeStore.close();
		}
	}

	/**
	 * Finish writing this chunk and get the associated pages
	 *
	 * @return the column chunk pages
	 */
	public ColumnChunkPages finishAndGetPages()
	{
		if (pages == null)
		{
			synchronized (this)
			{
				if (pages == null)
				{
					if (numVals == 0)
					{
						throw new IllegalArgumentException("A page cannot contain zero values " + columnDescriptor);
					}

					writeStore.flush();

					pages = new ColumnChunkPages(columnDescriptor, inMemPageWriter.getDictionaryPage(),
							inMemPageWriter.getPages());
				}
			}
		}
		return pages;
	}

	private void assertNotFinished()
	{
		if (pages != null)
		{
			throw new IllegalStateException("Already finished writing column " + columnDescriptor);
		}
	}

	@Override
	public void close()
	{
		Exception toThrow = null;
		try
		{
			columnWriter.close();
		}
		catch (Exception ex)
		{
			toThrow = ex;
			LOG.warn("Exception closing column writer", ex);
		}

		try
		{
			writeStore.close();
		}
		catch (Exception ex)
		{
			toThrow = toThrow == null ? ex : toThrow;
			LOG.warn("Exception closing write store", ex);
		}

		try
		{
			this.pageWriteStore.close();
		}
		catch (Exception ex)
		{
			toThrow = toThrow == null ? ex : toThrow;
			LOG.warn("Exception closing page store", ex);
		}

		try
		{
			this.inMemPageWriter.close();
		}
		catch (Exception ex)
		{
			toThrow = toThrow == null ? ex : toThrow;
			LOG.warn("Exception closing in mem page writer", ex);
		}

		if (toThrow != null)
		{
			if (toThrow instanceof RuntimeException)
			{
				throw (RuntimeException) toThrow;
			}
			throw new IllegalStateException(toThrow);
		}
	}

	public void write(int value)
	{
		columnWriter.write(value, maxRepetitionLevel(), maxDefinitionLevel());
		wroteValue();
	}

	public void write(long value)
	{
		columnWriter.write(value, maxRepetitionLevel(), maxDefinitionLevel());
		wroteValue();
	}

	public void write(boolean value)
	{
		columnWriter.write(value, maxRepetitionLevel(), maxDefinitionLevel());
		wroteValue();
	}

	public void write(Binary value)
	{
		columnWriter.write(value, maxRepetitionLevel(), maxDefinitionLevel());
		wroteValue();
	}

	public void write(float value)
	{
		columnWriter.write(value, maxRepetitionLevel(), maxDefinitionLevel());
		wroteValue();
	}

	public void write(double value)
	{
		columnWriter.write(value, maxRepetitionLevel(), maxDefinitionLevel());
		wroteValue();
	}

	private int maxRepetitionLevel()
	{
		return columnDescriptor.getMaxRepetitionLevel();
	}

	private int maxDefinitionLevel()
	{
		return columnDescriptor.getMaxDefinitionLevel();
	}

	public void writeNull()
	{
		if (columnDescriptor.getPrimitiveType().getRepetition() == Type.Repetition.REQUIRED)
			throw new IllegalStateException("Field is required!");
		columnWriter.writeNull(0, 0);
	}

	private void wroteValue()
	{
		writeStore.endRecord();
		++numVals;
		assertNotFinished();
	}

	public ColumnDescriptor getColumnDescriptor()
	{
		return columnDescriptor;
	}
}
