package com.earnix.parquet.columnar.utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

public class FileUtils
{
	public static void processPath(String fileName, String suffix, Consumer<Path> pathProcessor)
	{
		try
		{
			Path path = Files.createTempFile(fileName, suffix);
			try
			{
				pathProcessor.accept(path);
			}
			finally
			{
				Files.deleteIfExists(path);
			}
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}

}
