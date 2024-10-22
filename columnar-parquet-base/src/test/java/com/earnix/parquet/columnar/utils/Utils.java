package com.earnix.parquet.columnar.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class Utils
{
	public static void processForFile(String fileName, String suffix, Consumer<Path> fileProcessor)
	{
		try
		{
			Path file = Files.createTempFile(fileName, suffix);
			try
			{
				fileProcessor.accept(file);
			}
			finally
			{
				Files.deleteIfExists(file);
			}
		}
		catch (IOException ex)
		{
			throw new RuntimeException(ex);
		}
	}

	public static <T> T convertExceptionToRuntime(Callable<T> callable){
		try
		{
			return callable.call();
		}
		catch (Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}

}
