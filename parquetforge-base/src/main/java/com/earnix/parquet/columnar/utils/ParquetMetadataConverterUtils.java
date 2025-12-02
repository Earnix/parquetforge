package com.earnix.parquet.columnar.utils;

import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.EdgeInterpolationAlgorithm;
import org.apache.parquet.format.GeographyType;
import org.apache.parquet.format.GeometryType;
import org.apache.parquet.format.IntType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.LogicalTypes;
import org.apache.parquet.format.MicroSeconds;
import org.apache.parquet.format.MilliSeconds;
import org.apache.parquet.format.NanoSeconds;
import org.apache.parquet.format.TimeType;
import org.apache.parquet.format.TimeUnit;
import org.apache.parquet.format.TimestampType;
import org.apache.parquet.format.VariantType;
import org.apache.parquet.schema.LogicalTypeAnnotation;

import java.util.Optional;

import static java.util.Optional.of;

/**
 * This is a utility class to convert between thrift metadata and parquet-column objects. Most of this class is copy and
 * paste from the upstream ParquetMetadataConverter class in the hadoop project We cannot reuse that class as we do not
 * want to depend upon hadoop.
 * <br>
 * TODO: refactor class upstream and move type conversion to the parquet-column project. There is already a ticket
 * from upstream for this <a href="http://github.com/apache/parquet-java/issues/1835">here</a>. This is somewhat urgent as
 * this code *WILL* rot. Recently changes were made to add variant type as well as geography type.
 */
public class ParquetMetadataConverterUtils
{
	public static LogicalTypeAnnotation getLogicalTypeAnnotation(LogicalType type)
	{
		switch (type.getSetField())
		{
			case MAP:
				return LogicalTypeAnnotation.mapType();
			case BSON:
				return LogicalTypeAnnotation.bsonType();
			case DATE:
				return LogicalTypeAnnotation.dateType();
			case ENUM:
				return LogicalTypeAnnotation.enumType();
			case JSON:
				return LogicalTypeAnnotation.jsonType();
			case LIST:
				return LogicalTypeAnnotation.listType();
			case TIME:
				TimeType time = type.getTIME();
				return LogicalTypeAnnotation.timeType(time.isAdjustedToUTC, convertTimeUnit(time.unit));
			case STRING:
				return LogicalTypeAnnotation.stringType();
			case DECIMAL:
				DecimalType decimal = type.getDECIMAL();
				return LogicalTypeAnnotation.decimalType(decimal.scale, decimal.precision);
			case INTEGER:
				IntType integer = type.getINTEGER();
				return LogicalTypeAnnotation.intType(integer.bitWidth, integer.isSigned);
			case UNKNOWN:
				return LogicalTypeAnnotation.unknownType();
			case TIMESTAMP:
				TimestampType timestamp = type.getTIMESTAMP();
				return LogicalTypeAnnotation.timestampType(timestamp.isAdjustedToUTC, convertTimeUnit(timestamp.unit));
			case UUID:
				return LogicalTypeAnnotation.uuidType();
			case FLOAT16:
				return LogicalTypeAnnotation.float16Type();
			case GEOMETRY:
				GeometryType geometry = type.getGEOMETRY();
				return LogicalTypeAnnotation.geometryType(geometry.getCrs());
			case GEOGRAPHY:
				GeographyType geography = type.getGEOGRAPHY();
				return LogicalTypeAnnotation.geographyType(geography.getCrs(),
						toParquetEdgeInterpolationAlgorithm(geography.getAlgorithm()));
			case VARIANT:
				VariantType variant = type.getVARIANT();
				return LogicalTypeAnnotation.variantType(variant.getSpecification_version());
			default:
				throw new RuntimeException("Unknown logical type " + type);
		}
	}

	private static LogicalTypeAnnotation.TimeUnit convertTimeUnit(TimeUnit unit)
	{
		switch (unit.getSetField())
		{
			case MICROS:
				return LogicalTypeAnnotation.TimeUnit.MICROS;
			case MILLIS:
				return LogicalTypeAnnotation.TimeUnit.MILLIS;
			case NANOS:
				return LogicalTypeAnnotation.TimeUnit.NANOS;
			default:
				throw new RuntimeException("Unknown time unit " + unit);
		}
	}

	public static LogicalType convertToLogicalType(LogicalTypeAnnotation logicalTypeAnnotation)
	{
		return logicalTypeAnnotation.accept(new LogicalTypeConverterVisitor()).orElse(null);
	}

	private static class LogicalTypeConverterVisitor
			implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<LogicalType>
	{
		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType)
		{
			return of(LogicalTypes.UTF8);
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType)
		{
			return of(LogicalTypes.MAP);
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType)
		{
			return of(LogicalTypes.LIST);
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType)
		{
			return of(LogicalTypes.ENUM);
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType)
		{
			return of(LogicalTypes.DECIMAL(decimalLogicalType.getScale(), decimalLogicalType.getPrecision()));
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType)
		{
			return of(LogicalTypes.DATE);
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType)
		{
			return of(LogicalType.TIME(
					new TimeType(timeLogicalType.isAdjustedToUTC(), convertUnit(timeLogicalType.getUnit()))));
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType)
		{
			return of(LogicalType.TIMESTAMP(new TimestampType(timestampLogicalType.isAdjustedToUTC(),
					convertUnit(timestampLogicalType.getUnit()))));
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType)
		{
			return of(LogicalType.INTEGER(new IntType((byte) intLogicalType.getBitWidth(), intLogicalType.isSigned())));
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType)
		{
			return of(LogicalTypes.JSON);
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType)
		{
			return of(LogicalTypes.BSON);
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.UUIDLogicalTypeAnnotation uuidLogicalType)
		{
			return of(LogicalTypes.UUID);
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.Float16LogicalTypeAnnotation float16LogicalType)
		{
			return of(LogicalTypes.FLOAT16);
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.UnknownLogicalTypeAnnotation unknownLogicalType)
		{
			return of(LogicalTypes.UNKNOWN);
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType)
		{
			return of(LogicalTypes.UNKNOWN);
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.VariantLogicalTypeAnnotation variantLogicalType)
		{
			return of(LogicalTypes.VARIANT(variantLogicalType.getSpecVersion()));
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.GeometryLogicalTypeAnnotation geometryLogicalType)
		{
			GeometryType geometryType = new GeometryType();
			if (geometryLogicalType.getCrs() != null && !geometryLogicalType.getCrs().isEmpty())
			{
				geometryType.setCrs(geometryLogicalType.getCrs());
			}
			return of(LogicalType.GEOMETRY(geometryType));
		}

		@Override
		public Optional<LogicalType> visit(LogicalTypeAnnotation.GeographyLogicalTypeAnnotation geographyLogicalType)
		{
			GeographyType geographyType = new GeographyType();
			if (geographyLogicalType.getCrs() != null && !geographyLogicalType.getCrs().isEmpty())
			{
				geographyType.setCrs(geographyLogicalType.getCrs());
			}
			geographyType.setAlgorithm(fromParquetEdgeInterpolationAlgorithm(geographyLogicalType.getAlgorithm()));
			return of(LogicalType.GEOGRAPHY(geographyType));
		}
	}

	static org.apache.parquet.format.TimeUnit convertUnit(LogicalTypeAnnotation.TimeUnit unit)
	{
		switch (unit)
		{
			case MICROS:
				return org.apache.parquet.format.TimeUnit.MICROS(new MicroSeconds());
			case MILLIS:
				return org.apache.parquet.format.TimeUnit.MILLIS(new MilliSeconds());
			case NANOS:
				return TimeUnit.NANOS(new NanoSeconds());
			default:
				throw new RuntimeException("Unknown time unit " + unit);
		}
	}

	/** Convert Parquet Algorithm enum to Thrift Algorithm enum */
	public static EdgeInterpolationAlgorithm fromParquetEdgeInterpolationAlgorithm(
			org.apache.parquet.column.schema.EdgeInterpolationAlgorithm parquetAlgo)
	{
		if (parquetAlgo == null)
		{
			return null;
		}
		EdgeInterpolationAlgorithm thriftAlgo = EdgeInterpolationAlgorithm.findByValue(parquetAlgo.getValue());
		if (thriftAlgo == null)
		{
			throw new IllegalArgumentException("Unrecognized Parquet EdgeInterpolationAlgorithm: " + parquetAlgo);
		}
		return thriftAlgo;
	}

	/** Convert Thrift Algorithm enum to Parquet Algorithm enum */
	public static org.apache.parquet.column.schema.EdgeInterpolationAlgorithm toParquetEdgeInterpolationAlgorithm(
			EdgeInterpolationAlgorithm thriftAlgo)
	{
		if (thriftAlgo == null)
		{
			return null;
		}
		return org.apache.parquet.column.schema.EdgeInterpolationAlgorithm.findByValue(thriftAlgo.getValue());
	}
}
