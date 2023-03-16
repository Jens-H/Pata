/*		Pata JDBC Driver for connecting to DuckDB processes
		Copyright (C) 2023  Jens Hofer

		This program is free software: you can redistribute it and/or modify
		it under the terms of the GNU General Public License as published by
		the Free Software Foundation, either version 3 of the License, or
		(at your option) any later version.

		This program is distributed in the hope that it will be useful,
		but WITHOUT ANY WARRANTY; without even the implied warranty of
		MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
		GNU General Public License for more details.

		You should have received a copy of the GNU General Public License
		along with this program.  If not, see <https://www.gnu.org/licenses/>.*/

package duckdb_driver.pata.server;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.channels.WritableByteChannel;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.duckdb.DuckDBResultSet;
import org.duckdb.DuckDBResultSetMetaData;
import org.duckdb.DuckDBColumnType;
import org.duckdb.DuckDBTimestamp;

public class ArrowResultSet implements AutoCloseable
{
	private RootAllocator allocator;
	private VectorSchemaRoot vecSchemaRoot;
	private List<Field> arrowFields = new ArrayList<Field>();
	private List<FieldVector> arrowVectors = new ArrayList<FieldVector>();
	

	public ArrowResultSet(DuckDBResultSet rs) throws Exception
	{
		allocator = new RootAllocator(Long.MAX_VALUE);
		
		createSchemaData(allocator, rs);
		
		fillVectorSchemaRoot(rs);
	}
	
	public ArrowStreamWriter getArrowStreamWriter(WritableByteChannel chan)
	{
		return new ArrowStreamWriter(vecSchemaRoot, null, chan);
	}
	
	private void fillVectorSchemaRoot(DuckDBResultSet rs) throws Exception
	{	
		int columnCount = rs.getMetaData().getColumnCount();
		
		int row = 0;
		
		// All rows
		while(rs.next())
		{	
			// All columns
			for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
			{
				int sqlColumnIndex = columnIndex + 1;
				
				DuckDBColumnType colType = DuckDBResultSetMetaData.TypeNameToType(rs.getMetaData().getColumnTypeName(sqlColumnIndex));
			
				// Check for null value
				if (colType != DuckDBColumnType.VARCHAR)
				{
					rs.getObject(sqlColumnIndex);
					if (rs.wasNull())
					{
						BaseFixedWidthVector vec = (BaseFixedWidthVector) arrowVectors.get(columnIndex);
						vec.setNull(row);
						continue;
					}
				}

				try {
					switch (colType) {
						case BOOLEAN:
							BitVector bVector = (BitVector) arrowVectors.get(columnIndex);
							bVector.setSafe(row, rs.getBoolean(sqlColumnIndex) ? 1 : 0);
							break;
						case TINYINT:
							TinyIntVector tvector = (TinyIntVector) arrowVectors.get(columnIndex);
							tvector.setSafe(row, rs.getByte(sqlColumnIndex));
							break;
						case SMALLINT:
							SmallIntVector svector = (SmallIntVector) arrowVectors.get(columnIndex);
							svector.setSafe(row, rs.getShort(sqlColumnIndex));
							break;
						case INTEGER:
							IntVector ivector = (IntVector) arrowVectors.get(columnIndex);
							ivector.setSafe(row, rs.getInt(sqlColumnIndex));
							break;
						case BIGINT:
							BigIntVector bvector = (BigIntVector) arrowVectors.get(columnIndex);
							bvector.setSafe(row, rs.getLong(sqlColumnIndex));
							break;
						case HUGEINT:
							Decimal256Vector hvector = (Decimal256Vector) arrowVectors.get(columnIndex);
							hvector.setSafe(row, new BigDecimal(rs.getHugeint(sqlColumnIndex)));
							break;
						case UTINYINT:
							UInt1Vector u1vector = (UInt1Vector) arrowVectors.get(columnIndex);
							u1vector.setSafe(row, ((Short) rs.getObject(sqlColumnIndex)).intValue());
							break;
						case USMALLINT:
							UInt2Vector u2vector = (UInt2Vector) arrowVectors.get(columnIndex);
							u2vector.setSafe(row, (int) rs.getObject(sqlColumnIndex));
							break;
						case UINTEGER:
							UInt4Vector u4vector = (UInt4Vector) arrowVectors.get(columnIndex);
							u4vector.setWithPossibleTruncate(row, (long) rs.getObject(sqlColumnIndex));
							break;
						case UBIGINT:
							UInt8Vector u8vector = (UInt8Vector) arrowVectors.get(columnIndex);
							u8vector.setSafe(row, ((BigInteger) rs.getObject(sqlColumnIndex)).longValue()); // Cast to long is not correct!!!
							break;
						case FLOAT:
							Float4Vector f4vector = (Float4Vector) arrowVectors.get(columnIndex);
							f4vector.setSafe(row, rs.getFloat(sqlColumnIndex));
							break;
						case DOUBLE:
							Float8Vector f8vector = (Float8Vector) arrowVectors.get(columnIndex);
							f8vector.setSafe(row, rs.getDouble(sqlColumnIndex));
							break;
						case DECIMAL:
							Decimal256Vector dvector = (Decimal256Vector) arrowVectors.get(columnIndex);
							dvector.setSafe(row, rs.getBigDecimal(sqlColumnIndex));
							break;
						case VARCHAR:
						case ENUM:
							VarCharVector vvector = (VarCharVector) arrowVectors.get(columnIndex);
							String rsString = rs.getString(sqlColumnIndex);
							if (rsString != null) {
								vvector.setSafe(row, new Text(rsString));
							} else {
								vvector.setNull(row);
							}
							break;
						case TIME:
							TimeMilliVector tivector = (TimeMilliVector) arrowVectors.get(columnIndex);
							tivector.setSafe(row, (int) rs.getTime(sqlColumnIndex).getTime()); // Time is 4 bytes but wrapped into util.Date
							break;
						case DATE:
							DateMilliVector davector = (DateMilliVector) arrowVectors.get(columnIndex);
							davector.setSafe(row, rs.getDate(sqlColumnIndex).getTime());
							break;
						case TIMESTAMP:
							TimeStampMicroVector tsvector = (TimeStampMicroVector) arrowVectors.get(columnIndex);
							//tsvector.setSafe(row, rs.getTimestamp(sqlColumnIndex).getTime());
							tsvector.setSafe(row, rs.getLong(sqlColumnIndex));
							break;
						case TIMESTAMP_WITH_TIME_ZONE:
							TimeStampMilliTZVector tstzvector = (TimeStampMilliTZVector) arrowVectors.get(columnIndex);
							DuckDBTimestamp tsTz = new DuckDBTimestamp(rs.getObject(sqlColumnIndex, OffsetDateTime.class));
							tstzvector.setSafe(row, tsTz.getMicrosEpoch());
							break;
						default:
							throw new Exception("Not implemented type.");
					}
				}
				catch (Exception e)
				{
					// Something went wrong, but better ignore this value than blow up the complete response
					BaseFixedWidthVector vec = (BaseFixedWidthVector) arrowVectors.get(columnIndex);
					vec.setNull(row);					
				}
			}
			row++;
		}

		// Set no. of rows in result table
		for (FieldVector vec : arrowVectors)
		{
			vec.setValueCount(row);
		}
		
		vecSchemaRoot = new VectorSchemaRoot(arrowFields, arrowVectors);
	}
	
	private void createSchemaData(RootAllocator allocator, DuckDBResultSet rs) throws Exception
	{	
		int columnCount = rs.getMetaData().getColumnCount();
		
		for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++)
		{
			String columnName =  rs.getMetaData().getColumnName(columnIndex);
				
			DuckDBColumnType colType = DuckDBResultSetMetaData.TypeNameToType(rs.getMetaData().getColumnTypeName(columnIndex));
			
			// For easier copy-paste
			Field tmpField = null;
			
			switch (colType)
			{
			case BOOLEAN:
				BitVector vector = new BitVector(columnName, allocator);
				tmpField = vector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "BOOLEAN"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(vector);
				break;
			case TINYINT:
				TinyIntVector tvector = new TinyIntVector(columnName, allocator);
				tmpField = tvector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "TINYINT"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(tvector);
				break;
			case SMALLINT:
				SmallIntVector svector = new SmallIntVector(columnName, allocator);
				tmpField = svector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "SMALLINT"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(svector);
				break;
			case INTEGER:
				IntVector ivector = new IntVector(columnName, allocator);
				tmpField = ivector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "INTEGER"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(ivector);
				break;
			case BIGINT:
				BigIntVector bvector = new BigIntVector(columnName, allocator);
				tmpField = bvector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "BIGINT"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(bvector);
				break;
			case HUGEINT:
				Decimal256Vector hvector = new Decimal256Vector(columnName, allocator, 76, 0);
				hvector.allocateNewSafe();
				tmpField = hvector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "HUGEINT"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(hvector);
				break;
			case UTINYINT:
				UInt1Vector u1vector = new UInt1Vector(columnName, allocator);
				tmpField = u1vector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "UTINYINT"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(u1vector);
				break;
			case USMALLINT:
				UInt2Vector u2vector = new UInt2Vector(columnName, allocator);
				tmpField = u2vector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "USMALLINT"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(u2vector);
				break;
			case UINTEGER:
				UInt4Vector u4vector = new UInt4Vector(columnName, allocator);
				tmpField = u4vector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "UINTEGER"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(u4vector);
				break;
			case UBIGINT:
				UInt8Vector u8vector = new UInt8Vector(columnName, allocator);
				tmpField = u8vector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "UBIGINT"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(u8vector);
				break;
			case FLOAT:
				Float4Vector f4vector = new Float4Vector(columnName, allocator);
				tmpField = f4vector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "FLOAT"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(f4vector);
				break;
			case DOUBLE:
				Float8Vector f8vector = new Float8Vector(columnName, allocator);
				tmpField = f8vector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "DOUBLE"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(f8vector);
				break; 
			case DECIMAL:
				Decimal256Vector dvector = new Decimal256Vector(columnName, allocator
						, rs.getMetaData().getPrecision(columnIndex), rs.getMetaData().getScale(columnIndex));
				tmpField = dvector.getField();
				String precision = String.valueOf(rs.getMetaData().getPrecision(columnIndex));
				String scale = String.valueOf(rs.getMetaData().getScale(columnIndex));
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "DECIMAL"
										,"Precision", precision
										,"Scale", scale))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(dvector);
				break;
			case VARCHAR:
			case ENUM:
				VarCharVector vvector = new VarCharVector(columnName, allocator);
				tmpField = vvector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "VARCHAR"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(vvector);
				break;
			case TIME:
				TimeMilliVector tivector = new TimeMilliVector(columnName, allocator);
				tmpField = tivector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "TIME"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(tivector);
				break;
			case DATE:
				DateMilliVector davector = new DateMilliVector(columnName, allocator);
				tmpField = davector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "DATE"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(davector);
				break;
			case TIMESTAMP:
				TimeStampMicroVector tsvector = new TimeStampMicroVector(columnName, allocator);
				tmpField = tsvector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "TIMESTAMP"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(tsvector);
				break;
			case TIMESTAMP_WITH_TIME_ZONE:
				TimeStampMilliTZVector tstzvector = new TimeStampMilliTZVector(columnName, allocator, ZoneOffset.UTC.toString());
				tmpField = tstzvector.getField();
				tmpField = new Field(columnName, 
						new FieldType(tmpField.getFieldType().isNullable(), tmpField.getFieldType().getType(), tmpField.getFieldType().getDictionary()
								, Map.of("Datatype", "TIMESTAMP_WITH_TIME_ZONE"))
						, tmpField.getChildren());
				arrowFields.add(tmpField);
				arrowVectors.add(tstzvector);
				break;
			default:
				throw new SQLException("Not implemented type.");
			}
			
			FieldVector fvec = arrowVectors.get(columnIndex - 1);
			fvec.allocateNewSafe();
		}
	}
	
	public void close()
	{
		System.out.println("Autoclose ArrowResultSet");
		vecSchemaRoot.close();
		allocator.close();
	}
}
