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
		along with this program.  If not, see <https://www.gnu.org/licenses/>.

		The duckdb_driver.pata.jdbc package is derived from the DuckDB JDBC
		driver (www.duckdb.org). DuckDB is licensed under the MIT License.
		*/

package duckdb_driver.pata.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.duckdb.DuckDBColumnType;

public class PataResultSetMetaData implements ResultSetMetaData
{
	protected List<Field> arrowFields = new ArrayList<Field>();
	protected List<Integer> resultColumnSqlTypes = new ArrayList<Integer>();
	protected List<DuckDBColumnType> resultColumnDuckDBTypes = new ArrayList<DuckDBColumnType>();
	
	public PataResultSetMetaData(List<Field> arrowFields) throws SQLException
	{
		this.arrowFields = arrowFields;
	
		// Prepare DuckDB Types list
		for (Field field : arrowFields)
		{
			resultColumnDuckDBTypes.add(getDuckDBColumnType(field));
		}
		
		// Prepare SqlTypes list
		for (DuckDBColumnType col : resultColumnDuckDBTypes)
		{
			resultColumnSqlTypes.add(type_to_int(col));
		}

	}
	
	public static DuckDBColumnType getDuckDBColumnType(Field field) throws SQLException
	{
		try
		{
			return DuckDBColumnType.valueOf(field.getFieldType().getMetadata().get("Datatype"));
		}
		catch (IllegalArgumentException e)
		{
			throw new SQLException("Datatype empty or unknown: " + field.getName());
		}
	}
	
	public static int type_to_int(DuckDBColumnType type) throws SQLException {
		switch (type) {
			case BOOLEAN:
				return Types.BOOLEAN;
			case TINYINT:
				return Types.TINYINT;
			case SMALLINT:
				return Types.SMALLINT;
			case INTEGER:
				return Types.INTEGER;
			case BIGINT:
				return Types.BIGINT;
			case HUGEINT, INTERVAL, UBIGINT, UINTEGER, USMALLINT, UTINYINT:
				return Types.JAVA_OBJECT;
			case FLOAT:
				return Types.FLOAT;
			case DOUBLE:
				return Types.DOUBLE;
			case DECIMAL:
				return Types.DECIMAL;
			case VARCHAR:
				return Types.VARCHAR;
			case TIME:
				return Types.TIME;
			case DATE:
				return Types.DATE;
			case TIMESTAMP:
				return Types.TIMESTAMP;
			case TIMESTAMP_WITH_TIME_ZONE:
				return Types.TIME_WITH_TIMEZONE;
			case BLOB:
				return Types.BLOB;
	
			default:
				throw new SQLException("Unsupported type " + type.toString());
		}
	}
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getColumnCount() throws SQLException
	{
		return arrowFields.size();
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException
	{
		return true;
	}

	@Override
	public boolean isSearchable(int column) throws SQLException
	{
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException
	{
		return false;
	}

	@Override
	public int isNullable(int column) throws SQLException
	{
		return 0;
	}

	@Override
	public boolean isSigned(int column) throws SQLException
	{
		return false;
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException
	{
		return 0;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException
	{
		return getColumnName(column);
	}

	@Override
	public String getColumnName(int column) throws SQLException
	{
		if(column > getColumnCount())
		{
			throw new SQLException("Column index out of bounds");	
		}
		return arrowFields.get(column - 1).getName();
	}

	@Override
	public String getSchemaName(int column) throws SQLException
	{
		return "";
	}

	@Override
	public int getPrecision(int column) throws SQLException
	{
		if(column > getColumnCount())
		{
			throw new SQLException("Column index out of bounds");	
		}
		String precision = arrowFields.get(column - 1).getFieldType().getMetadata().get("Precision");
		return precision == null ? 0 : Integer.parseInt(precision);
	}

	@Override
	public int getScale(int column) throws SQLException
	{
		if(column > getColumnCount())
		{
			throw new SQLException("Column index out of bounds");	
		}
		String scale = arrowFields.get(column - 1).getFieldType().getMetadata().get("Scale");
		return scale == null ? 0 : Integer.parseInt(scale);
	}

	@Override
	public String getTableName(int column) throws SQLException
	{
		return "";
	}

	@Override
	public String getCatalogName(int column) throws SQLException
	{
		return "";
	}

	@Override
	public int getColumnType(int column) throws SQLException
	{
		if(column > getColumnCount())
		{
			throw new SQLException("Column index out of bounds");	
		}
		return resultColumnSqlTypes.get(column - 1);
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException
	{
		if(column > getColumnCount())
		{
			throw new SQLException("Column index out of bounds");	
		}
		return resultColumnDuckDBTypes.get(column - 1).toString();
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException
	{
		return true;
	}

	@Override
	public boolean isWritable(int column) throws SQLException
	{
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException
	{
		return false;
	}

	@Override
	public String getColumnClassName(int column) throws SQLException
	{
		switch (getColumnType(column)) 
		{
			case Types.BOOLEAN:
				return Boolean.class.toString();
			case Types.TINYINT:
				return Byte.class.toString();
			case Types.SMALLINT:
				return Short.class.toString();
			case Types.INTEGER:
				return Integer.class.toString();
			case Types.BIGINT:
				return Long.class.toString();
			case Types.FLOAT:
				return Float.class.toString();
			case Types.DOUBLE:
				return Double.class.toString();
			case Types.VARCHAR:
				return String.class.toString();
			case Types.TIME:
				return Time.class.toString();
			case Types.DATE:
				return Date.class.toString();
			case Types.TIMESTAMP:
				return Timestamp.class.toString();
			case Types.TIME_WITH_TIMEZONE:
				return OffsetDateTime.class.toString();
			case Types.BLOB:
				return "DuckDBBlobResult";
			case Types.DECIMAL:
				return BigDecimal.class.toString();
			default:
				throw new SQLException("Unknown type " + getColumnTypeName(column));
		}
	}
}
