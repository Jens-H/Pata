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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Map;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.duckdb.DuckDBColumnType;
import org.duckdb.DuckDBTimestamp;

public class PataResultSet implements ResultSet
{
	private static DateTimeFormatter timeFormat = DateTimeFormatter.ISO_TIME;

	private VectorSchemaRoot resultVector;
	private PataPreparedStatement stmt;
	private PataResultSetMetaData meta;

	private boolean finished = false;
	private int row = -1; // The first next() call will make it 0
	private int rowCnt;
	private boolean wasNull = false;

	public PataResultSet(PataPreparedStatement stmt, ArrowStreamReader ar)
	{
		try
		{
			this.stmt = stmt;
			
			// Create schema root and load data
			this.resultVector = ar.getVectorSchemaRoot();
			ar.loadNextBatch();
			
			this.meta = new PataResultSetMetaData(resultVector.getSchema().getFields());
			this.rowCnt = resultVector.getRowCount();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean next() throws SQLException
	{
		if (isClosed())
		{
			throw new SQLException("ResultSet was closed");
		}
		if (finished) {
			return false;
		}

		if (rowCnt > row + 1)
		{
			row++;
			return true;
		}
		else
		{
			finished = true;
			return false;
		}
	}

	@Override
	public void close() throws SQLException
	{
		stmt = null;
		meta = null;
		resultVector = null;
	}

	@SuppressWarnings("removal")
	@Override
	protected void finalize() throws Throwable
	{
		close();
	}

	@Override
	public boolean wasNull() throws SQLException
	{
		return wasNull;
	}

	private boolean isType(int columnIndex, DuckDBColumnType type) {
		return meta.resultColumnDuckDBTypes.get(columnIndex - 1) == type;
	}

	@Override
	public String getString(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return null;
		}

		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.VARCHAR)
				|| isType(columnIndex, DuckDBColumnType.ENUM))
		{
			return new String(((VarCharVector)resultVector.getVector(columnIndex - 1)).get(row));
		}

		Object res = getObject(columnIndex);
		if (res == null)
		{
			wasNull = true;
			return null;
		}
		else
		{
			return res.toString();
		}
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return false;
		}
		
		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.BOOLEAN))
		{
			return ((BitVector) resultVector.getVector(columnIndex - 1)).get(row) == 1;
		}

		Object res = getObject(columnIndex);
		if (res instanceof Number)
		{
			return ((Number)res).byteValue() == 1;
		}

		return Boolean.parseBoolean(getObject(columnIndex).toString());
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return 0;
		}
		
		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.TINYINT))
		{
			return ((TinyIntVector)resultVector.getVector(columnIndex - 1)).get(row);
		}

		Object res = getObject(columnIndex);
		if (res instanceof Number)
		{
			return ((Number)res).byteValue();
		}

		return Byte.parseByte(getObject(columnIndex).toString());
	}

	public short getUint8(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return 0;
		}

		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.UTINYINT))
		{
			// make byte appear unsigned with & 0xFF
			return (short)(((UInt1Vector)resultVector.getVector(columnIndex - 1)).get(row) & 0xFF);
		}

		Object res = getObject(columnIndex);
		if (res instanceof Number)
		{
			return ((Number)res).shortValue();
		}

		return Short.parseShort(getObject(columnIndex).toString());
	}

	public int getUint16(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return 0;
		}

		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.USMALLINT))
		{
			return ((UInt2Vector)resultVector.getVector(columnIndex - 1)).get(row);
		}

		Object res = getObject(columnIndex);
		if (res instanceof Number)
		{
			return ((Number)res).intValue();
		}

		return Integer.parseInt(getObject(columnIndex).toString());
	}

	public long getUint32(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return 0;
		}

		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.UINTEGER))
		{
			return ((UInt4Vector)resultVector.getVector(columnIndex - 1)).getValueAsLong(row);
		}

		Object res = getObject(columnIndex);
		if (res instanceof Number)
		{
			return ((Number)res).longValue();
		}

		return Long.parseLong(getObject(columnIndex).toString());
	}

	public BigInteger getUint64(int columnIndex) throws SQLException
	{
		check(columnIndex);

		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return null;
		}
		
		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.UBIGINT))
		{
			return ((UInt8Vector)resultVector.getVector(columnIndex - 1)).getObjectNoOverflow(row);
		}

		return new BigInteger(getObject(columnIndex).toString());
	}

	@Override
	public short getShort(int columnIndex) throws SQLException
	{
		check(columnIndex);

		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return 0;
		}
		
		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.SMALLINT))
		{
			return ((SmallIntVector)resultVector.getVector(columnIndex - 1)).get(row);
		}

		Object res = getObject(columnIndex);
		if (res instanceof Number)
		{
			return ((Number)res).shortValue();
		}

		return Short.parseShort(getObject(columnIndex).toString());
	}

	public BigInteger getHugeint(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return null;
		}

		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.HUGEINT))
		{
			return ((Decimal256Vector)resultVector.getVector(columnIndex - 1)).getObject(row).toBigInteger();
		}

		return new BigInteger(getObject(columnIndex).toString());
	}

	@Override
	public int getInt(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return 0;
		}

		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.INTEGER))
		{
			return ((IntVector)resultVector.getVector(columnIndex - 1)).get(row);
		}

		Object res = getObject(columnIndex);
		if (res instanceof Number)
		{
			return ((Number)res).intValue();
		}

		return Integer.parseInt(getObject(columnIndex).toString());
	}

	@Override
	public long getLong(int columnIndex) throws SQLException
	{
		check(columnIndex);

		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return 0;
		}
		
		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.BIGINT))
		{
			return ((BigIntVector)resultVector.getVector(columnIndex - 1)).get(row);
		}

		Object res = getObject(columnIndex);
		if (res instanceof Number)
		{
			return ((Number)res).longValue();
		}

		return Long.parseLong(getObject(columnIndex).toString());
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException
	{
		check(columnIndex);

		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return 0;
		}
		
		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.FLOAT))
		{
			return ((Float4Vector)resultVector.getVector(columnIndex - 1)).get(row);
		}

		Object res = getObject(columnIndex);
		if (res instanceof Number)
		{
			return ((Number)res).floatValue();
		}

		return Float.parseFloat(getObject(columnIndex).toString());
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException
	{
		check(columnIndex);

		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return 0;
		}
		
		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.DOUBLE))
		{
			return ((Float8Vector)resultVector.getVector(columnIndex - 1)).get(row);
		}

		Object res = getObject(columnIndex);
		if (res instanceof Number)
		{
			return ((Number)res).doubleValue();
		}

		return Double.parseDouble(getObject(columnIndex).toString());
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return null;
		}

		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.DATE))
		{
			return Date.valueOf(((DateMilliVector)resultVector.getVector(columnIndex - 1)).getObject(row).toLocalDate());
		}

		return Date.valueOf(getObject(columnIndex).toString());
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return null;
		}

		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.TIME))
		{
			//return Time.valueOf(((TimeMilliVector)resultVector.getVector(columnIndex - 1)).getObject(row).toLocalDate().format(timeFormat));
			return new Time(((TimeMilliVector)resultVector.getVector(columnIndex - 1)).get(row));
		}

		return Time.valueOf(getObject(columnIndex).toString());
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return null;
		}

		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP))
		{
			return Timestamp.valueOf(((TimeStampMicroVector)resultVector.getVector(columnIndex - 1)).getObject(row));
		}

		return Timestamp.valueOf(getObject(columnIndex).toString());
	}

	public OffsetDateTime getOffsetDateTime(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return null;
		}

		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE))
		{
			DuckDBTimestamp tsTz = new DuckDBTimestamp(((TimeStampMilliTZVector)resultVector.getVector(columnIndex - 1)).getObject(row));
			return OffsetDateTime.of(tsTz.toLocalDateTime(), ZoneOffset.UTC);
		}

		return OffsetDateTime.parse(getObject(columnIndex).toString());
	}

	public LocalDateTime getLocalDateTime(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return null;
		}

		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.TIMESTAMP))
		{
			return ((TimeStampMicroVector)resultVector.getVector(columnIndex - 1)).getObject(row);
		}

		return LocalDateTime.parse(getObject(columnIndex).toString());
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getString(String columnLabel) throws SQLException
	{
		return getString(findColumn(columnLabel));
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException
	{
		return getBoolean(findColumn(columnLabel));
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException
	{
		return getByte(findColumn(columnLabel));
	}

	@Override
	public short getShort(String columnLabel) throws SQLException
	{
		return getShort(findColumn(columnLabel));
	}

	@Override
	public int getInt(String columnLabel) throws SQLException
	{
		return getInt(findColumn(columnLabel));
	}

	@Override
	public long getLong(String columnLabel) throws SQLException
	{
		return getLong(findColumn(columnLabel));
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException
	{
		return getFloat(findColumn(columnLabel));
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException
	{
		return getDouble(findColumn(columnLabel));
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException
	{
		return getDate(findColumn(columnLabel));
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException
	{
		return getTime(findColumn(columnLabel));
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException
	{
		return getTimestamp(findColumn(columnLabel));
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearWarnings() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getCursorName() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		if (isClosed())
		{
			throw new SQLException("ResultSet was closed");
		}
		return meta;
	}

	private void check(int columnIndex) throws SQLException
	{
		if (isClosed())
		{
			throw new SQLException("ResultSet was closed");
		}

		if (columnIndex < 1 || columnIndex > meta.getColumnCount())
		{
			throw new SQLException("Column index out of bounds");
		}
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException
	{
		check(columnIndex);

		// Shortcut for empty result: Throw SQLException as there is no value
		if (resultVector.getRowCount() == 0)
		{
			throw new SQLException("No rows returned!");
		}
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return null;
		}
		
		wasNull = false;

		switch (meta.resultColumnDuckDBTypes.get(columnIndex - 1))
		{
			case BOOLEAN:
				return getBoolean(columnIndex);
			case TINYINT:
				return getByte(columnIndex);
			case SMALLINT:
				return getShort(columnIndex);
			case INTEGER:
				return getInt(columnIndex);
			case BIGINT:
				return getLong(columnIndex);
			case HUGEINT:
				return getHugeint(columnIndex);
			case UTINYINT:
				return getUint8(columnIndex);
			case USMALLINT:
				return getUint16(columnIndex);
			case UINTEGER:
				return getUint32(columnIndex);
			case UBIGINT:
				return getUint64(columnIndex);
			case FLOAT:
				return getFloat(columnIndex);
			case DOUBLE:
				return getDouble(columnIndex);
			case DECIMAL:
				return getBigDecimal(columnIndex);
			case VARCHAR, ENUM:
				return getString(columnIndex);
			case TIME:
				return getTime(columnIndex);
			case DATE:
				return getDate(columnIndex);
			case TIMESTAMP:
				return getTimestamp(columnIndex);
			case TIMESTAMP_WITH_TIME_ZONE:
				return getOffsetDateTime(columnIndex);
//			case INTERVAL:
//				return getLazyString(columnIndex);
			default:
				throw new SQLException("Not implemented type");
		}
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException
	{
		return getObject(findColumn(columnLabel));
	}

	@Override
	public int findColumn(String columnLabel) throws SQLException
	{
		if (isClosed())
		{
			throw new SQLException("ResultSet was closed");
		}
		for (int i = 0; i < meta.getColumnCount(); i++)
		{
			if (columnLabel.equals(meta.getColumnName(i + 1)))
			{
				return i + 1;
			}
		}

		throw new SQLException("Could not find column with label " + columnLabel);
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException
	{
		check(columnIndex);
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return null;
		}
		
		wasNull = false;
		
		if (isType(columnIndex, DuckDBColumnType.DECIMAL))
		{
			return ((Decimal256Vector)resultVector.getVector(columnIndex - 1)).getObject(row);
		}

		Object res = getObject(columnIndex);
		if (res instanceof Number)
		{
			return new BigDecimal(res.toString());
		}

		return new BigDecimal(getObject(columnIndex).toString());
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException
	{
		return getBigDecimal(findColumn(columnLabel));
	}

	@Override
	public boolean isBeforeFirst() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isAfterLast() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isFirst() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isLast() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void beforeFirst() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void afterLast() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean first() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean last() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getRow() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean absolute(int row) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean relative(int rows) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean previous() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException
	{
		if (direction != ResultSet.FETCH_FORWARD && direction != ResultSet.FETCH_UNKNOWN)
		{
			throw new SQLFeatureNotSupportedException();
		}
	}

	@Override
	public int getFetchDirection() throws SQLException
	{
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException
	{
		if (rows < 0)
		{
			throw new SQLException("Fetch size has to be >= 0");
		}
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchSize() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getType() throws SQLException
	{
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public int getConcurrency() throws SQLException
	{
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public boolean rowUpdated() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowInserted() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean rowDeleted() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void insertRow() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRow() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void deleteRow() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void refreshRow() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void cancelRowUpdates() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void moveToInsertRow() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void moveToCurrentRow() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Statement getStatement() throws SQLException
	{
		if (isClosed())
		{
			throw new SQLException("ResultSet was closed");
		}
		return stmt;
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();

	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();

	}

	@Override
	public int getHoldability() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return resultVector == null;
	}

	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getNString(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException
	{
		if (type == null)
		{
			throw new SQLException("type is null");
		}
		
		if (resultVector.getVector(columnIndex - 1).isNull(row))
		{
			wasNull = true;
			return null;
		}
		
		wasNull = false;
		
		DuckDBColumnType sqlType = meta.resultColumnDuckDBTypes.get(columnIndex - 1);
		if (type == BigDecimal.class)
		{
			if (sqlType == DuckDBColumnType.DECIMAL)
			{
				return type.cast(getBigDecimal(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to BigDecimal " + type.toString());
			}
		}
		else if (type == String.class)
		{
			if (sqlType == DuckDBColumnType.VARCHAR || sqlType == DuckDBColumnType.ENUM)
			{
				return type.cast(getString(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to String " + type.toString());
			}
		}
		else if (type == Boolean.class)
		{
			if (sqlType == DuckDBColumnType.BOOLEAN)
			{
				return type.cast(getBoolean(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to boolean " + type.toString());
			}
		}
		else if (type == Short.class)
		{
			if (sqlType == DuckDBColumnType.SMALLINT)
			{
				return type.cast(getShort(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to short " + type.toString());
			}
		}
		else if (type == Integer.class)
		{
			if (sqlType == DuckDBColumnType.INTEGER)
			{
				return type.cast(getInt(columnIndex));
			}
			else if (sqlType == DuckDBColumnType.SMALLINT)
			{
				return type.cast(getShort(columnIndex));
			}
			else if (sqlType == DuckDBColumnType.TINYINT)
			{
				return type.cast(getByte(columnIndex));
			}
			else if (sqlType == DuckDBColumnType.USMALLINT)
			{
				throw new SQLException("Can't convert value to integer " + type.toString());
				// return type.cast(getShort(columnIndex));
			}
			else if (sqlType == DuckDBColumnType.UTINYINT)
			{
				throw new SQLException("Can't convert value to integer " + type.toString());
				// return type.cast(getShort(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to integer " + type.toString());
			}
		}
		else if (type == Long.class)
		{
			if (sqlType == DuckDBColumnType.BIGINT)
			{
				return type.cast(getLong(columnIndex));
			}
			else if (sqlType == DuckDBColumnType.UINTEGER)
			{
				throw new SQLException("Can't convert value to long " + type.toString());
				// return type.cast(getLong(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to long " + type.toString());
			}
		}
		else if (type == Float.class)
		{
			if (sqlType == DuckDBColumnType.FLOAT)
			{
				return type.cast(getFloat(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to float " + type.toString());
			}
		}
		else if (type == Double.class)
		{
			if (sqlType == DuckDBColumnType.DOUBLE)
			{
				return type.cast(getDouble(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to float " + type.toString());
			}
		}
		else if (type == Date.class)
		{
			if (sqlType == DuckDBColumnType.DATE)
			{
				return type.cast(getDate(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to Date " + type.toString());
			}
		}
		else if (type == Time.class)
		{
			if (sqlType == DuckDBColumnType.TIME)
			{
				return type.cast(getTime(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to Time " + type.toString());
			}
		}
		else if (type == Timestamp.class)
		{
			if (sqlType == DuckDBColumnType.TIMESTAMP)
			{
				return type.cast(getTimestamp(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to Timestamp " + type.toString());
			}
		}
		else if (type == LocalDateTime.class)
		{
			if (sqlType == DuckDBColumnType.TIMESTAMP)
			{
				return type.cast(getLocalDateTime(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to LocalDateTime " + type.toString());
			}
		}
		else if (type == BigInteger.class)
		{
			if (sqlType == DuckDBColumnType.HUGEINT)
			{
				throw new SQLException("Can't convert value to BigInteger " + type.toString());
				// return type.cast(getLocalDateTime(columnIndex));
			}
			else if (sqlType == DuckDBColumnType.UBIGINT)
			{
				throw new SQLException("Can't convert value to BigInteger " + type.toString());
				// return type.cast(getLocalDateTime(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to BigInteger " + type.toString());
			}
		}
		else if (type == OffsetDateTime.class)
		{
			if (sqlType == DuckDBColumnType.TIMESTAMP_WITH_TIME_ZONE)
			{
				return type.cast(getOffsetDateTime(columnIndex));
			}
			else
			{
				throw new SQLException("Can't convert value to OffsetDateTime " + type.toString());
			}
		}
		else if (type == Blob.class)
		{
			throw new SQLException("Can't convert value to Blob " + type.toString());
		}
		else
		{
			throw new SQLException("Can't convert value to " + type + " " + type.toString());
		}
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException
	{
		return getObject(findColumn(columnLabel), type);
	}
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		// TODO Auto-generated method stub
		return false;
	}
}
