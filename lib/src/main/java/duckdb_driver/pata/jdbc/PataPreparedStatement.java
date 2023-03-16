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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.UUID;

import duckdb_driver.pata.commands.ExecuteUpdate;
import duckdb_driver.pata.responses.*;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.duckdb.StatementReturnType;

import duckdb_driver.pata.commands.Execute;
import duckdb_driver.pata.commands.PrepareSql;

public class PataPreparedStatement implements PreparedStatement
{
	PataConnection conn;
	UUID statementId;
	String preparedSql;

	private PataResultSet select_result = null;
	private int update_result = 0;
	private ArrayList<String> parameterTypes = new ArrayList<String>();
	private ArrayList<Object> parameters = new ArrayList<Object>();
	private PataResultSetMetaData meta = null;
	private boolean returnsChangedRows = false;
	private boolean returnsNothing = false;
	private boolean returnsResultSet = false;

	public PataPreparedStatement(PataConnection conn) throws SQLException
	{
		if (conn == null)
		{
			throw new SQLException("connection parameter cannot be null");
		}
		this.conn = conn;
		this.statementId = UUID.randomUUID();
	}

	public PataPreparedStatement(PataConnection conn, String sql) throws SQLException
	{
		if (conn == null)
		{
			throw new SQLException("connection parameter cannot be null");
		}
		if (sql == null)
		{
			throw new SQLException("sql query parameter cannot be null");
		}
		this.conn = conn;
		this.statementId = UUID.randomUUID();
		prepare(sql);
	}
	
	private void prepare(String sql) throws SQLException {
		if (isClosed()) 
		{
			throw new SQLException("Statement was closed");
		}
		if (sql == null) 
		{
			throw new SQLException("sql query parameter cannot be null");
		}

		meta = null;
		parameterTypes = null;
		parameters = null;

		select_result = null;
		update_result = 0;

		try 
		{
			// Send Prepare cmd
			PrepareSql cmd = new PrepareSql(sql);
			statementId = cmd.statementID;
			
			conn.connectionSocketChannel.write(cmd.encodeCommand());
			ByteBuffer response = ByteBuffer.allocate(1000);
					
			int readCnt = 0;
			
			readCnt = conn.connectionSocketChannel.read(response);
			
			response.position(0);
			
			PataResponse resp = ResponseDecoder.decodeBuffer(response);

			if (resp instanceof ExceptionRaised)
			{
				throw new SQLException(((ExceptionRaised)resp).exceptionMsg);
			}

			String query_type;
			if (resp.getOp() == Prepared.op)
			{
				query_type = ((Prepared)resp).queryType;
				returnsChangedRows = query_type.equals(StatementReturnType.CHANGED_ROWS.toString());
				
				parameterTypes = new ArrayList<String>();
				parameters = new ArrayList<Object>();
				
				preparedSql = sql;
				//meta = DuckDBNative.duckdb_jdbc_meta(stmt_ref); TODO
			}
			else
			{
				throw new SQLException("Response: no Prepared!");
			}
		}
		catch (Exception e) 
		{
			close();
			throw new SQLException(e);
		}
	}
	
	@Override
	public boolean execute() throws SQLException
	{
		if (isClosed()) 
		{
			throw new SQLException("Statement was closed");
		}
		if (preparedSql == null) 
		{
			throw new SQLException("Prepare something first");
		}

		select_result = null;
		
		try 
		{
			// Send Execute cmd
			Execute cmd = new Execute(statementId, parameterTypes, parameters, conn.autoCommit);
			
			conn.connectionSocketChannel.write(cmd.encodeCommand());
			ByteBuffer response = ByteBuffer.allocate(10_000_000);
			
			int readCnt = 0;
			readCnt = conn.connectionSocketChannel.read(response);
//			System.out.println("Execute - readCnt: " + readCnt);
					
			response.position(0);

			PataResponse pataResponse =	ResponseDecoder.decodeBuffer(response);

			if (pataResponse instanceof ExceptionRaised)
			{
				throw new SQLException(((ExceptionRaised)pataResponse).exceptionMsg);
			}

			Result resp = (Result)pataResponse;

			
			switch (resp.queryType)
			{
				case QUERY_RESULT:
					// Read potential rest of response from buffer
					while (readCnt < resp.arrowSize + resp.jsonSize + 50)
					{
						resp.arrowBuffer.position(readCnt);
						readCnt = readCnt + conn.connectionSocketChannel.read(response);
						System.out.println("Execute - readCnt: " + readCnt);
					}
					
					resp.arrowBuffer.position(resp.jsonSize + 50);
					
					byte[] arrowArray = new byte[resp.arrowBuffer.limit() - resp.arrowBuffer.position()];
					resp.arrowBuffer.get(resp.arrowBuffer.position(), arrowArray);
					
					ArrowStreamReader ar = new ArrowStreamReader(new ByteArrayInputStream(arrowArray), conn.allocator);
					
					select_result = new PataResultSet(this, ar);
					
					this.meta = (PataResultSetMetaData) select_result.getMetaData();
					
					returnsChangedRows = false;
					returnsNothing = false;
					returnsResultSet = true;
					break;
				case CHANGED_ROWS:
					resp.updateCount = update_result;
					
					returnsChangedRows = true;
					returnsNothing = false;
					returnsResultSet = true;
					break;
				case NOTHING:
					returnsChangedRows = false;
					returnsNothing = true;
					returnsResultSet = true;
					break;
			}
		}
		catch (Exception e) 
		{
			close();
			throw new SQLException(e);
		}	
		return !returnsChangedRows;
	}	
	
	@Override
	public ResultSet executeQuery(String sql) throws SQLException
	{
		prepare(sql);
		return executeQuery();
	}

	@Override
	public int executeUpdate(String sql) throws SQLException
	{
		if (isClosed())
		{
			throw new SQLException("Statement was closed");
		}
		prepare(sql);
		if (preparedSql == null)
		{
			throw new SQLException("Prepare something first");
		}
		return executeUpdate();
	}

	@Override
	public void close() throws SQLException
	{
		// TODO Auto-generated method stub
		// send close connection cmd
		statementId = null;
		preparedSql = null;
		conn = null;
	}

	@Override
	public int getMaxFieldSize() throws SQLException
	{
		return 0;
	}

	@Override
	public void setMaxFieldSize(int max) throws SQLException
	{
		// TODO Auto-generated method stub

	}

	@Override
	public int getMaxRows() throws SQLException
	{
		return 0;
	}

	@Override
	public void setMaxRows(int max) throws SQLException
	{
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException
	{
	}

	@Override
	public int getQueryTimeout() throws SQLException
	{
		return 0;
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void cancel() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException
	{
	}

	@Override
	public void setCursorName(String name) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql) throws SQLException
	{
		prepare(sql);
		return execute();
	}

	@Override
	public ResultSet getResultSet() throws SQLException
	{
		if (isClosed()) {
			throw new SQLException("Statement was closed");
		}
		if (preparedSql == null) {
			throw new SQLException("Prepare something first");
		}

		if (returnsChangedRows) {
			return null;
		}
		return select_result;
	}

	@Override
	public int getUpdateCount() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean getMoreResults() throws SQLException
	{
		return false;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException
	{
		if (direction == ResultSet.FETCH_FORWARD)
		{
			return;
		}
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getFetchDirection() throws SQLException
	{
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(int rows) throws SQLException
	{
	}

	@Override
	public int getFetchSize() throws SQLException
	{
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException
	{
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public int getResultSetType() throws SQLException
	{
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public void addBatch(String sql) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearBatch() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int[] executeBatch() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Connection getConnection() throws SQLException
	{
		return conn;
	}

	@Override
	public boolean getMoreResults(int current) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getResultSetHoldability() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return conn == null;
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isPoolable() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void closeOnCompletion() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException
	{
		return false;
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
	public ResultSet executeQuery() throws SQLException
	{
		if (returnsChangedRows) 
		{
			throw new SQLException("executeQuery() can only be used with SELECT queries");
		}
		execute();
		return getResultSet();
	}

	@Override
	public int executeUpdate() throws SQLException
	{
		select_result = null;

		try
		{
			// Send Execute cmd
			ExecuteUpdate cmd = new ExecuteUpdate(statementId, parameterTypes, parameters, conn.autoCommit);

			conn.connectionSocketChannel.write(cmd.encodeCommand());
			ByteBuffer response = ByteBuffer.allocate(10_000_000);

			int readCnt = 0;
			readCnt = conn.connectionSocketChannel.read(response);
//			System.out.println("Execute - readCnt: " + readCnt);

			response.position(0);

			PataResponse pataResponse =	ResponseDecoder.decodeBuffer(response);

			if (pataResponse instanceof ExceptionRaised)
			{
				throw new SQLException(((ExceptionRaised)pataResponse).exceptionMsg);
			}

			Result resp = (Result)pataResponse;

			switch (resp.queryType)
			{
				case CHANGED_ROWS:
					update_result = resp.updateCount;

					returnsChangedRows = true;
					returnsNothing = false;
					returnsResultSet = true;
					break;
				case NOTHING:
					returnsChangedRows = false;
					returnsNothing = true;
					returnsResultSet = true;
					break;
			}
		}
		catch (Exception e)
		{
			close();
			throw new SQLException(e);
		}
		return update_result;
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException
	{
		setObject(parameterIndex, null, Types.NULL);
	}

	@Override
	public void setBoolean(int parameterIndex, boolean x) throws SQLException
	{
		setObject(parameterIndex, x, Types.BOOLEAN);
	}

	@Override
	public void setByte(int parameterIndex, byte x) throws SQLException
	{
		setObject(parameterIndex, x, Types.TINYINT);
	}

	@Override
	public void setShort(int parameterIndex, short x) throws SQLException
	{
		setObject(parameterIndex, x, Types.SMALLINT);
	}

	@Override
	public void setInt(int parameterIndex, int x) throws SQLException
	{
		setObject(parameterIndex, x, Types.INTEGER);
	}

	@Override
	public void setLong(int parameterIndex, long x) throws SQLException
	{
		setObject(parameterIndex, x, Types.BIGINT);
	}

	@Override
	public void setFloat(int parameterIndex, float x) throws SQLException
	{
		setObject(parameterIndex, x, Types.FLOAT);
	}

	@Override
	public void setDouble(int parameterIndex, double x) throws SQLException
	{
		setObject(parameterIndex, x, Types.DOUBLE);
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
	{
		setObject(parameterIndex, x, Types.DECIMAL);
	}

	@Override
	public void setString(int parameterIndex, String x) throws SQLException
	{
		setObject(parameterIndex, x, Types.VARCHAR);
	}

	@Override
	public void setBytes(int parameterIndex, byte[] x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setDate(int parameterIndex, Date x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTime(int parameterIndex, Time x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
	{
		setObject(parameterIndex, x);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void clearParameters() throws SQLException
	{
		parameterTypes = new ArrayList<String>();
		parameters = new ArrayList<Object>();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
	{
		if (x == null)
		{
			targetSqlType = Types.NULL;
		}

		Object parameterObj = null;
		String parameterTypeStr = null;

		switch (targetSqlType)
		{
			case Types.NULL:
			{
				parameterTypeStr = "NULL";
				parameterObj = null;
			}
			break;
		case Types.BOOLEAN:
		case Types.BIT:
			if (x instanceof Boolean)
			{
				parameterObj = x;
			} 
			else if (x instanceof Number)
			{
				parameterObj =((Number) x).byteValue() == 1;
			} 
			else if (x instanceof String)
			{
				parameterObj = Boolean.parseBoolean((String) x);
			} 
			else
			{
				throw new SQLException("Can't convert value to boolean " + x.getClass().toString());
			}
			parameterTypeStr = "Boolean";
			break;
		case Types.TINYINT:
			if (x instanceof Byte)
			{
				parameterObj = x;
			} 
			else if (x instanceof Number)
			{
				parameterObj = ((Number) x).byteValue();
			} 
			else if (x instanceof String)
			{
				parameterObj = Byte.parseByte((String) x);
			} 
			else if (x instanceof Boolean)
			{
				parameterObj = (byte) (((Boolean) x) ? 1 : 0);
			} 
			else
			{
				throw new SQLException("Can't convert value to byte " + x.getClass().toString());
			}
			parameterTypeStr = "Byte";
			break;
		case Types.SMALLINT:
			if (x instanceof Short)
			{
				parameterObj = x;
			} 
			else if (x instanceof Number)
			{
				parameterObj = ((Number) x).shortValue();
			} 
			else if (x instanceof String)
			{
				parameterObj = Short.parseShort((String) x);
			} 
			else if (x instanceof Boolean)
			{
				parameterObj = (short) (((Boolean) x) ? 1 : 0);
			} 
			else
			{
				throw new SQLException("Can't convert value to short " + x.getClass().toString());
			}
			parameterTypeStr = "Short";
			break;
		case Types.INTEGER:
			if (x instanceof Integer)
			{
				parameterObj = x;
			} 
			else if (x instanceof Number)
			{
				parameterObj = ((Number) x).intValue();
			} 
			else if (x instanceof String)
			{
				parameterObj = Integer.parseInt((String) x);
			} 
			else if (x instanceof Boolean)
			{
				parameterObj = ((Boolean) x) ? 1 : 0;
			} 
			else
			{
				throw new SQLException("Can't convert value to int " + x.getClass().toString());
			}
			parameterTypeStr = "Int";
			break;
		case Types.BIGINT:
			if (x instanceof Long)
			{
				parameterObj = x;
			} 
			else if (x instanceof Number)
			{
				parameterObj = ((Number) x).longValue();
			} 
			else if (x instanceof String)
			{
				parameterObj = Long.parseLong((String) x);
			} 
			else if (x instanceof Boolean)
			{
				parameterObj = (long) (((Boolean) x) ? 1 : 0);
			} 
			else
			{
				throw new SQLException("Can't convert value to long " + x.getClass().toString());
			}
			parameterTypeStr = "Long";
			break;
		case Types.REAL:
		case Types.FLOAT:
			if (x instanceof Float)
			{
				parameterObj = x;
			} 
			else if (x instanceof Number)
			{
				parameterObj = ((Number) x).floatValue();
			} 
			else if (x instanceof String)
			{
				parameterObj = Float.parseFloat((String) x);
			} 
			else if (x instanceof Boolean)
			{
				parameterObj = (float) (((Boolean) x) ? 1 : 0);
			} 
			else
			{
				throw new SQLException("Can't convert value to float " + x.getClass().toString());
			}
			parameterTypeStr = "Float";
			break;
		case Types.DECIMAL:
			if (x instanceof BigDecimal)
			{
				parameterObj = x;
			} 
			else if (x instanceof Double)
			{
				parameterObj = BigDecimal.valueOf((Double) x);
			} 
			else if (x instanceof String)
			{
				parameterObj = new BigDecimal((String) x);
			} 
			else
			{
				throw new SQLException("Can't convert value to double " + x.getClass().toString());
			}
			parameterTypeStr = "Decimal";
			break;
		case Types.NUMERIC:
		case Types.DOUBLE:
			if (x instanceof Double)
			{
				parameterObj = x;
			} 
			else if (x instanceof Number)
			{
				parameterObj = ((Number) x).doubleValue();
			} 
			else if (x instanceof String)
			{
				parameterObj = Double.parseDouble((String) x);
			} 
			else if (x instanceof Boolean)
			{
				parameterObj = (double) (((Boolean) x) ? 1 : 0);
			} 
			else
			{
				throw new SQLException("Can't convert value to double " + x.getClass().toString());
			}
			parameterTypeStr = "Double";
			break;
		case Types.CHAR:
		case Types.LONGVARCHAR:
		case Types.VARCHAR:
			if (x instanceof String)
			{
				parameterObj = x;
			} 
			else
			{
				parameterObj = x.toString();
			}
			parameterTypeStr = "String";
			break;
		case Types.DATE:
		case Types.TIME:
		case Types.TIMESTAMP:
		case Types.TIMESTAMP_WITH_TIMEZONE:
			if (x instanceof Timestamp)
			{
				parameterObj = x;
				parameterTypeStr = "Timestamp";
			} 
			else if (x instanceof LocalDateTime)
			{
				parameterObj = x;
				parameterTypeStr = "LocalDateTime";
			} 
			else if (x instanceof OffsetDateTime)
			{
				parameterObj = x;
				parameterTypeStr = "OffsetDateTime";
			} 
			else
			{
				throw new SQLException("Can't convert value to timestamp " + x.getClass().toString());
			}
			break;
		default:
			throw new SQLException("Unknown target type " + targetSqlType);
		}

		// Set values, fill gaps if necessary
		if (parameters.size() <= parameterIndex)
		{
			// Fill missing array slots with null
			while(0 < parameterIndex - parameters.size())
			{
				parameters.add(null);
				parameterTypes.add(null);
			}
		}
		parameters.set(parameterIndex - 1, parameterObj);
		parameterTypes.set(parameterIndex -1, parameterTypeStr);
	}

	@Override
	public void setObject(int parameterIndex, Object x) throws SQLException
	{
		int targetSqlType;

		if (x instanceof Boolean)
		{
			targetSqlType = Types.BOOLEAN;
		}
		else if (x instanceof Byte)
		{
			targetSqlType = Types.TINYINT;
		}
		else
		if (x instanceof Short)
		{
			targetSqlType = Types.SMALLINT;
		}
		else
		if (x instanceof Integer)
		{
			targetSqlType = Types.INTEGER;
		}
		else
		if (x instanceof Long)
		{
			targetSqlType = Types.BIGINT;
		}
		else
		if (x instanceof BigDecimal)
		{
			targetSqlType = Types.DECIMAL;
		}
		else
		if (x instanceof Float)
		{
			targetSqlType = Types.FLOAT;
		}
		else
		if (x instanceof Double)
		{
			targetSqlType = Types.DOUBLE;
		}
		else
		if (x instanceof java.util.Date)
		{
			targetSqlType = Types.DATE;
		}
		else
		if (x instanceof String)
		{
			targetSqlType = Types.VARCHAR;
		}
		else
		if (x instanceof Timestamp)
		{
			targetSqlType = Types.TIMESTAMP;
		}
		else
		if (x instanceof LocalDateTime)
		{
			targetSqlType = Types.TIMESTAMP;
		}
		else
		if (x instanceof OffsetDateTime)
		{
			targetSqlType = Types.TIMESTAMP_WITH_TIMEZONE;
		}
		else
		{
			throw new SQLException("Unknown object type");
		}

		setObject(parameterIndex, x, targetSqlType);
	}

	@Override
	public void addBatch() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setRef(int parameterIndex, Ref x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, Blob x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(int parameterIndex, Clob x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setArray(int parameterIndex, Array x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setURL(int parameterIndex, URL x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setRowId(int parameterIndex, RowId x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(int parameterIndex, NClob value) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException
	{
		setObject(parameterIndex, x, targetSqlType);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(int parameterIndex, Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNClob(int parameterIndex, Reader reader) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}
}
