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
		driver (www.duckdb.org). DuckDB is licensed under the MIT License.*/

package duckdb_driver.pata.jdbc;

import com.fasterxml.jackson.core.JacksonException;
import org.apache.arrow.memory.RootAllocator;
import duckdb_driver.pata.commands.Commit;
import duckdb_driver.pata.commands.Rollback;
import duckdb_driver.pata.responses.Aborted;
import duckdb_driver.pata.responses.Committed;
import duckdb_driver.pata.responses.ResponseDecoder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class PataConnection implements Connection
{
	public RootAllocator allocator;
	public SocketChannel connectionSocketChannel;
	protected boolean autoCommit = true;
	protected boolean transactionRunning = false;
	boolean read_only = false;
	
	public PataConnection(int port, boolean read_only) throws SQLException
	{
		allocator = new RootAllocator(Long.MAX_VALUE);
		this.read_only = read_only;
		
		try 
		{
			connectionSocketChannel = SocketChannel.open();
			connectionSocketChannel.connect(new InetSocketAddress("localhost", port));
			connectionSocketChannel.configureBlocking(true);
		} 
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException
	{
		if (isClosed()) 
		{
			throw new SQLException("Connection was closed");
		}
		if (resultSetConcurrency == ResultSet.CONCUR_READ_ONLY && resultSetType == ResultSet.TYPE_FORWARD_ONLY) 
		{
			return new PataPreparedStatement(this);
		}
		throw new SQLFeatureNotSupportedException();
	}	

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException
	{
		if (isClosed()) 
		{
			throw new SQLException("Connection was closed");
		}
		if (resultSetConcurrency == ResultSet.CONCUR_READ_ONLY && resultSetType == ResultSet.TYPE_FORWARD_ONLY) 
		{
			return new PataPreparedStatement(this, sql);
		}
		throw new SQLFeatureNotSupportedException();
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
	public Statement createStatement() throws SQLException
	{
		return createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException
	{
		return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, 0);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String nativeSQL(String sql) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException
	{
		if (isClosed()) 
		{
			throw new SQLException("Connection was closed");
		}
		
		if (this.autoCommit != autoCommit) 
		{
			this.autoCommit = autoCommit;

			// A running transaction is committed if switched to auto-commit
			if (transactionRunning && autoCommit) 
			{
				this.commit();
			}
		}
	}

	@Override
	public boolean getAutoCommit() throws SQLException
	{
		if (isClosed()) {
			throw new SQLException("Connection was closed");
		}
		return this.autoCommit;
	}

	@Override
	public void commit() throws SQLException
	{
		Commit cmd = new Commit();
		try
		{
			int writeCnt = 0;
			ByteBuffer buf = cmd.encodeCommand();
			
			while (connectionSocketChannel.isOpen())
			{
				writeCnt = connectionSocketChannel.write(buf);
				
				// End-of-stream check
				if (writeCnt == 0)
					{ break;}
			}

			ByteBuffer response = ByteBuffer.allocate(1000);
			
			connectionSocketChannel.read(response);		
			response.position(0);
			
			Committed resp = (Committed)ResponseDecoder.decodeBuffer(response);
		} catch (JacksonException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		transactionRunning = false;
	}

	@Override
	public void rollback() throws SQLException
	{
		Rollback cmd = new Rollback();
		try
		{
			int writeCnt = 0;
			ByteBuffer buf = cmd.encodeCommand();
			
			while (connectionSocketChannel.isOpen())
			{
				writeCnt = connectionSocketChannel.write(buf);
				
				// End-of-stream check
				if (writeCnt == 0)
					{ break;}
			}

			ByteBuffer response = ByteBuffer.allocate(1000);
			
			connectionSocketChannel.read(response);		
			response.position(0);
			
			Aborted resp = (Aborted)ResponseDecoder.decodeBuffer(response);
		} catch (JacksonException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		transactionRunning = false;
	}

	@Override
	public void close() throws SQLException
	{
		try
		{
			connectionSocketChannel.close();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean isClosed() throws SQLException
	{
		return !connectionSocketChannel.isConnected();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException
	{
		return new PataDatabaseMetaData(this);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException
	{
		if (readOnly != this.read_only) {
			throw new SQLFeatureNotSupportedException("Can't change read-only status on connection level.");
		}
	}

	@Override
	public boolean isReadOnly() throws SQLException
	{
		return read_only;
	}

	@Override
	public void setCatalog(String catalog) throws SQLException
	{
	}

	@Override
	public String getCatalog() throws SQLException
	{
		return null;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException
	{
		if (level > TRANSACTION_REPEATABLE_READ) 
		{
			throw new SQLFeatureNotSupportedException();
		}
	}

	@Override
	public int getTransactionIsolation() throws SQLException
	{
		return TRANSACTION_REPEATABLE_READ;
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
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
	{
		return createStatement(resultSetType, resultSetConcurrency, 0);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException
	{
		return prepareStatement(sql, resultSetType, resultSetConcurrency, 0);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setHoldability(int holdability) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getHoldability() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Clob createClob() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Blob createBlob() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public NClob createNClob() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException
	{
		if (isClosed()) 
		{
			return false;
		}
		
		// run a query just to be sure
		Statement s = createStatement();
		ResultSet rs = s.executeQuery("SELECT 42");
		if (!rs.next() || rs.getInt(1) != 42) 
		{
			rs.close();
			s.close();
			return false;
		}
		rs.close();
		s.close();

		return true;
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException
	{
		throw new SQLClientInfoException();
	}

	@Override
	public void setClientInfo(Properties properties) throws SQLClientInfoException
	{
		throw new SQLClientInfoException();
	}

	@Override
	public String getClientInfo(String name) throws SQLException
	{
		throw new SQLClientInfoException();
	}

	@Override
	public Properties getClientInfo() throws SQLException
	{
		throw new SQLClientInfoException();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setSchema(String schema) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getSchema() throws SQLException
	{
		return "main";
	}

	@Override
	public void abort(Executor executor) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getNetworkTimeout() throws SQLException
	{
		throw new SQLFeatureNotSupportedException();
	}

}
