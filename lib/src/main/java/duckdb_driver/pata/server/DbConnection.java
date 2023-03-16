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

import duckdb_driver.pata.responses.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.*;
import duckdb_driver.pata.commands.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.UUID;

public class DbConnection implements Runnable
{
	private UUID connctionId;
	private DuckDBConnection jdbcDbConnection;
	private DuckDBPreparedStatement stmt;
	private ServerSocketChannel svrChannel;
	private Thread thread;

	private Charset utf8 = Charset.forName("UTF-8");
	
	public DbConnection(DuckDBConnection jdbcDbConnection)
	{
		this.connctionId = UUID.randomUUID();
		this.jdbcDbConnection = jdbcDbConnection;
		
		try 
		{
			svrChannel = ServerSocketChannel.open();
			svrChannel.socket().bind(new InetSocketAddress("localhost", 0));
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public UUID getConnctionId()
	{
		return connctionId;
	}

	public DuckDBConnection getRootDBConnection()
	{
		return jdbcDbConnection;
	}
	
	public int getSocketPort()
	{
		try
		{
			return ((InetSocketAddress)svrChannel.getLocalAddress()).getPort();
		} 
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public void run()
	{
		SocketChannel socketChannel;
		ByteBuffer receiveBuffer;
		try 
		{
			socketChannel = svrChannel.accept();
			socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
			socketChannel.configureBlocking(true);
			int readCnt = 0;
		
			while (socketChannel.isOpen())
			{
				receiveBuffer = ByteBuffer.allocate(10000);
				readCnt = socketChannel.read(receiveBuffer);
				
				// End-of-stream check
				if (readCnt == -1)
					{ break;}
				
				processInput(receiveBuffer, socketChannel);
			}
		}
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	private void processInput(ByteBuffer inputBuffer, SocketChannel socketChannel)
	{
		PataCommand cmd;
		try
		{
			cmd = CommandDecoder.decodeBuffer(inputBuffer);
	
			switch (cmd.getOp())		
			{
				case ExecuteSql.op:
				{
					executeSql(cmd, socketChannel);
					break;
				}
				case ExecuteQuery.op: // TODO remove, replaced by Execute
				{
					executeQuery(cmd, socketChannel);
					break;
				}
				case PrepareSql.op:
				{
					prepareSql(cmd, socketChannel);
					break;
				}
				case ExecuteUpdate.op:
				case Execute.op:
				{
					execute(cmd, socketChannel);
					break;
				}
				case Commit.op:
				{
					commit(cmd, socketChannel);
					break;
				}
				case Rollback.op:
				{
					rollback(cmd, socketChannel);
					break;
				}	
				default:
				socketChannel.write(ByteBuffer.wrap(("ERROR: Unknown command" + cmd.getOp()).getBytes(utf8)));	
			}
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}
	
	public void closeConnection()
	{
		thread.interrupt();
	}

	public void setThread(Thread thread)
	{
		this.thread = thread;
	}
	
	private void executeSql(PataCommand cmd, SocketChannel socketChannel)
	{
		boolean result = false;
		
		try
		{
			result = jdbcDbConnection.createStatement().execute(((ExecuteSql)cmd).sql);
		} 
		catch (SQLException e)
		{
			ExceptionRaised ex = new ExceptionRaised(e);
			try
			{
				socketChannel.write(ex.encodeResponse());
			}
			catch (Exception exc)
			{
				throw new RuntimeException(exc);
			}
		}
		
		String resp = "RESULT: " + result;
		
		try
		{
			socketChannel.write(ByteBuffer.wrap(resp.getBytes(utf8)));
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void executeQuery(PataCommand cmd, SocketChannel socketChannel)
	{
		// Parameter zu stmt hinzufügen
		try(DuckDBResultSet rs = (DuckDBResultSet)jdbcDbConnection.createStatement().executeQuery(((ExecuteQuery)cmd).sql);
			ArrowResultSet ars = new ArrowResultSet(rs);
			ArrowStreamWriter wrt = ars.getArrowStreamWriter(socketChannel);)
		{
			wrt.writeBatch();
			wrt.end();
		} 
		catch (Exception e)
		{
			ExceptionRaised ex = new ExceptionRaised(e);
			try
			{
				socketChannel.write(ex.encodeResponse());
			}
			catch (Exception exc)
			{
				throw new RuntimeException(exc);
			}
		}
	}
	
	private void prepareSql(PataCommand cmd, SocketChannel socketChannel)
	{		
		try
		{
			stmt = (DuckDBPreparedStatement) jdbcDbConnection.prepareStatement(((PrepareSql)cmd).sql);

			// Set return type, and handle case when there is no MetaData
			StatementReturnType returnType;
			try
			{
				returnType = ((DuckDBResultSetMetaData) stmt.getMetaData()).getReturnType();
			}
			catch (SQLException e)
			{
				returnType = StatementReturnType.NOTHING;
			}

			Prepared prep = new Prepared(returnType.toString());
			
			socketChannel.write(prep.encodeResponse());
		}
		catch (Exception e)
		{
			ExceptionRaised ex = new ExceptionRaised(e);
			try
			{
				socketChannel.write(ex.encodeResponse());
			}
			catch (Exception exc)
			{
				throw new RuntimeException(exc);
			}
		}
	}
	
	private void execute(PataCommand cmd, SocketChannel socketChannel)
	{
		try
		{
			setStatementParameters(((Execute)cmd).stmtParameter);
			jdbcDbConnection.setAutoCommit(((Execute)cmd).autoCommit);
			DuckDBResultSetMetaData meta;
			Result res = null;

			switch (cmd.getOp()) {
				case Execute.op:
					stmt.execute();
					meta= (DuckDBResultSetMetaData)stmt.getMetaData();
					res = new Result(meta.getReturnType(), stmt);
					break;
				case ExecuteUpdate.op:
					res = new Result(stmt.executeUpdate());
					break;
			}

			socketChannel.write(res.encodeResponse().flip());				
		}
		catch (Exception e)
		{
			ExceptionRaised ex = new ExceptionRaised(e);
			try
			{
				socketChannel.write(ex.encodeResponse());
			}
			catch (Exception exc)
			{
				throw new RuntimeException(exc);
			}
		}
	}

	private void executeUpdate(PataCommand cmd, SocketChannel socketChannel)
	{
		try
		{
			setStatementParameters(((Execute)cmd).stmtParameter);
			jdbcDbConnection.setAutoCommit(((Execute)cmd).autoCommit);
			stmt.executeUpdate();

			DuckDBResultSetMetaData meta = (DuckDBResultSetMetaData)stmt.getMetaData();
			Result res = new Result(meta.getReturnType(), stmt);

			socketChannel.write(res.encodeResponse().flip());
		}
		catch (Exception e)
		{
			ExceptionRaised ex = new ExceptionRaised(e);
			try
			{
				socketChannel.write(ex.encodeResponse());
			}
			catch (Exception exc)
			{
				throw new RuntimeException(exc);
			}
		}
	}
	
	private void commit(PataCommand cmd, SocketChannel socketChannel)
	{
		try
		{
			jdbcDbConnection.commit();
			Committed res = new Committed();
			
			int writeCnt = 0;
			ByteBuffer buf = res.encodeResponse();
			
			while (socketChannel.isOpen())
			{
				writeCnt = socketChannel.write(buf);
				
				// End-of-stream check
				if (buf.remaining() == 0)
					{ break;}
			}
		}
		catch (Exception e)
		{
			ExceptionRaised ex = new ExceptionRaised(e);
			try
			{
				socketChannel.write(ex.encodeResponse());
			}
			catch (Exception exc)
			{
				throw new RuntimeException(exc);
			}
		}
	}

	private void rollback(PataCommand cmd, SocketChannel socketChannel)
	{
		try
		{
			jdbcDbConnection.rollback();;
			Aborted res = new Aborted();
			
			int writeCnt = 0;
			ByteBuffer buf = res.encodeResponse();
			
			while (socketChannel.isOpen())
			{
				writeCnt = socketChannel.write(buf);
				
				// End-of-stream check
				if (buf.remaining() == 0)
					{ break;}
			}
		}
		catch (Exception e)
		{
			ExceptionRaised ex = new ExceptionRaised(e);
			try
			{
				socketChannel.write(ex.encodeResponse());
			}
			catch (Exception exc)
			{
				throw new RuntimeException(exc);
			}
		}
	}	
	
	private void setStatementParameters(StatementParameter stmtParams) throws SQLException
	{
		// Special case all params are NULL
		if (stmtParams.parameterValues().size() == 0)
		{
			for (int i = 0; i < stmtParams.parameterTypes().size(); i++)
			{
				stmt.setNull(i + 1, 0);
			}
		}

		int arrayPos = 0;
		for(Object pa : stmtParams.parameterValues())
		{
			switch(stmtParams.parameterTypes().get(arrayPos))
    		{
	    		case "String":
	    		{
	    			stmt.setObject(arrayPos + 1, pa);
	    			break;
	    		}
	    		case "Boolean":
	    		{
	    			stmt.setObject(arrayPos + 1, pa);
	    			break;
	    		}
	    		case "Byte":
	    		{
	    			stmt.setObject(arrayPos + 1, pa);
	    			break;
	    		}
	    		case "Short":
	    		{
	    			stmt.setObject(arrayPos + 1, pa);
	    			break;
	    		}
	    		case "Int":
	    		{
	    			stmt.setObject(arrayPos + 1, pa);
	    			break;
	    		}
	    		case "Long":
	    		{
	    			stmt.setObject(arrayPos + 1, pa);
	    			break;
	    		}
	    		case "Float":
	    		{
	    			stmt.setObject(arrayPos + 1, pa);
	    			break;
	    		}
	    		case "Decimal":
	    		{
	    			stmt.setObject(arrayPos + 1, pa);
	    			break;
	    		}
	    		case "Double":
	    		{
	    			stmt.setObject(arrayPos + 1, pa);
	    			break;
	    		}
	    		case "Timestamp":
				case "LocalDateTime":
	    		{
	    			stmt.setObject(arrayPos + 1, pa);
	    			break;
	    		}
				case "OffsetDateTime":
				{
					stmt.setObject(arrayPos + 1, pa);
				}
    		}		
			arrayPos++;
		}		
	}
}
