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

import com.fasterxml.jackson.core.JacksonException;
import org.duckdb.DuckDBConnection;
import duckdb_driver.pata.commands.CommandDecoder;
import duckdb_driver.pata.commands.Connect;
import duckdb_driver.pata.commands.Disconnect;
import duckdb_driver.pata.commands.PataCommand;
import duckdb_driver.pata.responses.Connected;
import duckdb_driver.pata.responses.PataResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


public class Server 
{
	private ServerSocketChannel svrChannel;
	
	private Charset utf8 = Charset.forName("UTF-8");
	
	private Map<UUID, DbConnection> connections = new HashMap<UUID, DbConnection>();
	
	private DuckDBConnection rootDBConnection;
	
	public Server(DuckDBConnection duckDBConnection, int port)
	{
		rootDBConnection = duckDBConnection;
		InetSocketAddress socketAddress = new InetSocketAddress("localhost", port);
		
		try 
		{
			svrChannel = ServerSocketChannel.open();
			svrChannel.socket().bind(socketAddress);
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void startServer()
	{
		while (true)
		{
			SocketChannel socketChannel;
			ByteBuffer receiveBuffer = ByteBuffer.allocate(100);
			
			try 
			{
				socketChannel = svrChannel.accept();
				socketChannel.read(receiveBuffer);
				
				ByteBuffer responseBuffer = processInput(receiveBuffer);
				
				socketChannel.write(responseBuffer);
			}
			catch (ClosedByInterruptException e)
			{
				e.printStackTrace();
			}
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}		
	}

	public void stopServer()
	{
		try
		{
			svrChannel.close();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	private ByteBuffer processInput(ByteBuffer inputBuffer)
	{
		PataCommand cmd;
		try
		{
			cmd = CommandDecoder.decodeBuffer(inputBuffer);
		
			switch (cmd.getOp())		
			{
				case Connect.op:
				{
					return connect();
				}
				case Disconnect.op:
				{
					return disconnect(cmd);
				}
			}
		} 
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ByteBuffer.wrap("ERROR: Unknown command".getBytes(utf8));
	}

	private ByteBuffer connect()
	{
		try
		{
			DbConnection con = new DbConnection((DuckDBConnection) rootDBConnection.duplicate());
			connections.put(con.getConnctionId(), con);
			
			Thread t = new Thread(con);
			con.setThread(t);
			t.start();
			
			PataResponse response = new Connected(con.getSocketPort());
			
			return response.encodeResponse();
		} 
		catch (SQLException | JacksonException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return ByteBuffer.wrap("ERROR: No Connection".getBytes(utf8));
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return ByteBuffer.wrap("ERROR: No Connection".getBytes(utf8));
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return ByteBuffer.wrap("ERROR: No Connection".getBytes(utf8));
		}
	}
	
	private ByteBuffer disconnect(PataCommand cmd)
	{
		DbConnection dbCon = connections.remove(((Disconnect)cmd).connectionID);
		
		dbCon.closeConnection();
		
		return ByteBuffer.wrap("Disconnected".getBytes(utf8));
	}
	
}
