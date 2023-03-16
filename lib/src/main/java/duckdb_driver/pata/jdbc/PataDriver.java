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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import duckdb_driver.pata.commands.Connect;
import duckdb_driver.pata.responses.Connected;
import duckdb_driver.pata.responses.PataResponse;
import duckdb_driver.pata.responses.ResponseDecoder;

public class PataDriver implements Driver
{
	static final String DUCKDB_READONLY_PROPERTY = "duckdb.read_only";
	static final Charset utf8 = Charset.forName("UTF-8");

	static
	{
		try 
		{
			DriverManager.registerDriver(new PataDriver());
		} 
		catch (SQLException e) 
		{
			e.printStackTrace();
		}
	}
	
	
	@Override
	public Connection connect(String url, Properties info) throws SQLException
	{
		if (!acceptsURL(url)) 
		{
			return null;
		}
		boolean read_only = false;
		if (info != null) 
		{
			String prop_val = info.getProperty(DUCKDB_READONLY_PROPERTY);
			if (prop_val != null) 
			{
				String prop_clean = prop_val.trim().toLowerCase();
				read_only = prop_clean.equals("1") || prop_clean.equals("true") || prop_clean.equals("yes");
			}
		}
		
		// Find port number from url
		String[] urlParts = url.trim().toLowerCase().split(":");
		String portString = urlParts[urlParts.length - 1]; 
		
		try 
		{
			SocketChannel controlSocketChannel = SocketChannel.open();
			controlSocketChannel.connect(new InetSocketAddress("localhost", Integer.parseInt(portString)));

			// Establish control connection
			controlSocketChannel.write(new Connect().encodeCommand());
			
			ByteBuffer response = ByteBuffer.allocate(1000);
			
			controlSocketChannel.read(response);
			
			response.position(0);
			
			PataResponse resp = ResponseDecoder.decodeBuffer(response);
			
			return new PataConnection(((Connected)resp).port, read_only);		
		} 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			throw new SQLException("No connection possible: " + e.getMessage());
		}
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException
	{
		return url.startsWith("jdbc:duckdb-pata:");
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
	{
		DriverPropertyInfo[] ret = {};
		return ret; // no properties
	}

	@Override
	public int getMajorVersion()
	{
		return 0;
	}

	@Override
	public int getMinorVersion()
	{
		return 1;
	}

	@Override
	public boolean jdbcCompliant()
	{
		return true; // Aber sowas von
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		throw new SQLFeatureNotSupportedException("no logger");
	}
}
