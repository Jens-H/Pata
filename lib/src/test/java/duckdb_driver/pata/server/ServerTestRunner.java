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

import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ServerTestRunner {

	static {
		try {
			Class.forName("org.duckdb.DuckDBDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}	
	
	public static void main(String[] args) {
		
		try
		{
			// DB vorbereiten
			Connection con = DriverManager.getConnection("jdbc:duckdb:");
			
			// Server starten
			Server s = new Server((DuckDBConnection) con, 41442);		
			s.startServer();
		} 
		catch (SQLException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
