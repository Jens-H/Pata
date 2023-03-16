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

package duckdb_driver.pata.commands;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class CommandDecoder
{
	private static Charset utf8 = Charset.forName("UTF-8");
	
	private static JsonFactory jsonFactory = new JsonFactory();
	
	private final static String connectStr = Connect.op;
	private final static String disconnectStr = Disconnect.op;
	private final static String executeSql = ExecuteSql.op;
	private final static String executeQuery = ExecuteQuery.op;
	private final static String prepareSql = PrepareSql.op;
	private final static String execute = Execute.op;
	private final static String executeUpdate = ExecuteUpdate.op;
	private final static String commit = Commit.op;
	private final static String rollback = Rollback.op;
	
	public static PataCommand decodeBuffer(ByteBuffer inputBuffer) throws Exception
	{
		inputBuffer.position(0);	
		CharBuffer inputChars = utf8.decode(inputBuffer);
		
		String commandStr = null;
		JsonParser jsonParser = null;
		
		try
		{
			jsonParser = jsonFactory.createParser(inputChars.toString());
			
			while(!jsonParser.isClosed())
			{
			    JsonToken jsonToken = jsonParser.nextToken();

			    if(JsonToken.FIELD_NAME.equals(jsonToken)
			    	&& jsonParser.getCurrentName().equals("op"))
			    {	
			    	// Move on to field value
			    	jsonToken = jsonParser.nextToken();
			    	
			    	commandStr = jsonParser.getValueAsString();
			    	break;
			    }
			}
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		// Decide which command it is
		switch (commandStr)		
		{
			case connectStr:
			{
				return new Connect();
			}
			case disconnectStr:
			{
				return new Disconnect(jsonParser);
			}
			case executeSql:
			{
				return new ExecuteSql(jsonParser);
			}
			case executeQuery:
			{
				return new ExecuteQuery(jsonParser);
			}
			case prepareSql:
			{
				return new PrepareSql(jsonParser);
			}
			case executeUpdate:
			{
				return new ExecuteUpdate(jsonParser);
			}
			case execute:
			{
				return new Execute(jsonParser);
			}
			case commit:
			{
				return new Commit();
			}
			case rollback:
			{
				return new Rollback();
			}
			default:
				throw new IllegalArgumentException("Unexpected value: " + inputChars.toString());
		}
	}
}
