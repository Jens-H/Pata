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

package duckdb_driver.pata.responses;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class Connected implements PataResponse
{
	public static final String op = "Connected";
	
	public final int port;
	
	private Charset utf8 = Charset.forName("UTF-8");
	
	public Connected(JsonParser jsonParser) throws Exception
	{
		int tmpPort = 0;
		
		// Process Command
		while(!jsonParser.isClosed())
		{
		    JsonToken jsonToken = jsonParser.nextToken();
	
		    if(JsonToken.FIELD_NAME.equals(jsonToken)
		    	&& jsonParser.getCurrentName().equals("port"))
		    {	
		    	// Move on to field value
		    	jsonToken = jsonParser.nextToken();
		    	
		    	tmpPort = jsonParser.getValueAsInt();
		    	break;
		    }
		}
		
		port = tmpPort;
		
		// We need a port
		if (tmpPort == 0)
		{
			throw new Exception();
		}
	}
	
	public Connected(int port)
	{
		this.port = port;
	}
	
	public String getOp()
	{
		return op;
	}
	
	public ByteBuffer encodeResponse()
	{
		String response = "{\"op\":\"Connected\", \"port\":\"" + port + "\"}";
		return ByteBuffer.wrap(response.getBytes(utf8));
	}
}
