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
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Prepared implements PataResponse
{
	public static final String op = "Prepared";
	public final String queryType;

	public Prepared(JsonParser jsonParser) throws Exception
	{
		String tmpQueryType = "";
		
		// Process Command
		while(!jsonParser.isClosed())
		{
		    JsonToken jsonToken = jsonParser.nextToken();
	
		    if(JsonToken.FIELD_NAME.equals(jsonToken)
		    	&& jsonParser.getCurrentName().equals("queryType"))
		    {	
		    	// Move on to field value
		    	jsonToken = jsonParser.nextToken();
		    	
		    	tmpQueryType = jsonParser.getValueAsString();
		    	break;
		    }
		}
		
		queryType = tmpQueryType;
		
		// We need a type
		if (tmpQueryType == "")
		{
			throw new Exception();
		}
	}
	
	public Prepared(String queryType)
	{
		this.queryType = queryType;
	}
	
	@Override
	public String getOp()
	{
		return op;
	}

	@Override
	public ByteBuffer encodeResponse() throws JacksonException
	{
		ObjectMapper mapper = new ObjectMapper();	
		ObjectNode cmd = mapper.createObjectNode();
		
		cmd.put("op", Prepared.op);
		cmd.put("queryType", queryType);
		
		return ByteBuffer.wrap(mapper.writeValueAsBytes(cmd));
	}

}
