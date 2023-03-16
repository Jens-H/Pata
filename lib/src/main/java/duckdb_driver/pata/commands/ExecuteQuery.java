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

import java.nio.ByteBuffer;
import java.util.UUID;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ExecuteQuery implements PataCommand
{
	public static final String op = "ExecuteQuery";
	public final String sql;
	public final UUID statementID;
	
	public ExecuteQuery(JsonParser jsonParser) throws Exception
	{
		String tmpSql = null;
		UUID tmpId = null;
		
		// Process Command
		while(!jsonParser.isClosed())
		{
		    JsonToken jsonToken = jsonParser.nextToken();

		    if(JsonToken.FIELD_NAME.equals(jsonToken)
		    	&& jsonParser.getCurrentName().equals("sqlString"))
		    {	
		    	// Move on to field value
		    	jsonToken = jsonParser.nextToken();
		    	
		    	tmpSql = jsonParser.getValueAsString();
		    	break;
		    }
		    if(JsonToken.FIELD_NAME.equals(jsonToken)
			    	&& jsonParser.getCurrentName().equals("statementID"))
		    {	
		    	// Move on to field value
		    	jsonToken = jsonParser.nextToken();
		    	
		    	tmpId = UUID.fromString(jsonParser.getValueAsString());
		    	break;
		    }
		}
		
		sql = tmpSql;
		this.statementID = tmpId;
		
		// We need an SQL String
		if (tmpSql == null)
		{
			throw new Exception();
		}
	}
	
	public ExecuteQuery(String sql)
	{
		this.sql = sql;
		this.statementID = UUID.randomUUID();
	}

	public ExecuteQuery(String sql, UUID statementID)
	{
		this.sql = sql;
		this.statementID = statementID;
	}
	
	@Override
	public String getOp()
	{
		return op;
	}

	@Override
	public ByteBuffer encodeCommand() throws JacksonException
	{
		ObjectMapper mapper = new ObjectMapper();	
		ObjectNode cmd = mapper.createObjectNode();
		
		cmd.put("op", ExecuteQuery.op);
		cmd.put("sqlString", sql);
		cmd.put("statementID", statementID.toString());
		
		return ByteBuffer.wrap(mapper.writeValueAsBytes(cmd));
	}
}
