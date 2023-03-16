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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import duckdb_driver.pata.server.StatementParameter;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.UUID;

public class Execute implements PataCommand
{
	public static final String op = "Execute";
	public final UUID statementID;
	public final boolean autoCommit;
	public StatementParameter stmtParameter;
	
	public Execute(JsonParser jsonParser) throws Exception
	{
		ArrayList<String> parameterTypes = new ArrayList<String>();
		ArrayList<Object> parameters = new ArrayList<Object>();
		
		UUID tmpId = null;
		boolean tmpAutoCommit = true;
		
		// Process Command
		while(!jsonParser.isClosed())
		{
		    JsonToken jsonToken = jsonParser.nextToken();
		    
		    if(JsonToken.FIELD_NAME.equals(jsonToken)
			    	&& jsonParser.getCurrentName().equals("parameterTypes"))
			{	
		    	// Move on to array
		    	jsonToken = jsonParser.nextToken();
		    	jsonToken = jsonParser.nextToken();
		    	while(!JsonToken.END_ARRAY.equals(jsonToken))
		    	{
		    		parameterTypes.add(jsonParser.getValueAsString());
		    		jsonToken = jsonParser.nextToken();
		    	}
			}
		    
		    if(JsonToken.FIELD_NAME.equals(jsonToken)
		    	&& jsonParser.getCurrentName().equals("parameters"))
		    {	
		    	// Move on to array
		    	jsonToken = jsonParser.nextToken();
		    	jsonToken = jsonParser.nextToken();
		    	
		    	int arrayPos = 0;
		    	
		    	while(!JsonToken.END_ARRAY.equals(jsonToken))
		    	{
		    		switch(parameterTypes.get(arrayPos))
		    		{
						case "NULL":
						{
							parameters.add(null);
							break;
						}
			    		case "String":
			    		{
			    			parameters.add(jsonParser.getValueAsString());
			    			break;
			    		}
			    		case "Boolean":
			    		{
			    			parameters.add(jsonParser.getValueAsBoolean());
			    			break;
			    		}
			    		case "Byte":
			    		{
			    			parameters.add((byte) jsonParser.getValueAsInt());
			    			break;
			    		}
			    		case "Short":
			    		{
			    			parameters.add((short) jsonParser.getValueAsInt());
			    			break;
			    		}
			    		case "Int":
			    		{
			    			parameters.add(jsonParser.getValueAsInt());
			    			break;
			    		}
			    		case "Long":
			    		{
			    			parameters.add(jsonParser.getValueAsLong());
			    			break;
			    		}
			    		case "Float":
			    		{
			    			parameters.add((float) jsonParser.getValueAsDouble());
			    			break;
			    		}
			    		case "Decimal":
			    		{
			    			parameters.add(new BigDecimal(jsonParser.getValueAsString()));
			    			break;
			    		}
			    		case "Double":
			    		{
			    			parameters.add(jsonParser.getValueAsDouble());
			    			break;
			    		}
			    		case "Timestamp":
						case "LocalDateTime":
			    		{
			    			parameters.add(LocalDateTime.parse(jsonParser.getValueAsString()));
			    			break;
			    		}
						case "OffsetDateTime":
						{
							parameters.add(OffsetDateTime.parse(jsonParser.getValueAsString()));
							break;
						}
		    		}
		    		jsonToken = jsonParser.nextToken();
		    		arrayPos++;
		    	}
		    	stmtParameter = new StatementParameter(parameterTypes, parameters);
		    } 		    
		    
		    if(JsonToken.FIELD_NAME.equals(jsonToken)
			    	&& jsonParser.getCurrentName().equals("statementID"))
		    {	
		    	// Move on to field value
		    	jsonToken = jsonParser.nextToken();
		    	
		    	tmpId = UUID.fromString(jsonParser.getValueAsString());
		    }
		    
		    if(JsonToken.FIELD_NAME.equals(jsonToken)
			    	&& jsonParser.getCurrentName().equals("autoCommit"))
		    {	
		    	// Move on to field value
		    	jsonToken = jsonParser.nextToken();
		    	
		    	tmpAutoCommit = jsonParser.getValueAsBoolean();
		    }	
		    
		    if (JsonToken.END_OBJECT.equals(jsonToken))
		    {
		    	break;
		    }
		}
		
		this.statementID = tmpId;
		this.autoCommit = tmpAutoCommit;
		
		// We need an SQL String
		if (statementID == null)
		{
			throw new Exception();
		}
	}
	
	public Execute()
	{
		this.statementID = UUID.randomUUID();
		this.autoCommit = true;
	}
	
	public Execute(UUID statementID)
	{
		this.statementID = statementID;
		this.autoCommit = true;
	}	
	
	public Execute(UUID statementID, ArrayList<String> parameterTypes, ArrayList<Object> parameters)
	{
		this.statementID = statementID;
		this.stmtParameter = new StatementParameter(parameterTypes, parameters);
		this.autoCommit = true;
	}
	
	public Execute(UUID statementID, ArrayList<String> parameterTypes, ArrayList<Object> parameters, boolean autoCommit)
	{
		this.statementID = statementID;
		this.stmtParameter = new StatementParameter(parameterTypes, parameters);
		this.autoCommit = autoCommit;
	}
	
	@Override
	public String getOp()
	{
		return Execute.op;
	}

	@Override
	public ByteBuffer encodeCommand() throws JacksonException
	{
		ObjectMapper mapper = new ObjectMapper();	
		ObjectNode cmd = mapper.createObjectNode();
		ArrayNode types = mapper.createArrayNode();
		ArrayNode params = mapper.createArrayNode();
		
		ArrayList<String> parameterTypes = stmtParameter.parameterTypes();
		ArrayList<Object> parameters = stmtParameter.parameterValues();
		
		cmd.put("op", this.getOp());
		cmd.put("autoCommit", autoCommit);
		
		// Add parameterTypes as Array
		for(String str : parameterTypes)
		{
			types.add(str);
		}
		cmd.set("parameterTypes", types);
		
		// Add parameters
		int arrayPos = 0;
		for(Object pa : parameters)
		{
			switch(parameterTypes.get(arrayPos))
    		{
	    		case "String":
	    		{
	    			params.add((String)pa);
	    			break;
	    		}
	    		case "Boolean":
	    		{
	    			params.add((Boolean)pa);
	    			break;
	    		}
	    		case "Byte":
	    		{
	    			params.add((Byte)pa);
	    			break;
	    		}
	    		case "Short":
	    		{
	    			params.add((Short)pa);
	    			break;
	    		}
	    		case "Int":
	    		{
	    			params.add((Integer)pa);
	    			break;
	    		}
	    		case "Long":
	    		{
	    			params.add((Long)pa);
	    			break;
	    		}
	    		case "Float":
	    		{
	    			params.add((Float)pa);
	    			break;
	    		}
	    		case "Decimal":
	    		{
	    			params.add((BigDecimal)pa);
	    			break;
	    		}
	    		case "Double":
	    		{
	    			params.add((Double)pa);
	    			break;
	    		}
	    		case "Timestamp":
				{
					params.add(((Timestamp)pa).toLocalDateTime().toString());
					break;
				}
				case "LocalDateTime":
				case "OffsetDateTime":
	    		{
	    			params.add(pa.toString());
	    			break;
	    		}
    		}		
			arrayPos++;
		}
		cmd.set("parameters", params);
		cmd.put("statementID", statementID.toString());
		
		return ByteBuffer.wrap(mapper.writeValueAsBytes(cmd));
	}
}
