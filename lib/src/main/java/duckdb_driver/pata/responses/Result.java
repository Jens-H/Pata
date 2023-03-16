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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBPreparedStatement;
import org.duckdb.DuckDBResultSet;
import org.duckdb.StatementReturnType;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import duckdb_driver.pata.server.ArrowResultSet;

public class Result implements PataResponse
{
	private static Charset utf8 = Charset.forName("UTF-8");
	
	private static JsonFactory jsonFactory = new JsonFactory();
	
	public static final String op = "Result";
	public final StatementReturnType queryType;
	public int updateCount;
	public Statement stmt;
	public int changedRows;
	public ByteBuffer arrowBuffer;
	
	public int arrowSize;
	public int jsonSize;
	
	@Override
	public String getOp()
	{
		return op;
	}
	
	public Result(StatementReturnType queryType, DuckDBPreparedStatement stmt)
	{
		this.queryType = queryType;
		this.stmt = stmt;
		try
		{
			this.changedRows = stmt.getUpdateCount();
		} catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	public Result(int changedRows)
	{
		this.queryType = StatementReturnType.CHANGED_ROWS;
		this.changedRows = changedRows;
	}

	public Result(ByteBuffer inputBuffer) throws Exception
	{
		inputBuffer.position(0);	
		
		// 50 bytes size msg
		CharBuffer sizeChars = utf8.decode(inputBuffer.slice(0, 50));
		
		JsonParser jsonParser = null;
		jsonParser = jsonFactory.createParser(sizeChars.toString());
					
		
		StatementReturnType tmpQueryType = null;
		
		// Process Command
		while(!jsonParser.isClosed())
		{
		    JsonToken jsonToken = jsonParser.nextToken();
	
		    if(JsonToken.FIELD_NAME.equals(jsonToken)
		    	&& jsonParser.getCurrentName().equals("jsonSize"))
		    {	
		    	// Move on to field value
		    	jsonToken = jsonParser.nextToken();
		    	jsonSize = Integer.parseInt(jsonParser.getValueAsString());
		    }
		    
		    if(JsonToken.FIELD_NAME.equals(jsonToken)
		    	&& jsonParser.getCurrentName().equals("arrowSize"))
		    {	
		    	// Move on to field value
		    	jsonToken = jsonParser.nextToken();
		    	arrowSize = Integer.parseInt(jsonParser.getValueAsString());
		    }
		    
		    if (JsonToken.END_OBJECT.equals(jsonToken))
		    {
		    	break;
		    }
		}

		// JSON Message
		CharBuffer jsonChars = utf8.decode(inputBuffer.slice(50, jsonSize));
		
		jsonParser = jsonFactory.createParser(jsonChars.toString());
		
		while(!jsonParser.isClosed())
		{
		    JsonToken jsonToken = jsonParser.nextToken();
		    
		    if(JsonToken.FIELD_NAME.equals(jsonToken)
		    	&& jsonParser.getCurrentName().equals("queryType"))
		    {	
		    	// Move on to field value
		    	jsonToken = jsonParser.nextToken();
		    	
		    	tmpQueryType = StatementReturnType.valueOf(jsonParser.getValueAsString());
		    }
		    
		    if(JsonToken.FIELD_NAME.equals(jsonToken)
			    	&& jsonParser.getCurrentName().equals("updateCount"))
		    {	
		    	// Move on to field value
		    	jsonToken = jsonParser.nextToken();
		    	
		    	updateCount = Integer.parseInt(jsonParser.getValueAsString());
		    }
		    
		    if (JsonToken.END_OBJECT.equals(jsonToken))
		    {
		    	break;
		    }
		}	
		queryType = tmpQueryType;
		
		// We need a type
		if (tmpQueryType == null)
		{
			throw new Exception();
		}	
		
		if (queryType == StatementReturnType.QUERY_RESULT)
		{
			arrowBuffer = inputBuffer.position(50 + jsonSize);
		}
	}
	
	@Override
	public ByteBuffer encodeResponse() throws Exception
	{
		// JSON part
		ObjectMapper mapper = new ObjectMapper();	
		ObjectNode cmd = mapper.createObjectNode();
		
		cmd.put("op", Result.op);
		cmd.put("queryType", queryType.toString());
			
		if (queryType == StatementReturnType.CHANGED_ROWS)
		{
			cmd.put("updateCount", changedRows);
		}	
		
		ByteBuffer jsonBuffer = ByteBuffer.wrap(mapper.writeValueAsBytes(cmd));
		ByteBuffer resultBuffer;	
		
		if (queryType == StatementReturnType.QUERY_RESULT)
		{
			// Arrow part
			try 
			(
				DuckDBResultSet resultSet = (DuckDBResultSet)stmt.getResultSet();
				ByteArrayOutputStream arrowOutStream = new ByteArrayOutputStream();
				ArrowResultSet ars = new ArrowResultSet(resultSet);
				ArrowStreamWriter writer =  ars.getArrowStreamWriter(Channels.newChannel(arrowOutStream));
				
			)
			{
				writer.start();
				writer.writeBatch();
				writer.end();
				
				ByteBuffer arrowBuffer = ByteBuffer.wrap(arrowOutStream.toByteArray());
				
				ByteBuffer sizeMsgBuffer = createSizeMsg(jsonBuffer.capacity(), arrowBuffer.capacity());
				
				System.out.println(arrowBuffer.capacity());	
				
				// Concate ByteBuffers
				resultBuffer = ByteBuffer.allocate(sizeMsgBuffer.capacity() + jsonBuffer.capacity() + arrowBuffer.capacity());
				resultBuffer.put(sizeMsgBuffer).put(jsonBuffer).put(arrowBuffer);	
			}
		}
		else
		{
			ByteBuffer sizeMsgBuffer = createSizeMsg(jsonBuffer.capacity(), 0);
			
			// Concate ByteBuffers
			resultBuffer = ByteBuffer.allocate(sizeMsgBuffer.capacity() + jsonBuffer.capacity());	
			resultBuffer.put(sizeMsgBuffer).put(jsonBuffer);
		}
		
		return resultBuffer;	
	}

	private ByteBuffer createSizeMsg(int jsonBufSize, int arrowBufSize) throws JsonProcessingException
	{
		// Known size of resulting message is: 50
		ObjectMapper mapper = new ObjectMapper();	
		ObjectNode msg = mapper.createObjectNode();
		
		msg.put("jsonSize", String.format("%010d", jsonBufSize));
		msg.put("arrowSize", String.format("%010d", arrowBufSize));
		
		return ByteBuffer.wrap(mapper.writeValueAsBytes(msg));	
	}
	
}
