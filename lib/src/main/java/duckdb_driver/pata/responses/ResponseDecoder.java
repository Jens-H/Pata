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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class ResponseDecoder
{
	private static Charset utf8 = Charset.forName("UTF-8");
	
	private static JsonFactory jsonFactory = new JsonFactory();
	
	private final static String connectedStr = Connected.op;
	private final static String preparedStr = Prepared.op;
	private final static String resultStr = Result.op;
	private final static String committedStr = Committed.op;
	private final static String abortedStr = Aborted.op;
	private final static String exceptionStr = ExceptionRaised.op;
	
	public static PataResponse decodeBuffer(ByteBuffer inputBuffer) throws Exception
	{
		inputBuffer.position(0);	
		
		// Only the first 1000 bytes should be relevant for JSON
		CharBuffer inputChars = utf8.decode(inputBuffer.slice(0, 1000));
		
		String responseStr = null;
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
			    	
			    	responseStr = jsonParser.getValueAsString();
			    	break;
			    }
			    
			    // Hack for size msg
			    if(JsonToken.FIELD_NAME.equals(jsonToken)
				    	&& jsonParser.getCurrentName().equals("jsonSize"))
			    {	 	
			    	responseStr = "Result";
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
		switch (responseStr)		
		{
			case connectedStr:
			{
				return new Connected(jsonParser);
			}
			case preparedStr:
			{
				return new Prepared(jsonParser);
			}
			case resultStr:
			{
				return new Result(inputBuffer);
			}
			case committedStr:
			{
				return new Committed();
			}
			case abortedStr:
			{
				return new Aborted();
			}
			case exceptionStr:
			{
				return new ExceptionRaised(jsonParser);
			}
			default:
				throw new IllegalArgumentException("Unexpected value: " + inputChars.toString());
		}
	}
}
