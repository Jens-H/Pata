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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ExceptionRaised  implements PataResponse
{
    public static final String op = "ExceptionRaised";

    public Exception exception;
    public String exceptionMsg;

    public ExceptionRaised(Exception exception)
    {
        this.exception = exception;
        this.exceptionMsg = exception.getMessage();
    }

    public ExceptionRaised(JsonParser jsonParser) throws Exception
    {
        // Process Command
        while(!jsonParser.isClosed())
        {
            JsonToken jsonToken = jsonParser.nextToken();

            if(JsonToken.FIELD_NAME.equals(jsonToken)
                    && jsonParser.getCurrentName().equals("exception"))
            {
                // Move on to field value
                jsonToken = jsonParser.nextToken();

                exceptionMsg = jsonParser.getValueAsString();
                break;
            }
        }
    }

    @Override
    public String getOp()
    {
        return op;
    }

    @Override
    public ByteBuffer encodeResponse() throws JacksonException, IOException, Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode cmd = mapper.createObjectNode();

        cmd.put("op", ExceptionRaised.op);
        cmd.put("exception", exception.getMessage());

        return ByteBuffer.wrap(mapper.writeValueAsBytes(cmd));
    }
}
