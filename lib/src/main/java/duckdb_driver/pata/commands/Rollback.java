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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Rollback implements PataCommand
{
	public static final String op = "Rollback";
	
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
		
		cmd.put("op", Rollback.op);
		
		return ByteBuffer.wrap(mapper.writeValueAsBytes(cmd));
	}
}
