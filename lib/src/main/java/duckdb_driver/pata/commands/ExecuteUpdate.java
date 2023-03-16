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

import com.fasterxml.jackson.core.JsonParser;

import java.util.ArrayList;
import java.util.UUID;

public class ExecuteUpdate extends Execute
{
    public static final String op = "ExecuteUpdate";

    public ExecuteUpdate(JsonParser jsonParser) throws Exception
    {
        super(jsonParser);
    }
    public ExecuteUpdate()
    {
        super();
    }

    public ExecuteUpdate(UUID statementID)
    {
        super(statementID);
    }

    public ExecuteUpdate(UUID statementID, ArrayList<String> parameterTypes, ArrayList<Object> parameters)
    {
        super(statementID, parameterTypes, parameters);
    }

    public ExecuteUpdate(UUID statementID, ArrayList<String> parameterTypes, ArrayList<Object> parameters, boolean autoCommit)
    {
        super(statementID, parameterTypes, parameters, autoCommit);
    }

    @Override
    public String getOp()
    {
        return ExecuteUpdate.op;
    }
}
