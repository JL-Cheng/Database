package database.operation;

import database.server.DatabaseManager;

public class ProcessSchema
{
	public static String operateShowTable(DatabaseManager manager, String table_name) throws Exception 
	{
		int id = manager.database.getTableManager().getTableId(table_name);
		if (id == -1)
		{
			throw new Exception("Invalid Table Name: " + table_name + "\n");
		}
		return table_name + manager.database.getTableManager().getSchema(id).toString() + "\n";
	}
}
