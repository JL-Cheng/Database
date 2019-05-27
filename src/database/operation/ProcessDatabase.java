package database.operation;

import database.server.DatabaseManager;

public class ProcessDatabase
{
	/**
	 * 创建数据库的执行函数
	 * @param dbname 数据库名
	 */
	public static String operateCreateDatabase(DatabaseManager manager, String dbname) throws Exception
	{
		System.out.println("CreateDatabase: " + dbname);
		manager.addDatabase(dbname);
		return "CreateDatabase: " + dbname;
	}
	
	/**
	 * 删除数据库的执行函数
	 * @param dbname 数据库名
	 */ 
	public static String operateDropDatabase(DatabaseManager manager, String dbname) throws Exception
	{
		System.out.println("DropDatabase: " + dbname);
		manager.removeDatabase(dbname);
		return "DropDatabase: " + dbname;
	}
	
	/**
	 * 切换数据库的执行函数
	 * @param dbname 数据库名
	 */
	public static String operateSwitchDatabase(DatabaseManager manager, String dbname) throws Exception
	{
		System.out.println("SwitchDatabase: " + dbname);
		manager.switchDatabase(dbname);
		return "SwitchDatabase: " + dbname;
	}
	
	/**
	 * 展示所有数据库的执行函数
	 */
	public static String operateShowDatabases(DatabaseManager manager) throws Exception
	{
		String[] dbs = manager.getDatabases();
		String res = "Databases:\n";
		for (String i_string : dbs)
		{
			res = res + "- " + i_string + '\n';
		}
		return res;
	}
	
}
