package database.operation;

import database.server.DatabaseManager;

public class DatabaseOperation {
	/**
	 * 创建数据库的执行函数
	 * @param dbname 数据库名
	 */
	public static void operateCreateDatabase(DatabaseManager manager, String dbname) throws Exception
	{
		System.out.println("CreateDatabase: " + dbname);
		manager.addDatabase(dbname);
	}
	
	/**
	 * 删除数据库的执行函数
	 * @param dbname 数据库名
	 */ 
	public static void operateDropDatabase(DatabaseManager manager, String dbname) throws Exception
	{
		System.out.println("DropDatabase: " + dbname);
		manager.removeDatabase(dbname);
	}
	
	/**
	 * 切换数据库的执行函数
	 * @param dbname 数据库名
	 */
	public static void operateSwitchDatabase(DatabaseManager manager, String dbname) throws Exception
	{
		System.out.println("SwitchDatabase: " + dbname);
		manager.switchDatabase(dbname);
	}
	
	/**
	 * 展示所有数据库的执行函数
	 * 
	 */
	public static void operateShowDatabases(DatabaseManager manager)
	{
		
	}
	
}
