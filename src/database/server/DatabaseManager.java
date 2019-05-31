package database.server;

import java.io.*;
import java.util.ArrayList;

public class DatabaseManager
{
	public String prefix; //文件地址的前缀
	public Database database;
	
	/**
	 * 构造函数
	 */ 
	public DatabaseManager()
	{
		database = new Database(this);
		setDBname("default");
		createSchema(database.dbname);
		database.getTableManager().loadSchema();
	}
	/**
	 * 设置数据库名
	 * @param name
	 */
	private void setDBname(String name)
	{
		database.dbname = name;
		prefix = getPrefix(name);
	}
	/**
	 * 根据数据库名获取地址前缀
	 * @param name
	 * @return 地址前缀字符串
	 */
	private String getPrefix(String name) { return "./db/" + name + '/'; }
	
	/**
	 * 检查指定数据库目录下的schema.txt是否存在
	 */
	private boolean schemaExists(String name) { return new File(getPrefix(name) + "/schema.txt").exists(); }
	
	/**
	 * 创建指定数据库目录和其下的schema.txt
	 * @param name
	 * @return true说明成功创建/false说明本来就已经存在该目录下的schema.txt
	 */
	private boolean createSchema(String name)
	{
		if (schemaExists(name))
		{
			return false;
		}
		String prefix = getPrefix(name);
		File dbpath = new File(prefix);
		if (!dbpath.exists())
		{
			try
			{
				dbpath.mkdirs();
			}
			catch(Exception e)
            {
            	System.err.println(e.getMessage());
            }
		}
		File schema_file = new File(prefix + "/schema.txt");
		if (!schema_file.exists())
		{
			try
			{
				schema_file.createNewFile();
			}
			catch(Exception e)
            {
            	System.err.println(e.getMessage());
            }
		}
		return true;
	}
	/**
	 * 切换数据库
	 * @param name 数据库名称
	 */
	public void switchDatabase(String name) throws Exception
	{
		if (!schemaExists(name))
		{
			throw new Exception("Invalid Database: " + name + "\n");
		}
		database.clearAll();
		setDBname(name);
		database.getTableManager().loadSchema();
	}
	
	/**
	 * 增加数据库
	 * @param name 数据库名称
	 */
	public void addDatabase(String name) throws Exception
	{
		if (schemaExists(name))
		{
			throw new Exception("Database already exists: " + name + "\n");
		}
		createSchema(name);
	}
	
	/**
	 * 删除数据库，不能删除当前数据库
	 * @param name
	 * @throws Exception
	 */
	public void removeDatabase(String name) throws Exception
	{
		if (name.equals(database.dbname))
		{
			throw new Exception("Can't delete current DB.\n");
		}
		if (!schemaExists(name))
		{
			throw new Exception("Invalid Database: " + name + "\n");
		}
		if (!deleteFolder(getPrefix(name)))
		{
			throw new Exception("Delete failure!\n");
		};
	}
	/**
	 * 展示所有数据库
	 * @throws Exception
	 */
	public String[] getDatabases() throws Exception
	{
		File file = new File("./db/");
		if (!file.exists())
		{
			throw new Exception("showDatabases Error.\n");
		}
		File[] files = file.listFiles();
		ArrayList<String> dbs = new ArrayList<String>();
		for (File elem: files)
		{
			if (elem.isDirectory() && schemaExists(elem.getName())) {
				dbs.add(elem.getName());
			}
		}
		return dbs.toArray(new String[0]);
	}
	
	/**
	 * 展示所有表
	 * @throws Exception
	 */
	public String[] getTables(String dbname) throws Exception
	{
		if (!schemaExists(dbname))
		{
			throw new Exception("Wrong dbname!\n");
		}
		String schema_file = getPrefix(dbname) + "schema.txt";
		String line = "";
		ArrayList<String> names = new ArrayList<String>();
		try
        {
			BufferedReader reader = new BufferedReader(new FileReader(new File(schema_file)));
			
			while ((line = reader.readLine()) != null)
			{
                String name = line.substring(0, line.indexOf("(")).trim();
                names.add(name);
			}
			reader.close();
        }
        catch (Exception e)
        {
        	System.err.println(e.getMessage());
            System.exit(0);
        }
		return names.toArray(new String[0]);
	}
	
	/**
	 * 删除文件夹
	 * @param sPath 文件夹路径
	 * @return 是否成功删除
	 */
	private boolean deleteFolder(String sPath)
	{  
	    boolean flag = false;  
		File folder = new File(sPath);  
		if (!folder.exists())
		{ 
			return flag;  
		}
		else
		{  
			File[] files = folder.listFiles();  
			for (int i = 0; i < files.length; i++)
			{
				if (files[i].isFile())
				{  
					flag = files[i].delete(); 
					if (!flag) break;
				}
				else
				{
					flag = deleteFolder(files[i].getAbsolutePath());
					if (!flag) break;
				}
			}
		}
		if (!flag) return false;
		if (folder.delete())
		{
			return true;
		}
		return false;
	} 
}
