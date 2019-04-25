package database.server;

import java.io.*;

public class DatabaseManager {
	public String prefix; //文件地址的前缀
	public Database database;
	public DatabaseManager() {
		database = new Database(this);
		setDBname("defalut");
		createBasic();
		database.getTableManager().loadSchema();
	}
	/**
	 * 设置数据库名
	 * @param name
	 */
	private void setDBname(String name) {
		database.dbname = name;
		prefix = getPrefix(name);
	}
	/**
	 * 根据数据库名获取地址前缀
	 * @param name
	 * @return
	 */
	private String getPrefix(String name) {
		return "./db/" + name + '/';
	}
	/**
	 * 检查指定数据库目录下的schema.txt是否存在
	 */
	private boolean schemaExists(String name) {
		return new File(getPrefix(name) + "/schema.txt").exists();
	}
	/**
	 * 创建指定数据库目录和其下的schema.txt
	 * 返回true说明成功创建 返回false说明本来就已经存在该目录下的schema.txt
	 */
	private boolean createBasic() {
		if (schemaExists(database.dbname)) {
			return false;
		}
		File dbpath = new File(prefix);
		if (!dbpath.exists()) {
			try {
				dbpath.mkdirs();
			}
			catch(Exception e)
            {
            	System.err.println(e.getMessage());
            }
		}
		File schema_file = new File(prefix + "/schema.txt");
		if (!schema_file.exists()) {
			try {
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
	 * @param name
	 */
	public void switchDatabase(String name) throws Exception {
		if (!schemaExists(name)) {
			throw new Exception("Invalid Database: " + name);
		}
		database.clearAll();
		setDBname(name);
		database.getTableManager().loadSchema();
	}
	/**
	 * 增加数据库 并切换到此数据库
	 * @param name
	 */
	public void addDatabase(String name) throws Exception {
		if (schemaExists(name)) {
			throw new Exception("Database already exists: " + name);
		}
		database.clearAll();
		setDBname(name);
		createBasic();
		database.getTableManager().loadSchema();
	}
	/**
	 * 删除数据库，不能删除当前数据库
	 * @param name
	 * @throws Exception
	 */
	public void removeDatabase(String name) throws Exception {
		if (name == database.dbname) {
			throw new Exception("Can't delete current DB.");
		}
		if (!schemaExists(name)) {
			throw new Exception("Invalid Database: " + name);
		}
		if (!deleteFolder(getPrefix(name))) {
			throw new Exception("Delete failure!");
		};
	}
	/**
	 * 删除文件夹 返回是否成功
	 * @param sPath
	 * @return
	 */
	private boolean deleteFolder(String sPath) {  
	    boolean flag = false;  
		File folder = new File(sPath);  
		if (!folder.exists()) {  // 不存在返回 false  
			return flag;  
		} else {  
			File[] files = folder.listFiles();  
			for (int i = 0; i < files.length; i++) {
				if (files[i].isFile()) {  
					flag = files[i].delete(); 
					if (!flag) break;
				} else {
					flag = deleteFolder(files[i].getAbsolutePath());
					if (!flag) break;
				}
			}
		}
		if (!flag) return false;
		if (folder.delete()) {
			return true;
		}
		return false;
	} 
}
