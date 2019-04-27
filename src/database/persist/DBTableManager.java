package database.persist;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.*;

import database.field.FieldType;
import database.persist.DBPage.DBPageId;
import database.server.DatabaseManager;
import database.structure.Schema;

/**
 * 类型：类
 * 
 * 功能：数据表文件的管理器，跟踪所有数据库中的表
 * 
 */
public class DBTableManager
{
	
	private Map<Integer,String> dbnames;//数据库中表的id与名字键值对
	private Map<Integer,DBFile> dbfiles;//数据库中表的id与表文件键值对
	private DatabaseManager manager;
	
	/**
	 * 构造函数
	 * @param m
	 */
    public DBTableManager(DatabaseManager m)
    {
    	manager = m;
    	dbnames = new HashMap<Integer,String>();
    	dbfiles = new HashMap<Integer,DBFile>();
    }
    /** 
     * 清空
     */
    public void clearAll() {
    	dbnames.clear();
    	dbfiles.clear();
    }

    /**
     * 加入一张新表
     * @param dbfile 数据表文件
     * @param dbname 数据表名字
     * 
     */
    public void addTable(DBFile dbfile, String dbname)
    {
    	int table_id = dbfile.getId();
    	for (Integer id: dbnames.keySet())
    	{
    		if (dbnames.get(id).equals(dbname))
    		{
    			if (id != table_id)
    			{
    				dbnames.remove(id);
	    			dbfiles.remove(id);
    			}
    			break;
    		}
    	}
    	dbnames.put(table_id, dbname);
    	dbfiles.put(table_id, dbfile);
    }
    
    /**
     * 获得一张表的id
     * @param dbname 数据表名字
     */
    public int getTableId(String dbname)
    {
    	for (Integer id: dbnames.keySet())
    	{
    		if (dbnames.get(id).equals(dbname))
    		{
    			return id;
    		}
    	}
        return -1;
    }


    /**
     * 获得一张表的元数据
     * @param table_id 表的id
     */
    public Schema getSchema(int table_id)
    {
    	if (dbfiles.containsKey(table_id))
    	{
    		return dbfiles.get(table_id).getSchema();
    	}
    	return null;
    }

    /**
     * 获得一张表的数据文件
     * @param table_id 表的id
     */
    public DBFile getDatabaseFile(int table_id)
    {
    	if (dbfiles.containsKey(table_id))
    	{
    		return dbfiles.get(table_id);
    	}
    	return null;
    }

    /**
     * 获得一张表的主键
     * @param table_id 表的id
     */
    public String[] getPrimaryKey(int table_id)
    {
    	if (dbfiles.containsKey(table_id))
    	{
    		return dbfiles.get(table_id).getSchema().getIndex();
    	}
    	return null;
    }

    /**
     * 获得一张表的名字
     * @param table_id 表的id
     */
    public String getTableName(int table_id)
    {
    	if (dbnames.containsKey(table_id))
    	{
    		return dbnames.get(table_id);
    	}
    	return null;
    }
    /**
     * 创建一个新表 创建一个新的空的数据表文件，向其中加入一个空的页。
     * @param name 表名
     * @param n_schema 模式
     * @return 
     */
    public DBFile createNewTable(String name,Schema schema) throws Exception
    {
    	String path = manager.prefix + name + ".dat";
        File f = new File(path);
        if (f.exists()) {
        	throw new Exception("The Table Already exists.");
        }
        try
        {
        	FileOutputStream outstream = new FileOutputStream(f);
            outstream.write(new byte[0]);
            outstream.close();
        }
        catch(Exception e)
        {
        	System.err.println(e.getMessage());
        }
        
        DBFile dbfile = new DBFile(manager, f, schema);
        manager.database.getTableManager().addTable(dbfile,name);
        
        DBPageId page_id = new DBPageId(dbfile.getId(), 0);
        DBPage page = null;
        page = new DBPage(manager, page_id, DBPage.createEmptyPageData());

        dbfile.writePage(page);
        return dbfile;
    }
    
	/**
	 * 删除一个表 不会遍历缓冲区删除page 等待page自己被慢慢删除
	 * @param name
	 * @throws Exception
	 */
    public void removeTable(String name) throws Exception
    {
    	String path = manager.prefix + name + ".dat";
        File f = new File(path);
        if (!f.exists()) {
        	throw new Exception("The Table does not exists.");
        }
        f.delete();
        Integer id = -1;
    	for (Map.Entry<Integer, String> entry: dbnames.entrySet())
    	{
    		if (entry.getValue() == name)
    		{
    			id = entry.getKey();
    			break;
    		}
    	}
    	if (id == -1) {
    		throw new Exception("Can't find table record.");
    	}
    	dbnames.remove(id);
    	dbfiles.remove(id);
    }
    
    /**
     * 加载所有表的元数据来构建所有表
     * -文件中每一行格式为： 数据库名字 (列名字 数据类型, 列名字 数据类型, ...)
     * 
     */
    public void loadSchema()
    {
    	String schema_file = manager.prefix + "schema.txt";
        String line = "";
        String base_folder=new File(new File(schema_file).getAbsolutePath()).getParent();
        try
        {
			BufferedReader reader = new BufferedReader(new FileReader(new File(schema_file)));
			
			while ((line = reader.readLine()) != null)
			{
                String name = line.substring(0, line.indexOf("(")).trim();
                String[] fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim().split(","); 
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<FieldType> types = new ArrayList<FieldType>();
                ArrayList<Integer> primary_key = new ArrayList<Integer>();
                for (String str : fields)
                {
                    String[] n_fields = str.trim().split(" ");
                    names.add(n_fields[0].trim());
                    if (n_fields[1].trim().equals("INT_TYPE"))
                        types.add(FieldType.INT_TYPE);
                    else if (n_fields[1].trim().equals("LONG_TYPE"))
                        types.add(FieldType.LONG_TYPE);
                    else if (n_fields[1].trim().equals("FLOAT_TYPE"))
                        types.add(FieldType.FLOAT_TYPE);
                    else if (n_fields[1].trim().equals("DOUBLE_TYPE"))
                        types.add(FieldType.DOUBLE_TYPE);
                    else if (n_fields[1].trim().equals("STRING_TYPE"))
                        types.add(FieldType.STRING_TYPE);
                    else
                    {
                        System.out.println("wrong type :" + n_fields[1]);
                        System.exit(0);
                    }
                    if (n_fields.length == 3)
                    {
                        if (n_fields[2].trim().equals("primary"))
                            primary_key.add(names.size() - 1);
                        else
                        {
                            System.out.println("wrong constraint :" + n_fields[2]);
                            System.exit(0);
                        }
                    }
                }
                FieldType[] types_array = types.toArray(new FieldType[0]);
                String[] names_array = names.toArray(new String[0]);
                int[] primary_key_array = new int[primary_key.size()];
                for (int i = 0; i < primary_key.size(); i++) {
                	primary_key_array[i] = primary_key.get(i).intValue();
                }
                Schema n_schema = new Schema(types_array, names_array, primary_key_array);
                File dat = new File(base_folder+"/"+ name + ".dat");
                if (!dat.exists()) {
                	System.err.println("Data Lost: " + name);
                    System.exit(0);
                }
                DBFile n_dbfile = new DBFile(manager, dat, n_schema);
                addTable(n_dbfile,name);
                System.out.println("[Load "+ manager.database.dbname + "]Added table : " + name + n_schema);
            }
			reader.close();
        }
        catch (Exception e)
        {
        	System.err.println(e.getMessage());
            System.exit(0);
        }
    }
    /**
     * 持久化schema
     */
    public void writeSchema() {
    	String schema_file = manager.prefix + "schema.txt";
    	try {
        	FileOutputStream outstream = new FileOutputStream(schema_file);
        	PrintStream ps = new PrintStream(outstream);
        	for (Map.Entry<Integer, DBFile> entry : dbfiles.entrySet()) { 
        		DBFile file = entry.getValue();
        		ps.println(dbnames.get(entry.getKey()) + file.getSchema().toString());
        	}
        	ps.close();
    	} catch(Exception e)
        {
        	System.err.println(e.getMessage());
        }
    }
}
