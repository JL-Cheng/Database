package database;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

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
	private Map<Integer,String> primary_keys;//数据库中表的id与主键键值对
	
    /**
     * 构造函数
     */
    public DBTableManager()
    {
    	dbnames = new HashMap<Integer,String>();
    	dbfiles = new HashMap<Integer,DBFile>();
    	primary_keys =  new HashMap<Integer,String>();
    }

    /**
     * 加入一张新表
     * @param dbfile 数据表文件
     * @param dbname 数据表名字
     * @param primary_key 数据表主键
     */
    public void addTable(DBFile dbfile, String dbname, String primary_key)
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
	    			primary_keys.remove(id);
    			}
    			break;
    		}
    	}
    	dbnames.put(table_id, dbname);
    	dbfiles.put(table_id, dbfile);
    	primary_keys.put(table_id, primary_key);
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
    public String getPrimaryKey(int table_id)
    {
    	if (primary_keys.containsKey(table_id))
    	{
    		return primary_keys.get(table_id);
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
     * 加载所有表的元数据来构建所有表
     * -文件中每一行格式为： 数据库名字 (列名字 数据类型, 列名字 数据类型, ...)
     * @param schema_file
     */
    public void loadSchema(String schema_file)
    {
        String line = "";
        String baseFolder=new File(new File(schema_file).getAbsolutePath()).getParent();
        try
        {
			BufferedReader reader = new BufferedReader(new FileReader(new File(schema_file)));
			
			while ((line = reader.readLine()) != null)
			{
                String name = line.substring(0, line.indexOf("(")).trim();
                String[] fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim().split(","); 
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<FieldType> types = new ArrayList<FieldType>();
                String primary_key = "";
                for (String str : fields)
                {
                    String[] n_fields = str.trim().split(" ");
                    names.add(n_fields[0].trim());
                    if (n_fields[1].trim().toLowerCase().equals("int"))
                        types.add(FieldType.INT_TYPE);
                    else if (n_fields[1].trim().toLowerCase().equals("long"))
                        types.add(FieldType.LONG_TYPE);
                    else if (n_fields[1].trim().toLowerCase().equals("float"))
                        types.add(FieldType.FLOAT_TYPE);
                    else if (n_fields[1].trim().toLowerCase().equals("double"))
                        types.add(FieldType.DOUBLE_TYPE);
                    else if (n_fields[1].trim().toLowerCase().equals("string"))
                        types.add(FieldType.STRING_TYPE);
                    else
                    {
                        System.out.println("wrong type :" + n_fields[1]);
                        System.exit(0);
                    }
                    if (n_fields.length == 3)
                    {
                        if (n_fields[2].trim().equals("primary"))
                            primary_key = n_fields[0].trim();
                        else
                        {
                            System.out.println("wrong constraint :" + n_fields[2]);
                            System.exit(0);
                        }
                    }
                }
                FieldType[] types_array = types.toArray(new FieldType[0]);
                String[] names_array = names.toArray(new String[0]);
                Schema n_schema = new Schema(types_array, names_array);
                DBFile n_dbfile = new DBFile(new File(baseFolder+"/"+ name + ".dat"), n_schema);
                addTable(n_dbfile,name,primary_key);
                System.out.println("Added table : " + name + " ( " + n_schema + " ) ");
            }
			reader.close();
        }
        catch (Exception e)
        {
        	System.err.println(e.getMessage());
            System.exit(0);
        }
    }
}
