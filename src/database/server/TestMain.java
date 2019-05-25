package database.server;

import database.field.FieldInt;
import database.field.FieldType;
import database.persist.DBTable;
import database.structure.Schema;
import database.structure.Tuple;

/**
 * 类型：类
 * 
 * 功能：用于测试已有的模块是否正常运行
 * 
 */
public class TestMain
{
    /**
     * 创建有n列的元数据，其中每一列均为整型，每一列名字均为name+i
     */
    public static Schema createSchema(int n, String column_name, String table_name)
    {
        FieldType[] types = new FieldType[n];
        String[] names = new String[n];
        for (int i = 0; i < n; ++i)
        {
        	types[i] = FieldType.INT_TYPE;
        }
        for (int i = 0; i < n; ++i)
        {
        	names[i] = table_name + "."+column_name + i;
        }           
        return new Schema(types, names, new int[]{0, 2}, null);
    }
    
    /**
     * 创建有n列的元组（行），每一列均为整型
     */
    public static Tuple createTuple(int[] data,Schema schema)
    {
        Tuple tuple = new Tuple(schema);
        for (int i = 0; i < data.length; ++i)
        {
        	tuple.setField(i, new FieldInt(data[i]));
        }           
        return tuple;
    }

 
    /**
     * 创建一个名为table1的数据表，向其中插入一个(1,2,3)的数据。
     * 将该表的结构存于schema.txt文件中。
     */
    public static void test1_createTable(DatabaseManager manager)
    {
    	int num_col = 3;
    	String name = "name";
    	int[] data1 = {1,2,3};
    	
    	Schema n_schema = createSchema(num_col,name,"table1");
    	System.out.println("Schema : " + n_schema);
    	
    	Tuple tuple_1 = createTuple(data1,n_schema);
    	System.out.println("Tuple : " + tuple_1);
    	try
    	{    		
    		DBTable dbfile = manager.database.getTableManager().createNewTable("table1",n_schema);
    		manager.database.getPageBuffer().insertTuple(dbfile.getId(), tuple_1);
    	}
    	catch (Exception e)
        {
        	System.err.println(e.getMessage());
            System.exit(0);
        }
    }
    
    /**
     *从schema.txt文件中恢复table1，并向其中再插入一个元组。
     */
    public static void test2_recoverTables(DatabaseManager manager)
    {
    	int[] data2 = {4,5,6};
    	Schema n_schema = manager.database.getTableManager().getDatabaseFile(manager.database.getTableManager().getTableId("table1")).getSchema();
    	System.out.println("Schema : " + n_schema);
    	
    	Tuple tuple_2 = createTuple(data2,n_schema);
    	System.out.println("Tuple : " + tuple_2);
    	
    	manager.database.getPageBuffer().insertTuple(manager.database.getTableManager().getTableId("table1"), tuple_2);
    }
    
    /**
     * 创建一个名为public的数据库，切换至该数据库后执行第一项测试。
     */
    public static void test3_switchDB_createTable(DatabaseManager manager)
    {
    	try
    	{
    		manager.addDatabase("public");
    		manager.switchDatabase("public");
    		test1_createTable(manager);
    		
    	}
    	catch (Exception e)
        {
        	System.err.println(e.getMessage());
            System.exit(0);
        }
    }
    
    /**
     * 切换至public数据库后执行第二项测试。
     */
    public static void test4_switchDB_recover(DatabaseManager manager)
    {
    	try
    	{
    		manager.switchDatabase("public");
    		test2_recoverTables(manager);
    		
    	}
    	catch (Exception e)
        {
        	System.err.println(e.getMessage());
            System.exit(0);
        }
    }
    
    /**
     * 数据库中删除表的操作的测试。
     */
    public static void test5_deleteTable(DatabaseManager manager)
    {
    	try
    	{    		
    		manager.database.getTableManager().removeTable("table1");
    	}
    	catch (Exception e)
        {
        	System.err.println(e.getMessage());
            System.exit(0);
        }
    }
    
    /**
     * 主键测试。
     */
    public static void test6_primaryKey(DatabaseManager manager)
    {
    	Schema n_schema = manager.database.getTableManager().getDatabaseFile(manager.database.getTableManager().getTableId("table1")).getSchema();
    	String[] primary_key = n_schema.getIndex();
    	for (String elem: primary_key)
    	{
    		System.out.println(elem);    		
    	}
    }
    
    public static void main (String args[])
    {
    	DatabaseManager manager = new DatabaseManager();
    	test1_createTable(manager);
    	test2_recoverTables(manager);
    	test3_switchDB_createTable(manager);
    	test4_switchDB_recover(manager);
    	test6_primaryKey(manager);
    	test5_deleteTable(manager);  	
    	manager.database.close();
    }
    
}

