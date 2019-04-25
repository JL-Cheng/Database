package database.server;

import java.io.*;

import database.field.FieldInt;
import database.field.FieldType;
import database.persist.DBFile;
import database.persist.DBPage;
import database.persist.DBPage.DBPageId;
import database.structure.Schema;
import database.structure.Tuple;

/**
 * 类型：类
 * 
 * 功能：主入口程序
 * 
 */
public class Main
{
    /**
     * 创建有n列的元数据，其中每一列均为整型，每一列名字均为name+i
     */
    public static Schema createSchema(int n, String name)
    {
        FieldType[] types = new FieldType[n];
        String[] names = new String[n];
        for (int i = 0; i < n; ++i)
        {
        	types[i] = FieldType.INT_TYPE;
        }
        for (int i = 0; i < n; ++i)
        {
        	names[i] = name + i;
        }           
        return new Schema(types, names, 0);
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
    	
    	Schema n_schema = createSchema(num_col,name);
    	System.out.println("Schema : " + n_schema);
    	
    	Tuple tuple_1 = createTuple(data1,n_schema);
    	System.out.println("Tuple : " + tuple_1);
    	try {    		
    		DBFile dbfile = manager.database.getTableManager().createNewTable("table1",n_schema);
    		manager.database.getPageBuffer().insertTuple(dbfile.getId(), tuple_1);
    	} catch (Exception e)
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
    
    public static void test3_switchDB_createTable(DatabaseManager manager)
    {
    	try {
    		manager.addDatabase("public");
    		test1_createTable(manager);
    		
    	} catch (Exception e)
        {
        	System.err.println(e.getMessage());
            System.exit(0);
        }
    }
    public static void test4_switchDB_recover(DatabaseManager manager)
    {
    	try {
    		manager.switchDatabase("public");
    		test2_recoverTables(manager);
    		
    	} catch (Exception e)
        {
        	System.err.println(e.getMessage());
            System.exit(0);
        }
    }
    public static void test5_deleteTable(DatabaseManager manager) {
    	test1_createTable(manager);
    	try {    		
    		manager.database.getTableManager().removeTable("table1");
    	} catch (Exception e)
        {
        	System.err.println(e.getMessage());
            System.exit(0);
        }
    }
    public static void main (String args[])
    {
    	DatabaseManager manager = new DatabaseManager();
    	test4_switchDB_recover(manager);
    	manager.database.close();
    }
    
}

