package database;

import java.io.*;

import database.DBPage.DBPageId;

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
        return new Schema(types, names);
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
     * 创建一个新的空的数据表文件，向其中加入一个空的页。
     */
    public static DBFile createEmptyDBFile(String path,Schema schema)
    {
        File f = new File(path);
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
        

        DBFile dbfile = new DBFile(f, schema);
        Database.getTableManager().addTable(dbfile,"TestTable","name0");
        
        DBPageId page_id = new DBPageId(dbfile.getId(), 0);
        DBPage page = null;
        page = new DBPage(page_id, DBPage.createEmptyPageData());

        dbfile.writePage(page);
        return dbfile;
    }
 
    /**
     * 创建一个名为table1的数据表，将其存于'./db/table1.dat’文件中，向其中插入一个(1,2,3)的数据。
     * 将该表的结构存于schema.txt文件中。
     */
    public static void test1_createTable()
    {
    	int num_col = 3;
    	String name = "name";
    	int[] data1 = {1,2,3};
    	
    	Schema n_schema = createSchema(num_col,name);
    	System.out.println("Schema : " + n_schema);
    	
    	Tuple tuple_1 = createTuple(data1,n_schema);
    	System.out.println("Tuple : " + tuple_1);
    	
    	DBFile dbfile = createEmptyDBFile("./db/table1.dat",n_schema);
    	Database.getPageBuffer().insertTuple(dbfile.getId(), tuple_1);
    	Database.getPageBuffer().writeOperatedPages();
    	
        File schema_file = new File("./db/schema.txt");
        try
        {
        	FileOutputStream outstream = new FileOutputStream(schema_file);
        	PrintStream ps = new PrintStream(outstream);
        	ps.println("table1(name1 int primary,name2 int,name3 int)");
        	ps.close();
        }
        catch(Exception e)
        {
        	System.err.println(e.getMessage());
        }
    }
    
    /**
     *从schema.txt文件中恢复table1，并向其中再插入一个元组。
     */
    public static void test2_recoverTables()
    {
    	int[] data2 = {4,5,6};
    	
    	Database.getTableManager().loadSchema("./db/schema.txt");
    	Schema n_schema = Database.getTableManager().getDatabaseFile(Database.getTableManager().getTableId("table1")).getSchema();
    	System.out.println("Schema : " + n_schema);
    	
    	Tuple tuple_2 = createTuple(data2,n_schema);
    	System.out.println("Tuple : " + tuple_2);
    	
    	Database.getPageBuffer().insertTuple(Database.getTableManager().getTableId("table1"), tuple_2);
    	Database.getPageBuffer().writeOperatedPages();
    	
    }
    
    public static void main (String args[])
    {
    	test1_createTable();
    }
    
}

