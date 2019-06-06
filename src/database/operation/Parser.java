package database.operation;


import java.io.*;
import java.util.Vector;
import java.util.regex.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.statement.delete.*;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import database.persist.DBTable;
import database.server.DatabaseManager;
import database.structure.ITupleIterator;
import database.structure.Schema;
import database.structure.Tuple;

/**
 * 类型：类
 * 
 * 功能：SQL语句解析
 * 
 */
public class Parser
{
	private DatabaseManager manager;//数据库管理器
	private CCJSqlParserManager parser;//解析器
	private Pattern databasePattern;
	
	 /**
     * 构造函数
     */
    public Parser(DatabaseManager m)
    {
    	this.manager = m;
    	this.parser = new CCJSqlParserManager();
    	databasePattern = Pattern.compile("(.*)(DATABASE|database)(.*)");
    }

//********************SQL类型判断与分派处理********************
    /**
     * 处理读入的SQL字符串
     * @param str String类型的SQL语句
     * @return 结果字符串
     */
	public String processStatement(String str) throws Exception
	{
		//数据库操作语句
		Matcher matcher = databasePattern.matcher(str);
		if (matcher.find())
		{
			String optype = matcher.group(1).trim().toUpperCase();
			switch (optype)
			{
			case "CREATE": 
				//创建数据库
				return ProcessDatabase.operateCreateDatabase(manager, matcher.group(3).trim());
			case "DROP":
				//删除数据库
				return ProcessDatabase.operateDropDatabase(manager, matcher.group(3).trim());
			case "USE":
				//切换数据库
				return ProcessDatabase.operateSwitchDatabase(manager, matcher.group(3).trim());
			case "SHOW":
				if (matcher.group(3).toUpperCase().equals("S"))
				{
					//展示所有数据库
					return ProcessDatabase.operateShowDatabases(manager);
				}
				else
				{
					//展示所有表
					return ProcessDatabase.operateShowTables(manager, matcher.group(3).trim());
				}
			default:
				throw new Exception("Parse error.\n");
			}
		} 
		// 展示表
		String[] fields = str.trim().split(" ");
		if (fields.length == 3 && fields[0].toLowerCase().equals("show") && fields[1].toLowerCase().equals("table"))
		{
			return ProcessSchema.operateShowTable(manager, fields[2].trim());
		}
		
		Statement statement = null;
		try
		{
			statement = parser.parse(new StringReader(str));
		}
		catch(Exception e)
		{
			throw new Exception("Parse error.\n");
		}
		try
		{
			//插入语句
			if(statement instanceof Insert)
			{
				return ProcessDML.operateInsert(manager, (Insert) statement);
			}
			//删除语句
			else if(statement instanceof Delete)
			{
				return ProcessDML.operateDelete(manager, (Delete) statement);
			}
			//修改语句
			else if(statement instanceof Update)
			{
				return ProcessDML.operateUpdate(manager, (Update) statement);
			}
			//创建表语句
			else if(statement instanceof CreateTable)
			{
				return ProcessDDL.operateCreateTable(this.manager, (CreateTable)statement);
			}
			//删除表语句
			else if(statement instanceof Drop)
			{
				return ProcessDDL.operateDropTable(this.manager, (Drop)statement);
			}
			//查询语句
			else if(statement instanceof Select)
			{
				Select select_statement = (Select) statement;
				ProcessDQL pro = new ProcessDQL(this.manager);
				return pro.operateQuery(select_statement);
			}
			//解析错误
			else 
			{
				throw new Exception("Parse error.\n");					
			}
		}
		catch(Exception e)
		{
			throw e;
		}
	}
	
//********************测试函数********************	
	public static String showResults(ITupleIterator it)
	{
		int max_len = 0;
		Schema schema = it.getSchema();
		Vector<String[]> res_vec = new Vector<String[]>();
		int num_cols = schema.numFields();
		String res = "";
		
		it.start();	
		String[] temp = new String[num_cols];
		for(int i=0;i<num_cols;i++)
		{
			temp[i] = schema.getFieldName(i);
			max_len = temp[i].length()>max_len?temp[i].length():max_len;
		}
		res_vec.add(temp);
		while(it.hasNext())
		{
			temp = new String[num_cols];
			Tuple tuple = it.next();
			for(int i=0;i<num_cols;i++)
			{
				temp[i] = (tuple.getField(i) == null) ? "null" : tuple.getField(i).toString();
				max_len = temp[i].length()>max_len?temp[i].length():max_len;
			}
			res_vec.add(temp);
		}
		max_len += 2;
		for(int i=0;i<(max_len*num_cols + num_cols + 1);i++,res += "—");
		res += "\n";
		for(int i=0;i<res_vec.size();i++)
		{
			temp = res_vec.get(i);
			res += "|";
			for(int j=0;j<temp.length;j++)
			{
				int space = (max_len - temp[j].length())/2;
				for(int k=0;k<space;k++,res += " ");
				res += temp[j];
				for(int k=0;k<(max_len - space - temp[j].length());k++,res += " ");
				res += "|";
			}
			res += "\n";
			for(int j=0;j<(max_len*num_cols + num_cols + 1);j++,res += "—");
			res += "\n";
		}
		return res;
	}
	
	public static void testQuery(DatabaseManager manager, Parser parser) 
	{
		//测试查询语句的解析
		//String str = "SELECT table1.column1,table2.* FROM table1 JOIN table2 ON table1.column1 <= table2.column0 WHERE table2.column2 < 4";
		String str = "SELECT * from table1,table2 where table1.column1 >= table2.column2 and table2.column2 > table1.column0";
    	try
    	{
    		System.out.println(parser.processStatement(str));
    		System.out.println("QUERY FINISH.");
		}
    	catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
	}
	
	public static void createTestData(DatabaseManager manager)
	{
    	int num_col = 3;
    	String column_name = "column";
    	int[] data = new int[num_col];
    	
    	Schema schema1 = database.server.TestMain.createSchema(num_col,column_name,"table1");
    	Schema schema2 = database.server.TestMain.createSchema(num_col,column_name,"table2");
    	
    	Tuple tuple1 = null;
    	Tuple tuple2 = null;
    	try
    	{    		
    		DBTable dbfile1 = manager.database.getTableManager().createNewTable("table1",schema1);
    		DBTable dbfile2 = manager.database.getTableManager().createNewTable("table2",schema2);
    		for(int i=0;i<5;i++)
    		{
    			data[0]= i;
    			data[1]=i+1;
    			data[2]=i+2;
    			tuple1 = database.server.TestMain.createTuple(data, schema1);
        		manager.database.getPageBuffer().insertTuple(dbfile1.getId(), tuple1);
    			tuple2 = database.server.TestMain.createTuple(data, schema2);
        		manager.database.getPageBuffer().insertTuple(dbfile2.getId(), tuple2);   			
    		}
    	}
    	catch (Exception e)
        {
        	System.err.println(e.getMessage());
            System.exit(0);
        }
	}
	
	public static void testCreateTable(DatabaseManager manager, Parser parser)
	{
    	String str = "CREATE TABLE Person \n" + 
    			"(\n" + 
    			"LastName String(10),\n" + 
    			"FirstName String(20) NOT NULL,\n" + 
    			"Address String(50),\n" + 
    			"Age int,\n" + 
    			"PRIMARY KEY (LastName, FirstName)" +
    			") ";
    	try 
    	{    		
    		System.out.println(parser.processStatement(str));
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
    	
	}
	
	public static void testDropTable(DatabaseManager manager, Parser parser)
	{
    	String str = "DROP TABLE Person";
    	try 
    	{    		
    		System.out.println(parser.processStatement(str));
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
    	
	}
	
	public static void testInsertOperation1(DatabaseManager manager, Parser parser)
	{
    	String str = "INSERT INTO table1(table1.column0, table1.column1) VALUES(5, 6);";
    	try 
    	{    		
    		System.out.println(parser.processStatement(str));
    		int table_id = manager.database.getTableManager().getTableId("table1");
    		System.out.print(showResults(manager.database.getTableManager().getDatabaseFile(table_id).iterator()));
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
	}
	
	public static void testInsertOperation2(DatabaseManager manager, Parser parser)
	{
    	String str = "INSERT INTO table1 VALUES(5, 6, 7);";
    	try 
    	{    		
    		System.out.println(parser.processStatement(str));
    		int table_id = manager.database.getTableManager().getTableId("table1");
    		System.out.print(showResults(manager.database.getTableManager().getDatabaseFile(table_id).iterator()));
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
    }
	
	public static void testDatabaseOperation(DatabaseManager manager, Parser parser)
	{
    	String create = "CREATE DATABASE public";
    	String use = "use DATABASE default";
    	String show = "SHOW DATABASES";
    	String showtable = "SHOW DATABASE default";
    	String drop = "drop DATABASE public";
    	try 
    	{    		
    		System.out.println(parser.processStatement(create));
    		System.out.println(parser.processStatement(use));
    		System.out.println(parser.processStatement(show));
    		System.out.println(parser.processStatement(showtable));
    		System.out.println(parser.processStatement(drop));
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
    	
	}
	
	public static void testDeleteOperation(DatabaseManager manager, Parser parser)
	{
		String str = "DELETE FROM table1 WHERE column0 <= 2";
    	try 
    	{    		
    		int table_id = manager.database.getTableManager().getTableId("table1");
    		System.out.print(showResults(manager.database.getTableManager().getDatabaseFile(table_id).iterator()));
    		System.out.println(parser.processStatement(str));
    		System.out.print(showResults(manager.database.getTableManager().getDatabaseFile(table_id).iterator()));
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
	}
	
	public static void testUpdateOperation(DatabaseManager manager, Parser parser)
	{
		String str = "Update table1 set column0 = 0, column2 = 0 WHERE column0 <= 2";
    	try 
    	{    		
    		int table_id = manager.database.getTableManager().getTableId("table1");
    		System.out.print(showResults(manager.database.getTableManager().getDatabaseFile(table_id).iterator()));
    		System.out.println(parser.processStatement(str));
    		System.out.print(showResults(manager.database.getTableManager().getDatabaseFile(table_id).iterator()));
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
	}
	public static void testShowTable(DatabaseManager manager, Parser parser)
	{
		String str = "show table table1";
    	try 
    	{    		
    		System.out.println(parser.processStatement(str));
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
	}
	
//********************主函数********************	
    public static void main (String args[])
    {
    	DatabaseManager manager = new DatabaseManager();
    	Parser parser = new Parser(manager);
//    	testCreateTable(manager, parser);
//    	createTestData(manager);
//    	testDatabaseOperation(manager, parser);
    	//testDeleteOperation(manager, parser);
//    	testUpdateOperation(manager, parser);
    	testShowTable(manager, parser);
//    	testInsertOperation2(manager, parser);
    	//testDropTable(manager, parser);
//    	testQuery(manager, parser);
    	manager.database.close();
    }
}