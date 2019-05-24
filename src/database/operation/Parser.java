package database.operation;


import java.io.*;
import java.util.regex.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.statement.delete.*;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.select.*;
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
     * @return 处理结束的元组迭代器
     */
	public ITupleIterator processStatement(String str) throws Exception
	{
		try
		{
			//数据库操作语句
			Matcher matcher = databasePattern.matcher(str);
			if (matcher.find()) {
				String optype = matcher.group(1).strip().toUpperCase();
				switch (optype) {
				case "CREATE":
					DatabaseOperation.operateCreateDatabase(manager, matcher.group(3).strip());
					break;
				case "DROP":
					DatabaseOperation.operateDropDatabase(manager, matcher.group(3).strip());
					break;
				case "USE":
					DatabaseOperation.operateSwitchDatabase(manager, matcher.group(3).strip());
					break;
				case "SHOW":
					if (matcher.group(3).toUpperCase().equals("S"))
					{
						DatabaseOperation.operateShowDatabases(manager);
						break;
					}
					else
					{
						throw new Exception("Parse error.");
					}
				default:
					throw new Exception("Parse error.");
				}
				return null;
			} 
			
			Statement statement = parser.parse(new StringReader(str));
			//插入语句
			if(statement instanceof Insert)
			{
				DMLOperation.operateInsert(manager, (Insert) statement);
				return null;
			}
			//删除语句
			else if(statement instanceof Delete)
			{
				
			}
			//查询语句
			else if(statement instanceof Select)
			{
				Select select_statement = (Select) statement;
				return processQueryStatement(select_statement);
			}
			//创建表语句
			else if(statement instanceof CreateTable)
			{
				DDLOperation.operateCreateTable(this.manager, (CreateTable)statement);
				return null;
			}
			//删除表语句
			else if(statement instanceof Drop)
			{
				DDLOperation.operateDropTable(this.manager, (Drop)statement);
				return null;
			}
			//解析错误
			else 
			{
				throw new Exception("Parse error.");					
			}
			return null;
		}
		catch(Exception e)
		{
			throw e;
		}
	}
	

//********************SQL具体类型处理********************
	/**
	 * 处理查询语句
	 * @param statement 查询语句
	 * @return 查询成功的元组迭代器
	 * @throws Exception
	 */
	public ITupleIterator processQueryStatement(Select statement) throws Exception
	{
		ParseQuery parse_query = new ParseQuery(this.manager);
		parse_query.parse(statement);
		ITupleIterator tuple_iterator = parse_query.getQuery().operateQuery();
		return tuple_iterator;
	}
	
//********************测试函数********************	
	public static void testQuery(DatabaseManager manager, Parser parser) 
	{
		//测试查询语句的解析
    	String str = "SELECT table1.column1,table2.* FROM table1 JOIN table2 ON table1.column1 <= table2.column0 WHERE table1.column1 < 3";

    	try
    	{
    		ITupleIterator it = parser.processStatement(str);
    		showResults(it);
    		System.out.println("QUERY FINISH.");
		}
    	catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
	}
	
	public static void createTestData(DatabaseManager manager) {
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
	public static void showResults(ITupleIterator it) {
		it.start();
		Schema schema = it.getSchema();
		for(int i=0;i<schema.numFields();i++)
		{
			System.out.print(schema.getFieldName(i)+"    ");
		}
		System.out.println();
		while(it.hasNext())
		{
			System.out.println(it.next());
		}
	}
	public static void testCreateTable(DatabaseManager manager, Parser parser)
	{
    	String str = "CREATE TABLE Person \n" + 
    			"(\n" + 
    			"LastName String,\n" + 
    			"FirstName String NOT NULL,\n" + 
    			"Address String,\n" + 
    			"Age Int,\n" + 
    			"PRIMARY KEY (LastName, FirstName)" +
    			") ";
    	try 
    	{    		
    		parser.processStatement(str);
    	} catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
    	
	}
	public static void testDropTable(DatabaseManager manager, Parser parser)
	{
    	String str = "DROP TABLE Person";
    	try 
    	{    		
    		parser.processStatement(str);
    	} catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
    	
	}
	public static void testInsert(DatabaseManager manager, Parser parser)
	{
    	String str = "INSERT INTO table1 VALUES(1, 2, 3);";
    	try 
    	{    		
    		parser.processStatement(str);
    		int table_id = manager.database.getTableManager().getTableId("table1");
    		showResults(manager.database.getTableManager().getDatabaseFile(table_id).iterator());
    	} catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
    	
	}
	public static void testDatabaseOperation(DatabaseManager manager, Parser parser)
	{
    	String create1 = "CREATE DATABASE public1";
    	String create2 = "CREATE DATABASE public2";
    	String use = "use DATABASE public1";
    	String drop = "drop DATABASE public2";
    	try 
    	{    		
    		parser.processStatement(create1);
    		parser.processStatement(create2);
    		parser.processStatement(use);
    		parser.processStatement(drop);
    	} catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
    	
	}
//********************主函数********************	
    public static void main (String args[])
    {
    	DatabaseManager manager = new DatabaseManager();
    	Parser parser = new Parser(manager);
//    	createTestData(manager);
    	testDatabaseOperation(manager, parser);
//    	testInsert(manager, parser);
//    	testQuery(manager, parser);
//    	testCreateTable(manager, parser);
//    	testDropTable(manager, parser);
    	
    	manager.database.close();
    }
}