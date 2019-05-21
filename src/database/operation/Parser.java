package database.operation;

import java.io.*;

import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.statement.delete.*;
import net.sf.jsqlparser.statement.select.*;
import database.persist.DBFile;
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
	
	 /**
     * 构造函数
     */
    public Parser(DatabaseManager m)
    {
    	this.manager = m;
    	this.parser = new CCJSqlParserManager();
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
			Statement statement = parser.parse(new StringReader(str));
			
			//插入语句
			if(statement instanceof Insert)
			{
				
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
			//解析失败
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
	
	public ITupleIterator processInsertStatement(Insert statement) throws Exception
	{
		return null;
	}
	
	public ITupleIterator processDeleteStatement(Delete statement) throws Exception
	{
		return null;	
	}

//********************主函数********************	
    public static void main (String args[])
    {
    	//测试查询语句的解析
    	DatabaseManager manager = new DatabaseManager();
    	Parser parser = new Parser(manager);
    	int num_col = 3;
    	String column_name = "column";
    	int[] data = new int[num_col];
    	
    	Schema schema1 = database.server.TestMain.createSchema(num_col,column_name,"table1");
    	Schema schema2 = database.server.TestMain.createSchema(num_col,column_name,"table2");
    	
    	Tuple tuple1 = null;
    	Tuple tuple2 = null;
    	try
    	{    		
    		DBFile dbfile1 = parser.manager.database.getTableManager().createNewTable("table1",schema1);
    		DBFile dbfile2 = parser.manager.database.getTableManager().createNewTable("table2",schema2);
    		for(int i=0;i<5;i++)
    		{
    			data[0]= i;
    			data[1]=i+1;
    			data[2]=i+2;
    			tuple1 = database.server.TestMain.createTuple(data, schema1);
        		parser.manager.database.getPageBuffer().insertTuple(dbfile1.getId(), tuple1);
    			tuple2 = database.server.TestMain.createTuple(data, schema2);
        		parser.manager.database.getPageBuffer().insertTuple(dbfile2.getId(), tuple2);   			
    		}
    	}
    	catch (Exception e)
        {
        	System.err.println(e.getMessage());
            System.exit(0);
        }
    	
    	String str = "SELECT table1.column1,table2.* FROM table1 JOIN table2 ON table1.column1 <= table2.column0 WHERE table1.column1 < 3";

    	try
    	{
    		ITupleIterator it = parser.processStatement(str);
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
    		System.out.println("QUERY FINISH.");
		}
    	catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
    	
    }

}