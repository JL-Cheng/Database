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
import net.sf.jsqlparser.statement.update.Update;
import database.server.DatabaseManager;

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
}