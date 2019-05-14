package database.operation;

import java.io.*;
import Zql.*;
import database.structure.ITupleIterator;

/**
 * 类型：类
 * 
 * 功能：SQL语句解析（此类中方法均为静态方法，无需实例化）
 * 
 */
public class Parser
{

//********************SQL类型判断与分派处理********************
    /**
     * 处理读入的SQL字符串
     * @param str String类型的SQL语句
     */
	public static void processStatement(String str)
	{
		try
		{
			processStatement(new ByteArrayInputStream(str.getBytes("UTF-8")));
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
		}
	}
	
    /**
     * 处理读入的SQL字节流
     * @param instream 字节流类型的SQL语句
     */
	public static void processStatement(InputStream instream)
	{
		try
		{
			ZqlParser parser = new ZqlParser(instream);
			ZStatement str = parser.readStatement();
			
			//插入语句
			if(str instanceof ZInsert)
			{
				
			}
			//删除语句
			else if(str instanceof ZDelete)
			{
				
			}
			//查询语句
			else if(str instanceof ZQuery)
			{
				
			}
			//解析失败
			else
			{
				System.out.println("Parse error.");
			}
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
		}
	}

//********************SQL具体类型处理********************
	public static void processQueryStatement(ZQuery str) {}
	public static void processInsertStatement(ZInsert str) {}
	public static void processDeleteStatement(ZDelete str) {}
	
//********************其他辅助函数********************	
    /**
     * 解析读入的SQL查询语句
     * @param str ZQuery类的查询语句
     * @return 查询之后的元组迭代器（即为输出的表）
     */
	public static ITupleIterator parserQuery(ZQuery str)
	{
		return null;
	}

}
