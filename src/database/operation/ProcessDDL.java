package database.operation;

import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.drop.Drop;

import database.field.FieldType;
import database.server.DatabaseManager;
import database.structure.Schema;


/**
 * 类型：类
 * 
 * 功能：处理数据定义语言的语句
 * 
 */
public class ProcessDDL
{
	
	/**
	 * 创建表的执行函数
	 * @param CreateTable CreateTable类对象，代表解析结果
	 */
	public static String operateCreateTable(DatabaseManager manager, CreateTable createTableStatement) throws Exception 
	{
		System.out.println("Start Create Table");
		System.out.println("CreateTable:"+createTableStatement.toString());
		Table table = createTableStatement.getTable();
		List<ColumnDefinition> columns =  createTableStatement.getColumnDefinitions();
		List<Index> indexes = createTableStatement.getIndexes();
		String tablename = table.getName();
		System.out.println("Table name:" + tablename);
		if (manager.database.getTableManager().isTableExist(tablename)) 
		{
			throw new Exception("Duplicated Table Name.\n");
		}
		int n = columns.size();
		FieldType[] types = new FieldType[n];
        String[] names = new String[n];
        HashMap<Integer, Integer> str_len = new HashMap<Integer, Integer>();
        Vector<Integer> not_null = new Vector<Integer>();
        int i = 0;
        for (ColumnDefinition column: columns)
        {
        	names[i] = tablename + "." + column.getColumnName();
        	String typename = column.getColDataType().getDataType();
        	types[i] = FieldType.getType(typename);
        	if (types[i] == null)
        	{
        		throw new Exception("Invalid Data Type.\n");
        	}
        	if (types[i].equals(FieldType.STRING_TYPE))
        	{
        		str_len.put(i, Integer.parseInt(column.getColDataType().getArgumentsStringList().get(0)));
        	}
        	List<String> other_strings = column.getColumnSpecStrings();
        	if (other_strings != null && other_strings.size()==2 && other_strings.get(0).toLowerCase().equals("not") && other_strings.get(1).toLowerCase().equals("null"))
        	{
        		not_null.add(i);
        	}
        	i++;
        }        
        int[] i_indexes = new int[indexes.size()];
        i = 0;
        for (Index index: indexes)
        {
        	List<String> indexnames = index.getColumnsNames();
        	for (String indexname: indexnames)
        	{
        		for (int j = 0; j < n; j++)
        		{
        			if (names[j].equals(indexname))
        			{
        				i_indexes[i++] = j;
        				break;
        			}
        		}
        	}
        }
        int[] not_null_array = new int[not_null.size()];
        for (int j = 0; j < not_null.size(); j++)
        {
        	not_null_array[j] = not_null.get(j).intValue();
        }
		Schema schema = new Schema(types, names, i_indexes, str_len, not_null_array);
		System.out.println("Schema:" + schema.toString());
		manager.database.getTableManager().createNewTable(tablename, schema);
		System.out.println("Finish Create Table");
		return "Create Table (Schema:" + schema + ")\n";
	}
	
	/**
	 * 删除表的执行函数
	 * @param Drop Drop类对象，代表解析结果
	 */
	public static String operateDropTable(DatabaseManager manager, Drop statement) throws Exception
	{
		System.out.println("Start Drop Table");
		System.out.println("DropTable:"+statement.toString());
		Table table = statement.getName();
		String tablename = table.getName();
		System.out.println("Table name:" + tablename);
		if (!manager.database.getTableManager().isTableExist(tablename)) 
		{
			throw new Exception("Invalid Table Name.\n");
		}
		manager.database.getTableManager().removeTable(tablename);
		System.out.println("Finish Drop Table");
		return "Drop Table " + tablename + "\n";
	}
}
