package database.operation;


import java.util.HashMap;
import java.util.List;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Block;
import net.sf.jsqlparser.statement.Commit;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.SetStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.UseStatement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.comment.Comment;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.create.view.AlterView;
import net.sf.jsqlparser.statement.create.view.CreateView;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import database.field.FieldType;
import database.field.FieldCompare.Re;
import database.server.DatabaseManager;
import database.structure.Schema;


/**
 * 类型：类
 * 
 * 功能：处理数据定义语言的语句
 * 
 */
public class DDLOperation {
	
	/**
	 * 创建表的执行函数
	 * @param CreateTable CreateTable类对象，代表解析结果
	 */
	public static void operateCreateTable(DatabaseManager manager, CreateTable createTableStatement) throws Exception {
		System.out.println("Start Create Table");
		System.out.println("CreateTable:"+createTableStatement.toString());
		Table table = createTableStatement.getTable();
		List<ColumnDefinition> columns =  createTableStatement.getColumnDefinitions();
		List<Index> indexes = createTableStatement.getIndexes();
		String tablename = table.getName();
		System.out.println("Table name:" + tablename);
		if (manager.database.getTableManager().isTableExist(tablename)) {
			throw new Exception("Duplicated Table Name.");
		}
		int n = columns.size();
		FieldType[] types = new FieldType[n];
        String[] names = new String[n];
        HashMap<Integer, Integer> str_len = new HashMap<Integer, Integer>();
        int i = 0;
        for (ColumnDefinition column: columns)
        {
        	names[i] = tablename + "." + column.getColumnName();
        	String typename = column.getColDataType().getDataType();
        	types[i] = FieldType.getType(typename);
        	if (types[i] == null) {
        		throw new Exception("Invalid Data Type.");
        	}
        	if (types[i].equals(FieldType.STRING_TYPE))
        	{
        		str_len.put(i, Integer.parseInt(column.getColDataType().getArgumentsStringList().get(0)));
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
		Schema schema = new Schema(types, names, i_indexes, str_len);
		System.out.println("Schema:" + schema.toString());
		manager.database.getTableManager().createNewTable(tablename, schema);
		System.out.println("Finish Create Table");
	}
	
	/**
	 * 删除表的执行函数
	 * @param Drop Drop类对象，代表解析结果
	 */
	public static void operateDropTable(DatabaseManager manager, Drop statement) throws Exception {
		System.out.println("Start Drop Table");
		System.out.println("DropTable:"+statement.toString());
		Table table = statement.getName();
		String tablename = table.getName();
		System.out.println("Table name:" + tablename);
		if (!manager.database.getTableManager().isTableExist(tablename)) {
			throw new Exception("Invalid Table Name.");
		}
		manager.database.getTableManager().removeTable(tablename);
		System.out.println("Finish Drop Table");
	}
}
