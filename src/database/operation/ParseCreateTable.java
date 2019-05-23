package database.operation;


import java.util.Iterator;
import java.util.List;
import java.util.Vector;

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
 * 功能：解析创建表的语句
 * 
 */
public class ParseCreateTable implements StatementVisitor{
	private DatabaseManager manager;
	private List<ColumnDefinition> columns;
	private List<Index> indexes;
	private Table table;
	
	
	/**
	 * 构造函数
	 * @param m 数据库管理器
	 */
	public ParseCreateTable(DatabaseManager m)
	{
		this.manager = m;
	}
	
	/**
	 * 创建表语句的语法解析函数
	 * @param select Select类对象，代表查询语句
	 */
	public void parse(CreateTable createTable)
	{
		System.out.println("CreateTable:"+createTable.toString());
		createTable.accept(this);
	}
	
	public void oprateCreate() throws Exception {
		System.out.println("Start Create Table");
		String tablename = table.getName();
		System.out.println("Table name:" + tablename);
		if (this.manager.database.getTableManager().isTableExist(tablename)) {
			throw new Exception("Duplicated Table Name.");
		}
		int n = columns.size();
		FieldType[] types = new FieldType[n];
        String[] names = new String[n];
        int i = 0;
        for (ColumnDefinition column: columns)
        {
        	names[i] = tablename + "." + column.getColumnName();
        	String typename = column.getColDataType().getDataType();
        	types[i] = FieldType.getType(typename);
        	if (types[i] == null) {
        		throw new Exception("Invalid Data Type.");
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
		Schema schema = new Schema(types, names, i_indexes);
		System.out.println("Schema:" + schema.toString());
		manager.database.getTableManager().createNewTable(tablename, schema);
		System.out.println("Finish Create Table");
	}

	@Override
	public void visit(CreateTable createTableStatement) {
		table = createTableStatement.getTable();
		columns =  createTableStatement.getColumnDefinitions();
		indexes = createTableStatement.getIndexes();
	}
	
	@Override
	public void visit(Comment arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Commit arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Delete arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Update arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Insert arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Replace arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Drop arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Truncate arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CreateIndex arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CreateView arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AlterView arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Alter arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Statements arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Execute arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SetStatement arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ShowStatement arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Merge arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Select arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Upsert arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(UseStatement arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Block arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ValuesStatement arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DescribeStatement arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExplainStatement arg0) {
		// TODO Auto-generated method stub
		
	}
	
}
