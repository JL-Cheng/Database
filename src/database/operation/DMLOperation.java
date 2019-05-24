package database.operation;

import java.util.List;
import java.util.Vector;


import database.field.FieldCompare;
import database.persist.DBTable;
import database.server.DatabaseManager;
import database.structure.ITupleIterator;
import database.structure.Schema;
import database.structure.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.SubSelect;

public class DMLOperation {
	/**
	 * 插入元组的执行函数
	 * @param Insert Insert类对象，代表解析结果
	 */
	public static void operateInsert(DatabaseManager manager, Insert statement) throws Exception {
		System.out.println("Start Insert");
		Table table = statement.getTable();
		int table_id = manager.database.getTableManager().getTableId(table.getName());
		if (table_id == -1) {
			throw new Exception("Invalid Table Name");
		}
		Schema schema = manager.database.getTableManager().getSchema(table_id);
		Tuple tuple = new Tuple(schema);
		
		List<Column> columns = statement.getColumns();
		ExpressionList itemslist = (ExpressionList) statement.getItemsList();
		List<Expression> exlist = itemslist.getExpressions();
		
		if (columns != null) 
		{
			int size = exlist.size();
			for (int i = 0; i < size; i++) {
				int index = schema.getFieldIndex(columns.get(i).getColumnName());
				if (index == -1) {
					throw new Exception("Invalid column name.");
				}
				String value = exlist.get(i).toString();
				tuple.setField(index, schema.getFieldType(index).parse(value));
			}
		} 
		else
		{
			int size = schema.getColumnSize();
			for (int i = 0; i < size; i++)
			{
				String value = exlist.get(i).toString();
				tuple.setField(i, schema.getFieldType(i).parse(value));
			}
		}
		
		System.out.println(tuple);
		manager.database.getPageBuffer().insertTuple(table_id, tuple);
		System.out.println("Finish Insert");
	}
	
	public static void operateDelete(DatabaseManager manager, Delete statement) throws Exception {
		System.out.println("Start Delete");
		WhereVisitor whereVisitor = new WhereVisitor();
		Table table = statement.getTable();
		if (table == null || !manager.database.getTableManager().isTableExist(table.getName()))
		{
			throw new Exception("[operateDelete]: wrong table");
		}
		int table_id = manager.database.getTableManager().getTableId(table.getName());
		DBTable dbTable = manager.database.getTableManager().getDatabaseFile(table_id);
		ITupleIterator it = dbTable.iterator();
		statement.getWhere().accept(whereVisitor);
		Vector<NodeWhere> nodes = whereVisitor.getNodes();
		for (NodeWhere node : nodes)
		{
			System.out.println(node.table_name);
			System.out.println(table.getName());
			// 处理左边
			if (!node.table_name.equals("") && !node.table_name.equals(table.getName()))
			{
				throw new Exception("[operateDelete] wrong table name :" + node.field_name);
			}
			if (node.table_name.equals(""))
			{
				node.field_name = table.getName() + '.' + node.field_name;
			}
			int field_id = dbTable.getSchema().getFieldIndex(node.field_name);
			if (field_id == -1)
			{
				throw new Exception("[operateDelete] wrong field name :" + node.field_name);
			}
			// 处理右边
			if (node.isConsType)
			{
				FieldCompare field_cp = new FieldCompare(field_id, node.re, dbTable.getSchema().getFieldType(field_id).parse(node.cons));
				it = new OperatorFilter(field_cp, it);
			}
			else 
			{
				if (!node.right_table_name.equals("") && !node.right_table_name.equals(table.getName()))
				{
					throw new Exception("[operateDelete] wrong right table name :" + node.right_field_name);
				}
				if (node.right_table_name.equals(""))
				{
					node.right_field_name = table.getName() + '.' + node.right_field_name;
				}
				int right_field_id = dbTable.getSchema().getFieldIndex(node.right_field_name);
				if (right_field_id == -1)
				{
					throw new Exception("[operateDelete] wrong right field name :" + node.right_field_name);
				}
				FieldCompare field_cp = new FieldCompare(field_id, node.re, right_field_id);	
				it = new OperatorFilter(field_cp, it);
			}
		}
		it.start();
		while(it.hasNext())
		{
			manager.database.getPageBuffer().deleteTuple(it.next());
		}
		System.out.println("Finish Delete");
	}
}
