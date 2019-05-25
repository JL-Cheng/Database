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
import net.sf.jsqlparser.statement.update.Update;

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
				tuple.setField(index, schema.getFieldType(index).parse(value, schema.getStringlen(index)));
			}
		} 
		else
		{
			int size = schema.getColumnSize();
			for (int i = 0; i < size; i++)
			{
				String value = exlist.get(i).toString();
				tuple.setField(i, schema.getFieldType(i).parse(value, schema.getStringlen(i)));
			}
		}
		
		System.out.println(tuple);
		manager.database.getPageBuffer().insertTuple(table_id, tuple);
		System.out.println("Finish Insert");
	}
	/**
	 * 删除元组的执行函数
	 * @param Delete Delete类对象，代表解析结果
	 */
	public static void operateDelete(DatabaseManager manager, Delete statement) throws Exception {
		System.out.println("Start Delete");
		// 处理表
		Table table = statement.getTable();
		if (table == null || !manager.database.getTableManager().isTableExist(table.getName()))
		{
			throw new Exception("[operateDelete]: wrong table");
		}
		// 处理where
		WhereVisitor whereVisitor = new WhereVisitor();
		statement.getWhere().accept(whereVisitor);
		ITupleIterator it = whereVisitor.workOnOneTable(manager, table);
		// 执行删除
		int table_id = manager.database.getTableManager().getTableId(table.getName());
		DBTable dbTable = manager.database.getTableManager().getDatabaseFile(table_id);
		it.start();
		while(it.hasNext())
		{
			dbTable.deleteTuple(it.next());
		}
		System.out.println("Finish Delete");
	}
	/**
	 * 修改元组的执行函数
	 * @param Update Update类对象，代表解析结果
	 */
	public static void operateUpdate(DatabaseManager manager, Update statement) throws Exception {
		System.out.println("Update Delete");
		// 处理表 
		Table table = statement.getTables().get(0);
		System.out.println(table);
		if (table == null || !manager.database.getTableManager().isTableExist(table.getName()))
		{
			throw new Exception("[operateDelete]: wrong table");
		}
		// 处理where部分
		WhereVisitor whereVisitor = new WhereVisitor();
		statement.getWhere().accept(whereVisitor);
		ITupleIterator it = whereVisitor.workOnOneTable(manager, table);
		// 处理set部分
		List<Column> columns = statement.getColumns();
		List<Expression> expressions = statement.getExpressions();
		int table_id = manager.database.getTableManager().getTableId(table.getName());
		DBTable dbTable = manager.database.getTableManager().getDatabaseFile(table_id);
		Schema schema = dbTable.getSchema();
		int size = columns.size();
		int[] field_ids = new int[size];
		int i = 0;
		for (Column column: columns)
		{
			if (column.getTable() != null && !column.getTable().getName().equals(table.getName()))
			{
				throw new Exception("[operateUpdate] wrong table :" + column.toString());
			}
			int id = schema.getFieldIndex(table.getName() + '.' + column.getColumnName());
			if (id == -1)
			{
				throw new Exception("[operateUpdate] wrong column :" + column.toString());
			}
			else
			{
				field_ids[i++] = id;
			}
		}
		// 执行更改
		it.start();
		while(it.hasNext())
		{
			Tuple tuple = it.next();
			Tuple newTuple = new Tuple(tuple);
			for (i = 0; i < size; i++)
			{
				newTuple.setField(field_ids[i], schema.getFieldType(field_ids[i]).parse(expressions.get(i).toString(), schema.getStringlen(field_ids[i])));
			}
			dbTable.updateTuple(tuple, newTuple);
		}
		System.out.println("Finish Update");
	}
}
