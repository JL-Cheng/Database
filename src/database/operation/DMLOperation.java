package database.operation;

import java.io.*;
import java.util.List;

import database.field.FieldType;
import database.server.DatabaseManager;
import database.structure.Schema;
import database.structure.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.insert.Insert;

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

}
