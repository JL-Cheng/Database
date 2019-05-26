package database.operation;

import java.util.Vector;

import javax.swing.text.StyledEditorKit.BoldAction;

import database.field.FieldCompare;
import database.field.FieldCompare.Re;
import database.persist.DBTable;
import database.server.DatabaseManager;
import database.structure.ITupleIterator;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NextValExpression;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.ValueListExpression;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.JsonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * 类型：类
 * 
 * 功能：通用的解析表达式的访问者
 * 
 */
public class VisitorWhere implements ExpressionVisitor
{
	private Vector<NodeWhere> where_nodes = new Vector<NodeWhere>();//WHERE语句中的节点
	private String error_message;//解析过程中是否出现错误，若是""则说明没有错误
	public void setErrorMessage(String error_message) { this.error_message = this.error_message + error_message + "\n"; }
    public String getErrorMessage() { return error_message; }
    public Vector<NodeWhere> getNodes() { return where_nodes; }
	
    /**
     * 把解析结果作用在一张表上
     * @param manager 数据库管理类
     * @param table 解析的到的表
     * @return 符合条件的元组迭代器
     * @throws Exception
     */
    public ITupleIterator workOnOneTable(DatabaseManager manager, Table table) throws Exception
    {
    	int table_id = manager.database.getTableManager().getTableId(table.getName());
		DBTable dbTable = manager.database.getTableManager().getDatabaseFile(table_id);
    	ITupleIterator it = dbTable.iterator();
		for (NodeWhere node : where_nodes)
		{
			System.out.println(node.table_name);
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
				FieldCompare field_cp = new FieldCompare(field_id, node.re, dbTable.getSchema().parse(field_id, node.cons));
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
		return it;
    }
	/**
     * 增加一个WHERE节点
     * @param field_name 表列名
     */
    public void addNodeWhere(NodeWhere node) throws Exception
    {
    	if(!where_nodes.contains(node))
    	{
    		where_nodes.addElement(node);
    	}  
    }
	@Override
	public void visit(AndExpression and_expression) {
		System.out.println("AndExpression:"+and_expression.toString());
		and_expression.getLeftExpression().accept(this);
		and_expression.getRightExpression().accept(this);
	}
	/**
	 * 解析二元表达式
	 * @param statement
	 * @param re
	 */
	private void binaryExpressionVisit(BinaryExpression statement, Re re) 
	{
		//第一步，获取左侧的列
		if(!(statement.getLeftExpression() instanceof Column))
		{
			this.setErrorMessage("Wrong column : "+statement.getLeftExpression().toString());
			return;
		}
		Column left_column = (Column) statement.getLeftExpression();
		Table left_table = left_column.getTable();
		String left_column_name = "";
		String left_table_name = "";
		if (left_table != null)
		{
			left_table_name = left_table.getName();
		}
		left_column_name = left_column.toString();
		System.out.println("Left Column:"+left_column_name);
		//第二步，获取右侧的列或者值
		if(statement.getRightExpression() instanceof Column)
		{
			Column right_column = (Column) statement.getRightExpression();
			Table right_table = right_column.getTable();
			String right_column_name = "";
			String right_table_name = "";
			if(right_table != null)
			{
				right_table_name = right_table.getName();
			}
			right_column_name = right_column.toString();
			System.out.println("Right Column:"+right_column_name);
			try
			{
				this.addNodeWhere(new NodeWhere(left_table_name, left_column_name, re, right_table_name, right_column_name));
			}
			catch(Exception e)
			{
				this.setErrorMessage(e.getMessage());
				return;
			}
		}
		else if(statement.getRightExpression() instanceof DoubleValue ||
					statement.getRightExpression() instanceof LongValue ||
					statement.getRightExpression() instanceof StringValue)
		{
			System.out.println("Right value:"+statement.getRightExpression().toString());
			try
			{
				this.addNodeWhere(new NodeWhere(left_table_name, left_column_name, re, statement.getRightExpression().toString()));
			}
			catch(Exception e)
			{
				this.setErrorMessage(e.getMessage());
				return;
			}
		}
		else
		{
			this.setErrorMessage("Wrong value : "+statement.getRightExpression().toString());
			return;
		}
	}
	@Override
	public void visit(EqualsTo statement) {
		System.out.println("EqualsTo:"+statement.toString());
		binaryExpressionVisit(statement, Re.Eq);
	}

	@Override
	public void visit(GreaterThan statement) {
		System.out.println("GreaterThan:"+statement.toString());
		binaryExpressionVisit(statement, Re.Gt);
	}

	@Override
	public void visit(GreaterThanEquals statement) {
		System.out.println("GreaterThanEquals:"+statement.toString());
		binaryExpressionVisit(statement, Re.Ge);
	}

	@Override
	public void visit(MinorThan statement) {
		System.out.println("MinorThan:"+statement.toString());
		binaryExpressionVisit(statement, Re.Lt);
	}

	@Override
	public void visit(MinorThanEquals statement) {
		System.out.println("MinorThanEquals:"+statement.toString());
		binaryExpressionVisit(statement, Re.Le);
	}

	@Override
	public void visit(NotEqualsTo statement) {
		System.out.println("NotEqualsTo:"+statement.toString());
		binaryExpressionVisit(statement, Re.Ne);
	}

	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Column arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CastExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Modulo arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnalyticExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExtractExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IntervalExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OracleHierarchicalExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMatchOperator arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JsonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JsonOperator arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RegExpMySQLOperator arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(UserVariable arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NumericBind arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(KeepExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(MySQLGroupConcat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ValueListExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(RowConstructor arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OracleHint arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeKeyExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateTimeLiteralExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NotExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NextValExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CollateExpression arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(BitwiseRightShift arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseLeftShift arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SignedExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcNamedParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(HexValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}
	
}
