package database.operation;

import java.util.Iterator;

import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.*;
import database.field.FieldCompare.Re;
import database.server.DatabaseManager;

/**
 * 类型：类
 * 
 * 功能：解析查询语句
 * 
 */
public class ParseQuery implements SelectVisitor, FromItemVisitor, ExpressionVisitor, SelectItemVisitor
{
	private DatabaseManager manager;
	private Query query;
	
	/**
	 * 构造函数
	 * @param m 数据库管理器
	 */
	public ParseQuery(DatabaseManager m)
	{
		this.manager = m;
		this.query = new Query(manager);
	}
	
	public Query getQuery() { return this.query; }
	
	/**
	 * 查询语句的语法解析函数
	 * @param select Select类对象，代表查询语句
	 */
	public void parse(Select select)
	{
		System.out.println("Select:"+select.toString());
		select.getSelectBody().accept(this);
		if(!(this.query.getErrorMessage().equals("")))
		{
			System.out.println(this.query.getErrorMessage());
		}
	}
	
	/**
	 * 解析一般的查询语句（不包含UNION、ORDER BY等）
	 */
	@Override
	public void visit(PlainSelect plain_select)
	{
		System.out.println("PlainSelect:"+plain_select.toString());
		FromItem from_item = null;
		//第一步，解析FROM语句
		//1. 获取FROM子句中第一个表元素，如：FROM table1,table2，则返回的是table1
		from_item = plain_select.getFromItem();
		if(!(from_item instanceof Table))
		{
			this.query.setErrorMessage("Wrong table : "+from_item.toString());
			return;
		}
		Table left_table = (Table)from_item;
		left_table.accept(this);
		//2. 获取其后其他的表元素
		if (plain_select.getJoins() != null)
		{
			Table right_table = null;
			for (Iterator<Join> joins_it = plain_select.getJoins().iterator(); joins_it.hasNext();)
			{
				Join join = (Join) joins_it.next();
				//先将右侧的表加入节点中
				from_item = join.getRightItem();
				if(!(from_item instanceof Table))
				{
					this.query.setErrorMessage("Wrong table : "+from_item.toString());
					return;
				}
				right_table = (Table)from_item;
				right_table.accept(this);
				//判断其是否有ON语句
				if(join.getOnExpression()==null)
				{
					try
					{
						this.query.addNodeJoin(left_table.getName(),right_table.getName(),null);
					}
					catch(Exception e)
					{
						this.query.setErrorMessage(e.getMessage());
						return;
					}
				}
				else
				{
					System.out.println("OnExpression:"+join.getOnExpression().toString());
					join.getOnExpression().accept(this);
				}
			}
		}
		//第二步，解析WHERE语句
		if(plain_select.getWhere()!=null)
		{
			plain_select.getWhere().accept(this);
		}	
		//第三步，解析SELECT语句
		for(Iterator<SelectItem> select_it = plain_select.getSelectItems().iterator();select_it.hasNext();)
		{
			SelectItem select = (SelectItem) select_it.next();
			select.accept(this);
		}
		
	}
	
	/**
	 * 解析FROM语句得到的表，加入到from_nodes中
	 */
	@Override
	public void visit(Table table)
	{
		System.out.println("Table:"+table.toString());
		String table_name = table.getName();
		int table_id = this.manager.database.getTableManager().getTableId(table_name);
		if(table_id == -1)
		{
			this.query.setErrorMessage("Wrong table : "+table_name);
			return;
		}
		try
		{
			this.query.addNodeFrom(table_id, table_name);
		}
		catch(Exception e)
		{
			this.query.setErrorMessage(e.getMessage());
			return;
		}
	}
	
	/**
	 * 解析FROM语句与WHERE语句中的AND子句
	 */
	@Override
	public void visit(AndExpression and_expression)
	{
		System.out.println("AndExpression:"+and_expression.toString());
		and_expression.getLeftExpression().accept(this);
		and_expression.getRightExpression().accept(this);
	}
	
	/**
	 * 解析FROM语句与WHERE语句中的相等子句
	 */
	@Override
	public void visit(EqualsTo equals_to)
	{
		System.out.println("EqualsTo:"+equals_to.toString());
		//第一步，获取左侧的列
		if(!(equals_to.getLeftExpression() instanceof Column))
		{
			this.query.setErrorMessage("Wrong column : "+equals_to.getLeftExpression().toString());
			return;
		}
		Column left_column = (Column) equals_to.getLeftExpression();
		Table left_table = left_column.getTable();
		String left_column_name = "";
		if(left_table == null)
		{
			left_column_name = "null."+left_column.getColumnName();
		}
		else
		{
			left_column_name = left_table.getName()+"."+left_column.getColumnName();
		}
		System.out.println("Left Column:"+left_column_name);
		//第二步，获取右侧的列或者值
		if(equals_to.getRightExpression() instanceof Column)
		{
			Column right_column = (Column) equals_to.getRightExpression();
			Table right_table = right_column.getTable();
			String right_column_name = "";
			if(right_table == null)
			{
				right_column_name = "null."+right_column.getColumnName();
			}
			else
			{
				right_column_name = right_table.getName()+"."+right_column.getColumnName();
			}
			System.out.println("Right Column:"+right_column_name);
			try
			{
				this.query.addNodeJoin(left_column_name, right_column_name, Re.Eq);
			}
			catch(Exception e)
			{
				this.query.setErrorMessage(e.getMessage());
				return;
			}
		}
		else if(equals_to.getRightExpression() instanceof DoubleValue ||
					equals_to.getRightExpression() instanceof LongValue ||
					equals_to.getRightExpression() instanceof StringValue)
		{
			System.out.println("Right value:"+equals_to.getRightExpression().toString());
			try
			{
				this.query.addNodeWhere(left_column_name, Re.Eq, equals_to.getRightExpression().toString());
			}
			catch(Exception e)
			{
				this.query.setErrorMessage(e.getMessage());
				return;
			}
		}
		else
		{
			this.query.setErrorMessage("Wrong value : "+equals_to.getRightExpression().toString());
			return;
		}
			
	}

	/**
	 * 解析FROM语句与WHERE语句中的大于子句
	 */
	@Override
	public void visit(GreaterThan greater_than)
	{
		System.out.println("GreaterThan:"+greater_than.toString());
		//第一步，获取左侧的列
		if(!(greater_than.getLeftExpression() instanceof Column))
		{
			this.query.setErrorMessage("Wrong column : "+greater_than.getLeftExpression().toString());
			return;
		}
		Column left_column = (Column) greater_than.getLeftExpression();
		Table left_table = left_column.getTable();
		String left_column_name = "";
		if(left_table == null)
		{
			left_column_name = "null."+left_column.getColumnName();
		}
		else
		{
			left_column_name = left_table.getName()+"."+left_column.getColumnName();
		}
		System.out.println("Left Column:"+left_column_name);
		//第二步，获取右侧的列或者值
		if(greater_than.getRightExpression() instanceof Column)
		{
			Column right_column = (Column) greater_than.getRightExpression();
			Table right_table = right_column.getTable();
			String right_column_name = "";
			if(right_table == null)
			{
				right_column_name = "null."+right_column.getColumnName();
			}
			else
			{
				right_column_name = right_table.getName()+"."+right_column.getColumnName();
			}
			System.out.println("Right Column:"+right_column_name);
			try
			{
				this.query.addNodeJoin(left_column_name, right_column_name, Re.Gt);
			}
			catch(Exception e)
			{
				this.query.setErrorMessage(e.getMessage());
				return;
			}
		}
		else if(greater_than.getRightExpression() instanceof DoubleValue ||
				greater_than.getRightExpression() instanceof LongValue ||
					greater_than.getRightExpression() instanceof StringValue)
		{
			System.out.println("Right value:"+greater_than.getRightExpression().toString());
			try
			{
				this.query.addNodeWhere(left_column_name, Re.Gt, greater_than.getRightExpression().toString());
			}
			catch(Exception e)
			{
				this.query.setErrorMessage(e.getMessage());
				return;
			}
		}
		else
		{
			this.query.setErrorMessage("Wrong value : "+greater_than.getRightExpression().toString());
			return;
		}		
	}

	/**
	 * 解析FROM语句与WHERE语句中的大于等于子句
	 */
	@Override
	public void visit(GreaterThanEquals greater_than_equals)
	{
		System.out.println("GreaterThanEquals:"+greater_than_equals.toString());
		//第一步，获取左侧的列
		if(!(greater_than_equals.getLeftExpression() instanceof Column))
		{
			this.query.setErrorMessage("Wrong column : "+greater_than_equals.getLeftExpression().toString());
			return;
		}
		Column left_column = (Column) greater_than_equals.getLeftExpression();
		Table left_table = left_column.getTable();
		String left_column_name = "";
		if(left_table == null)
		{
			left_column_name = "null."+left_column.getColumnName();
		}
		else
		{
			left_column_name = left_table.getName()+"."+left_column.getColumnName();
		}
		System.out.println("Left Column:"+left_column_name);
		//第二步，获取右侧的列或者值
		if(greater_than_equals.getRightExpression() instanceof Column)
		{
			Column right_column = (Column) greater_than_equals.getRightExpression();
			Table right_table = right_column.getTable();
			String right_column_name = "";
			if(right_table == null)
			{
				right_column_name = "null."+right_column.getColumnName();
			}
			else
			{
				right_column_name = right_table.getName()+"."+right_column.getColumnName();
			}
			System.out.println("Right Column:"+right_column_name);
			try
			{
				this.query.addNodeJoin(left_column_name, right_column_name, Re.Ge);
			}
			catch(Exception e)
			{
				this.query.setErrorMessage(e.getMessage());
				return;
			}
		}
		else if(greater_than_equals.getRightExpression() instanceof DoubleValue ||
				greater_than_equals.getRightExpression() instanceof LongValue ||
				greater_than_equals.getRightExpression() instanceof StringValue)
		{
			System.out.println("Right value:"+greater_than_equals.getRightExpression().toString());
			try
			{
				this.query.addNodeWhere(left_column_name, Re.Ge, greater_than_equals.getRightExpression().toString());
			}
			catch(Exception e)
			{
				this.query.setErrorMessage(e.getMessage());
				return;
			}
		}
		else
		{
			this.query.setErrorMessage("Wrong value : "+greater_than_equals.getRightExpression().toString());
			return;
		}		
	}
	
	/**
	 * 解析FROM语句与WHERE语句中的小于子句
	 */
	@Override
	public void visit(MinorThan minor_than)
	{
		System.out.println("MinorThan:"+minor_than.toString());
		//第一步，获取左侧的列
		if(!(minor_than.getLeftExpression() instanceof Column))
		{
			this.query.setErrorMessage("Wrong column : "+minor_than.getLeftExpression().toString());
			return;
		}
		Column left_column = (Column) minor_than.getLeftExpression();
		Table left_table = left_column.getTable();
		String left_column_name = "";
		if(left_table == null)
		{
			left_column_name = "null."+left_column.getColumnName();
		}
		else
		{
			left_column_name = left_table.getName()+"."+left_column.getColumnName();
		}
		System.out.println("Left Column:"+left_column_name);
		//第二步，获取右侧的列或者值
		if(minor_than.getRightExpression() instanceof Column)
		{
			Column right_column = (Column) minor_than.getRightExpression();
			Table right_table = right_column.getTable();
			String right_column_name = "";
			if(right_table == null)
			{
				right_column_name = "null."+right_column.getColumnName();
			}
			else
			{
				right_column_name = right_table.getName()+"."+right_column.getColumnName();
			}
			System.out.println("Right Column:"+right_column_name);
			try
			{
				this.query.addNodeJoin(left_column_name, right_column_name, Re.Lt);
			}
			catch(Exception e)
			{
				this.query.setErrorMessage(e.getMessage());
				return;
			}
		}
		else if(minor_than.getRightExpression() instanceof DoubleValue ||
				minor_than.getRightExpression() instanceof LongValue ||
				minor_than.getRightExpression() instanceof StringValue)
		{
			System.out.println("Right value:"+minor_than.getRightExpression().toString());
			try
			{
				this.query.addNodeWhere(left_column_name, Re.Lt, minor_than.getRightExpression().toString());
			}
			catch(Exception e)
			{
				this.query.setErrorMessage(e.getMessage());
				return;
			}
		}
		else
		{
			this.query.setErrorMessage("Wrong value : "+minor_than.getRightExpression().toString());
			return;
		}				
	}

	/**
	 * 解析FROM语句与WHERE语句中的小于等于子句
	 */
	@Override
	public void visit(MinorThanEquals minor_than_equals)
	{
		System.out.println("MinorThanEquals:"+minor_than_equals.toString());
		//第一步，获取左侧的列
		if(!(minor_than_equals.getLeftExpression() instanceof Column))
		{
			this.query.setErrorMessage("Wrong column : "+minor_than_equals.getLeftExpression().toString());
			return;
		}
		Column left_column = (Column) minor_than_equals.getLeftExpression();
		Table left_table = left_column.getTable();
		String left_column_name = "";
		if(left_table == null)
		{
			left_column_name = "null."+left_column.getColumnName();
		}
		else
		{
			left_column_name = left_table.getName()+"."+left_column.getColumnName();
		}
		System.out.println("Left Column:"+left_column_name);
		//第二步，获取右侧的列或者值
		if(minor_than_equals.getRightExpression() instanceof Column)
		{
			Column right_column = (Column) minor_than_equals.getRightExpression();
			Table right_table = right_column.getTable();
			String right_column_name = "";
			if(right_table == null)
			{
				right_column_name = "null."+right_column.getColumnName();
			}
			else
			{
				right_column_name = right_table.getName()+"."+right_column.getColumnName();
			}
			System.out.println("Right Column:"+right_column_name);
			try
			{
				this.query.addNodeJoin(left_column_name, right_column_name, Re.Le);
			}
			catch(Exception e)
			{
				this.query.setErrorMessage(e.getMessage());
				return;
			}
		}
		else if(minor_than_equals.getRightExpression() instanceof DoubleValue ||
				minor_than_equals.getRightExpression() instanceof LongValue ||
				minor_than_equals.getRightExpression() instanceof StringValue)
		{
			System.out.println("Right value:"+minor_than_equals.getRightExpression().toString());
			try
			{
				this.query.addNodeWhere(left_column_name, Re.Le, minor_than_equals.getRightExpression().toString());
			}
			catch(Exception e)
			{
				this.query.setErrorMessage(e.getMessage());
				return;
			}
		}
		else
		{
			this.query.setErrorMessage("Wrong value : "+minor_than_equals.getRightExpression().toString());
			return;
		}			
	}

	/**
	 * 解析FROM语句与WHERE语句中的不等于子句
	 */
	@Override
	public void visit(NotEqualsTo not_equals_to)
	{
		System.out.println("NotEqualsTo:"+not_equals_to.toString());
		//第一步，获取左侧的列
		if(!(not_equals_to.getLeftExpression() instanceof Column))
		{
			this.query.setErrorMessage("Wrong column : "+not_equals_to.getLeftExpression().toString());
			return;
		}
		Column left_column = (Column) not_equals_to.getLeftExpression();
		Table left_table = left_column.getTable();
		String left_column_name = "";
		if(left_table == null)
		{
			left_column_name = "null."+left_column.getColumnName();
		}
		else
		{
			left_column_name = left_table.getName()+"."+left_column.getColumnName();
		}
		System.out.println("Left Column:"+left_column_name);
		//第二步，获取右侧的列或者值
		if(not_equals_to.getRightExpression() instanceof Column)
		{
			Column right_column = (Column) not_equals_to.getRightExpression();
			Table right_table = right_column.getTable();
			String right_column_name = "";
			if(right_table == null)
			{
				right_column_name = "null."+right_column.getColumnName();
			}
			else
			{
				right_column_name = right_table.getName()+"."+right_column.getColumnName();
			}
			System.out.println("Right Column:"+right_column_name);
			try
			{
				this.query.addNodeJoin(left_column_name, right_column_name, Re.Ne);
			}
			catch(Exception e)
			{
				this.query.setErrorMessage(e.getMessage());
				return;
			}
		}
		else if(not_equals_to.getRightExpression() instanceof DoubleValue ||
				not_equals_to.getRightExpression() instanceof LongValue ||
				not_equals_to.getRightExpression() instanceof StringValue)
		{
			System.out.println("Right value:"+not_equals_to.getRightExpression().toString());
			try
			{
				this.query.addNodeWhere(left_column_name, Re.Ne, not_equals_to.getRightExpression().toString());
			}
			catch(Exception e)
			{
				this.query.setErrorMessage(e.getMessage());
				return;
			}
		}
		else
		{
			this.query.setErrorMessage("Wrong value : "+not_equals_to.getRightExpression().toString());
			return;
		}					
	}

	/**
	 * 解析SELECT语句中SELECT *
	 */
	@Override
	public void visit(AllColumns all_columns)
	{
		System.out.println("AllColumns:"+all_columns.toString());
		try
		{
			this.query.addNodeSelect("null.*");
		}
		catch(Exception e)
		{
			this.query.setErrorMessage(e.getMessage());
			return;
		}		
	}

	/**
	 * 解析SELECT语句中SELECT table.*
	 */
	@Override
	public void visit(AllTableColumns all_table_columns)
	{
		System.out.println("AllTableColumns:"+all_table_columns.toString());
		try
		{
			this.query.addNodeSelect(all_table_columns.getTable().getName()+".*");
		}
		catch(Exception e)
		{
			this.query.setErrorMessage(e.getMessage());
			return;
		}			
	}

	/**
	 * 解析SELECT语句中SELECT table.attr
	 */
	@Override
	public void visit(SelectExpressionItem select_expression_item)
	{
		System.out.println("SelectExpressionItem:"+select_expression_item.toString());
		if(!(select_expression_item.getExpression() instanceof Column))
		{
			this.query.setErrorMessage("Wrong column : "+select_expression_item.getExpression());
			return;
		}
		Column column = (Column) select_expression_item.getExpression();
		Table table = column.getTable();
		String column_name = "";
		if(table == null)
		{
			column_name = "null."+column.getColumnName();
		}
		else
		{
			column_name = table.getName()+"."+column.getColumnName();
		}
		System.out.println("Select Column:"+column_name);	
		try
		{
			this.query.addNodeSelect(column_name);
		}
		catch(Exception e)
		{
			this.query.setErrorMessage(e.getMessage());
			return;
		}		
	}
	
	/**
	 * 解析FROM语句中的JOIN子句，形如：(table1 join table2 (on table1.attr1 = table2.attr2))，暂不支持
	 */
	@Override
	public void visit(SubJoin arg0) {
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
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LateralSubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ValuesList arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TableFunction arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ParenthesisFromItem arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SetOperationList arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WithItem arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ValuesStatement arg0) {
		// TODO Auto-generated method stub
		
	}

}
