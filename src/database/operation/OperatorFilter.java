package database.operation;

import database.structure.Schema;
import database.structure.Tuple;
import database.field.FieldCompare;
import database.structure.ITupleIterator;

/**
 * 类型：类
 * 
 * 功能：筛选操作
 * 
 */
public class OperatorFilter extends Operator
{

	private FieldCompare field_compare;//筛选条件
	private ITupleIterator tuples;//待筛选的元组
	
	/**
	 * 构造函数
	 */
	public OperatorFilter(FieldCompare field_compare,ITupleIterator tuples)
	{
		this.field_compare=field_compare;
		this.tuples=tuples;
	}
	
	public void start() { tuples.start(); }
	
	public void reset() { tuples.reset(); }

	public Schema getSchema() { return tuples.getSchema(); }

	/**
	 * 选取元组中满足条件的下一个元组
	 * @see database.operation.Operator#readNext()
	 */
	protected Tuple readNext()
	{
		while(tuples.hasNext())
		{
			Tuple temp = tuples.next();
			if(field_compare.filter(temp))
			{
				return temp;
			}
		}
		return null;
	}

}
