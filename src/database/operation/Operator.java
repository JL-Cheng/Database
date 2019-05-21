package database.operation;

import database.structure.ITupleIterator;
import database.structure.Tuple;

/**
 * 类型：抽象类
 * 
 * 功能：操作的抽象类（辅助实现插入、删除、筛选、投影等操作）
 * 
 */
public abstract class Operator implements ITupleIterator
{
	private Tuple next = null;//下一个元组

	/**
	 * 读取下一个元组
	 */
	protected abstract Tuple readNext();
	
	public boolean hasNext()
	{
		if(next == null)
		{
			next = readNext();
		}
		return (next!=null);
	}

	public Tuple next()
	{
		if(next == null)
		{
			next = readNext();
		}
		
		Tuple temp = next;
		next = null;
		return temp;
	}

	public void stop()
	{
		next = null;
	}

}
