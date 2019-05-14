package database.operation;

import java.util.*;

import database.structure.ITupleIterator;
import database.structure.Schema;
import database.structure.Tuple;
import database.field.FieldCompare.Re;

/**
 * 类型：类
 * 
 * 功能：结合操作
 * 
 */
public class OperatorJoin extends Operator
{
	
	/**
	 * 类型：类
	 * 
	 * 功能：结合操作中比较两元组是否满足结合条件
	 * 
	 */
	public static class JoinCompare
	{
		private int field1_id, field2_id;
		private Re re;
		
		/** 
		 * 构造函数
	     * @param field1_id 在第一个元组中需要比较的列的id
	     * @param field2_id 在第二个元组中需要比较的列的id
	     * @param re 比较关系
	     */
		public JoinCompare(int field1_id, int field2_id, Re re)
		{
			this.field1_id = field1_id;
			this.field2_id = field2_id;
			this.re = re;
		}

	    /**
	     * 比较两个元组是否满足条件
	     */
		public boolean filter(Tuple tuple1, Tuple tuple2)
		{
			return tuple1.getField(field1_id).compare(re, tuple2.getField(field2_id));
		}
	}


	private JoinCompare join_compare;//比较条件
	private ITupleIterator tuples1,tuples2;//待筛选的元组
	private List<Tuple> cached_tuples = new ArrayList<>();//缓存的元组列表，这样第二次遍历就不用再计算了
	private int list_index = 0;//元组列表读取的下标
	private boolean is_cached = false;//是否已经成功缓存所有元组
	private Tuple fst_tuple;//第一个表中目前读到的元组
	
	/**
	 * 构造函数
	 */
	public OperatorJoin(JoinCompare join_compare, ITupleIterator tuples1,  ITupleIterator tuples2)
	{
		this.join_compare=join_compare;
		this.tuples1=tuples1;
		this.tuples2=tuples2;
	}
	
	public void start()
	{
		tuples1.start();
		tuples2.start();
		fst_tuple = tuples1.hasNext()?tuples1.next():null;
		list_index = 0;
	}

	public void stop()
	{
		tuples1.stop();
		tuples2.stop();
		fst_tuple = null;
	}

	public void reset()
	{
		tuples1.reset();
		tuples2.reset();
		fst_tuple = tuples1.hasNext()?tuples1.next():null;
		list_index = 0;
	}

	public Schema getSchema()
	{
		return Schema.merge(tuples1.getSchema(), tuples2.getSchema());
	}

	/**
	 * 读取由JOIN操作生成的下一个元组
	 * （这里的生成过程只是简单的元组拼接，重复的列需要之后用投影操作去除）
	 * 注：此处用的只是课上讲的最简单的循环生成过程，后续若有时间可进行优化
	 */
	protected Tuple readNext()
	{
		//若还没有缓存完成
		if(!is_cached)
		{
			Tuple temp = new Tuple(getSchema());
			if(fst_tuple == null)
			{
				temp = null;
			}
			else
			{
				while(true)
				{
					while(!tuples2.hasNext())
					{
						if(!tuples1.hasNext())
						{
							fst_tuple = null;
							temp = null;
							break;
						}
						else
						{
							fst_tuple = tuples1.next();
							tuples2.reset();
						}
					}
					Tuple snd_tuple = tuples2.next();
					if(join_compare.filter(fst_tuple, snd_tuple))
					{
						int i = 0;
						for(int j = 0;j<fst_tuple.getSchema().numFields();j++)
						{
							temp.setField(i, fst_tuple.getField(j));
						}
						for(int j = 0;j<snd_tuple.getSchema().numFields();j++)
						{
							temp.setField(i, snd_tuple.getField(j));
						}
						break;
					}
				}
			}
			if(cached_tuples.size()<=list_index)
			{
				cached_tuples.add(temp);
			}
		}
		//若缓存完毕
		if(cached_tuples.get(list_index)==null)
		{
			is_cached = true;
			return null;
		}
		else
		{
			return cached_tuples.get(list_index++);
		}
	}

}
