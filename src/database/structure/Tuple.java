package database.structure;

import java.io.Serializable;
import java.util.Iterator;

import database.field.IField;
import database.persist.DBPage.DBPageId;

/**
 * 类型：类
 * 
 * 功能：表的元组（即每一行数据记录）
 * 
 */
public class Tuple implements Serializable
{
	
	private static final long serialVersionUID = 1L;//控制序列化版本
	

	/**
	 * 类型：类
	 * 
	 * 功能：管理元组id，每个id对应于一个特定表的一个特定页的一个元组
	 * 
	 */
	public static class TupleId
	{
	
	    private DBPageId page_id;
	    private int tuple_no;
	    
	    /**
	     * 构造函数
	     * @param page_id 页的id
	     * @param tuple_no 对应的元组索引
	     */
	    public TupleId(DBPageId page_id, int tuple_no)
	    {
	    	this.page_id = page_id;
	    	this.tuple_no = tuple_no;
	    }

	    public int getTupleNo() { return tuple_no; }
	
	    public DBPageId getPageId() { return page_id; }
	
	    /**
	     * 比较两元组是否相同
	     * @param obj 与之比较的元组的id对象
	     * @return 相同则返回true
	     */
	    public boolean equals(Object obj)
	    {
	    	if (!(obj instanceof TupleId))
	    	{
	    		return false;
	    	}
	    	else
	    	{
	    		TupleId m_obj = (TupleId)obj;    		
	    		return (this.page_id.equals(m_obj.page_id) && this.tuple_no == m_obj.tuple_no);
	    	}
	    }
	}


    private Schema schema;//元组对应的元数据
    private IField[] fields;//元组的所有元素
    private TupleId id;//元组的id
    
    /**
     * 构造函数
     * @param schema 元组对应的元数据
     */
    public Tuple(Schema schema)
    {
    	this.schema = schema;
    	this.fields = new IField[schema.numFields()];
    }
    /**
     * 拷贝构造函数
     * @param tuple
     */
    public Tuple(Tuple tuple)
    {
    	this.schema = tuple.schema;
    	int size = schema.numFields();
    	this.fields = new IField[size];
    	this.id = tuple.id;
    	for (int i = 0; i < size; i++)
    	{
    		this.fields[i] = tuple.fields[i];
    	}
    }
    
    /**
     * 遍历元组元素的迭代器
     * @return IField迭代器
     * */
    public Iterator<IField> iterator()
    {
    	return new Iterator<IField>()
    	{
    		private int id = -1;
    		
			public boolean hasNext()
			{
				return id+1<fields.length;
			}

			public IField next()
			{
				if (++id == fields.length)
				{
					return null;
				}
				else
				{
					return fields[id];
				}
			}
    	};
    }
    
    /**
     * 设置元组对应的元数据
     * @param schema 元数据
     */
    public void setSchema(Schema schema) { this.schema = schema; }

    /**
     * 获取元组对应的元数据
     * @return 元数据
     */
    public Schema getSchema() { return schema; }

    /**
     * 设置元组对应的id
     * @param id 元组id
     */
    public void setTupleId(TupleId id) { this.id = id; }
    
    /**
     * 获取元组对应的id
     * @return 元组id
     */
    public TupleId getTupleId() { return id; }

    /**
     * 设置元组第i个元素的值
     * @param i 元素索引
     * @param value 新的值
     */
    public void setField(int i, IField value)
    {
    	System.out.println("set "+ i + ":" + value);
        try
        {
        	fields[i] = value;
        }
        catch (Exception e)
        {
        	System.err.println(e.getMessage());
        }
    	
    }
    
    /**
     * 获取元组第i个元素的值
     * @param i 元素索引
     * @return 第i个元素的值
     */
    public IField getField(int i)
    {
        try
        {
        	return fields[i];
        }
        catch (Exception e)
        {
        	System.err.println(e.getMessage());
        	return null;
        }	
    }
    
    /**
     * 将该元组以字符串形式表达
     * @return 表达的字符串
     */
    public String toString()
    {
    	String result = "";
    	
    	result += fields[0].toString();
    	for (int i=1; i<fields.length; i++)
    	{
    		if (fields[i] != null)
    		{    			
    			result += "\t" + fields[i].toString();
    		}
    		else 
    		{
    			result += "\t" + "null";
    		}
    	}
        return result;
    }    
}
