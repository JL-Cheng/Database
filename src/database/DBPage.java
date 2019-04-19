package database;

import java.util.*;
import java.io.*;

import database.Tuple.TupleId;

/**
 * 类型：类
 * 
 * 功能：数据页，用于存储数据库表中的一段信息
 * 
 */

public class DBPage
{
	
	/**
	 * 类型：类
	 * 
	 * 功能：管理数据页id
	 * 
	 */
	public static class DBPageId
	{
		
		private int table_Id, page_no;
		
	    /**
	     * 构造函数
	     * @param table_Id 数据库表的id
	     * @param page_no 对应的页数
	     */
	    public DBPageId(int table_Id, int page_no)
	    {
	    	this.table_Id = table_Id;
	    	this.page_no = page_no;
	    }
	    
	    public int getTableId() { return table_Id; }

	    public int getPageNumber() { return page_no; }
	    
	    /**
	     * 比较两页是否相同
	     * @param obj 与之比较的页的id对象
	     * @return 相同则返回true
	     */
	    public boolean equals(Object obj)
	    {
	    	if (!(obj instanceof DBPageId))
	    	{
	    		return false;
	    	}
	    	else
	    	{
	    		DBPageId m_obj = (DBPageId)obj;  		
	    		return (this.getTableId() == m_obj.getTableId() &&
	    				this.getPageNumber() == m_obj.getPageNumber());
	    	}
	    }

	}

    DBPageId id;//页的id
    Schema schema;//页对应的表的元数据
    byte header[];//页的头部数据，标识页中的元组使用情况
    Tuple tuples[];//页中的元组
    int num_tuples;//页中的元组个数
    boolean operated = false;//该页是否修改过
    
    /**
     * 构造函数，从磁盘数据构造数据页
     * @param id 页的id
     * @param data 页的写入数据
     */
    public DBPage(DBPageId id, byte[] data)
    {
        this.id = id;
        this.schema = Database.getTableManager().getSchema(id.getTableId());
        this.num_tuples = (8*DBPageBuffer.getPageSize())/(8*schema.getSize()+1);
        DataInputStream instream = new DataInputStream(new ByteArrayInputStream(data));

        header = new byte[(num_tuples+7)/8];
    
        try
        {
            for (int i=0; i<header.length; i++)
            {
            	header[i] = instream.readByte();
            }
        	tuples = new Tuple[num_tuples];
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(instream,i);
            instream.close();
        }
        catch(Exception e)
        {
        	System.err.println(e.getMessage());
        }
        
    }

    public DBPageId getId() { return id; }

    /**
     * 从输入流中读取数据
     * @param instream 输入流
     * @param index 元组索引
     */
    private Tuple readNextTuple(DataInputStream instream, int index)
    {
        if (!isTupleUsed(index))
        {
            for (int i=0; i<schema.getSize(); i++)
            {
                try
                {
                	instream.readByte();
                }
                catch (Exception e)
                {
                	System.err.println(e.getMessage());
                }
            }
            return null;
        }

        //创建一个新的元组
        Tuple tuple = new Tuple(schema);
        TupleId tuple_id = new TupleId(id, index);
        tuple.setTupleId(tuple_id);
        try
        {
            for (int i=0; i<schema.numFields(); i++)
            {
                IField f = schema.getFieldType(i).parse(instream);
                tuple.setField(i, f);
            }
        }
        catch (Exception e)
        {
        	System.err.println(e.getMessage());
        }
        return tuple;
    }

    /**
     * 获取该页的全部数据
     * @return 字符数组表示的数据
     */
    public byte[] getPageData()
    {
        int len = DBPageBuffer.getPageSize();
        ByteArrayOutputStream byte_outstream = new ByteArrayOutputStream(len);
        DataOutputStream data_outstream = new DataOutputStream(byte_outstream);

        //获取头部数据
        for (int i=0; i<header.length; i++)
        {
            try
            {
            	data_outstream.writeByte(header[i]);
            }
            catch (Exception e)
            {
            	System.err.println(e.getMessage());
            }
        }

        //获取元组数据
        for (int i=0; i<tuples.length; i++)
        {
        	if (!isTupleUsed(i))
        	{
                for (int j=0; j<schema.getSize(); j++)
                {
                    try
                    {
                    	data_outstream.writeByte(0);
                    }
                    catch (Exception e)
                    {
                    	System.err.println(e.getMessage());
                    }

                }
                continue;
            }
        	
        	for (int j=0; j<schema.numFields(); j++)
        	{
                IField f = tuples[i].getField(j);
                try
                {
                	f.serialize(data_outstream);
                }
                catch (Exception e)
                {
                	System.err.println(e.getMessage());
                }
            }
        }

        int m_len = DBPageBuffer.getPageSize() - (header.length + schema.getSize() * tuples.length);
        byte[] zeroes = new byte[m_len];
        try
        {
        	data_outstream.write(zeroes, 0, m_len);
        }
        catch (Exception e)
        {
        	System.err.println(e.getMessage());
        }

        try
        {
        	data_outstream.flush();
        }
        catch (Exception e)
        {
        	System.err.println(e.getMessage());
        }

        return byte_outstream.toByteArray();
    }

    /**
     * 创建一个空页
     * @return 长度为页长度、全为0的字符数组。
     */
    public static byte[] createEmptyPageData()
    {
        int len = DBPageBuffer.getPageSize();
        return new byte[len];
    }

    /**
     * 从页中删除特定元组
     * @param tuple 要删除的元组
     */
    public void deleteTuple(Tuple tuple)
    {
    	if (id.equals(tuple.getTupleId().getPageId()))
    	{
    		int tuple_no = tuple.getTupleId().getTupleNo();
    		
    		if (tuple_no >= 0 && tuple_no < num_tuples)
    		{
    			if (isTupleUsed(tuple_no))
    			{
    				tuples[tuple_no] = null;
	    			setTupleUsed(tuple_no, false);
	    		}
    		}
    	}
    }

    /**
     * 向页中插入特定元组
     * @param tuple 要插入的元组
     */
    public void insertTuple(Tuple tuple)
    {
    	for (int i=0; i<num_tuples; i++)
    	{
    		if (!isTupleUsed(i))
    		{
    			tuple.setTupleId(new TupleId(id, i));
    			tuples[i] = tuple;
    			setTupleUsed(i, true);
    			return;
    		}
    	}
    }


    public void setOperated(boolean value){ this.operated = value; }

    public boolean isOperated() { return this.operated; }

    /**
     * 获取该页上空余的tuple数
     * @return 空余的tuple数
     */
    public int getNumEmptyTuples()
    {
    	int result = 0;
    	
    	for (int i=0; i<num_tuples; i++) 
    	{
    		if (!isTupleUsed(i))
    		{
    			result++;
    		}
    	}
        return result;
    }

    /**
     * 判断元组数组中某元组是否被使用
     * @param index 元组索引
     * @result 若被使用，则为true
     */
    public boolean isTupleUsed(int index) { return 0 != (header[index/8]&(1<<(index%8))); }

    /**
     * 设置元组数组中某元组使用状态
     * @param index 元组索引
     * @param value 是否被使用
     */
    private void setTupleUsed(int index, boolean value)
    {
    	if (value)
    	{
    		header[index/8] |= (1<<(index%8));
    	}
    	else
    	{
    		header[index/8] &= ~(1<<(index%8));
    	}
    }

    /**
     * 遍历页中元组的迭代器
     * @return Tuple迭代器
     * */
    public Iterator<Tuple> iterator()
    {
        return new Iterator<Tuple>()
        {
        	private int id = -1;
        	
			public boolean hasNext()
			{
				while (id+1<num_tuples && !isTupleUsed(id+1))
				{
					id++;
				}
				return id+1<num_tuples;
			}

			public Tuple next()
			{
				if (hasNext())
				{
					return tuples[++id];
				}
				else
				{
					return null;
				}
			}
        };
    }

}

