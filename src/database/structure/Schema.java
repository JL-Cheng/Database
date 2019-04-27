package database.structure;

import java.io.*;
import java.util.*;

import database.field.FieldType;

/**
 * 类型：类
 * 
 * 功能：表的元数据/模式
 * 
 */
public class Schema implements Serializable
{
	
	private static final long serialVersionUID = 1L;//控制序列化版本
	
    private FieldType[] field_types;//所有列的数据类型
    private String[] field_names;//所有列的名称
    private int[] index;// 主键的下标
	
    /**
     * 构造函数
     * @param types 数据类型数组
     * @param names 名称数组
     * @param index 主键数组
     */
    public Schema(FieldType[] types, String[] names, int[] index)
    {
    	if (types.length == 0 || types.length != names.length) {
    		System.out.println("Invalid Schema Constructor Para.");
            System.exit(0);
    	}
    	this.index = index;
    	Arrays.sort(this.index);
    	field_types = new FieldType[types.length];
    	field_names = new String[types.length];
    	for (int i=0; i<types.length; i++)
    	{
    		field_types[i] = types[i];
    	}
    	
    	for (int i=0; i<names.length; i++)
    	{
    		field_names[i] = names[i];
    	}
    }
    
    /**
     * 获取主键对应的列名
     */
    public String[] getIndex()
    {
    	String[] primary_key = new String[index.length];
    	for (int i = 0; i < index.length; i++)
    	{
    		primary_key[i] = field_names[index[i]];
    	}
    	return primary_key;
	}
    
	/**
	 * 类型：类（辅助）
	 * 
	 * 功能：用于表示元数据中每一个元素的名字与数据类型
	 * 
	 */
    public static class SchemaItem implements Serializable
    {

        private static final long serialVersionUID = 1L;

        public FieldType field_type;
        public String field_name;

        public SchemaItem(FieldType t, String n)
        {
            this.field_name = n;
            this.field_type = t;
        }

        public String toString()
        {
            return field_name + "(" + field_type + ")";
        }
    }

    
    /**
     * 遍历元数据元素的迭代器
     * @return SchemaItem迭代器
     * */
    public Iterator<SchemaItem> iterator()
    {
    	return new Iterator<SchemaItem>()
    	{
    		private int id = -1;
        	
			public boolean hasNext()
			{
				return id+1<field_types.length;
			}

			public SchemaItem next()
			{
				if (++id == field_types.length)
				{
					return null;
				}
				else
				{
					return new SchemaItem(field_types[id], field_names[id]);
				}
			}
        };
    }

    /**
     * @return 元数据的长度
     */
    public int numFields() { return field_types.length; }

    /**
     * 获取某一列的名称
     * @param i 列的索引
     * @return 第i列的名称
     */
    public String getFieldName(int i)
    {  	
        try
        {
        	return field_names[i];
        }
        catch (Exception e)
        {
        	System.err.println(e.getMessage());
        	return null;
        }
    }

    /**
     * 获取某一列的数据类型
     * @param i 列的索引
     * @return 第i列的数据类型
     */
    public FieldType getFieldType(int i)
    {  	
        try
        {
        	return field_types[i];
        }
        catch (Exception e)
        {
        	System.err.println(e.getMessage());
        	return null;
        }
    }

    /**
     * 获取该元数据的大小（字节）
     * @return 元数据的大小
     */
    public int getSize()
    {
    	int size = 0;  	
    	for (int i=0; i<field_types.length; i++)
    	{
    		size += field_types[i].getLen();
    	}
        return size;
    }

    /**
     * 将该元数据以字符串形式表达
     * @return 表达的字符串
     */
    public String toString()
    {

    	String result = "(";
    	int n = this.field_types.length;
    	
    	result += this.field_names[0] + " "+ this.field_types[0];
    	int i_count = 0;
    	int index_length = index.length;
    	if (i_count < index_length && index[i_count] == 0) {
    		i_count++;
    		result += " primary";
    	}
    	for (int i=1; i<n; i++)
    	{
    		result += "," + this.field_names[i];
    		result += " "+this.field_types[i];
    		if (i_count < index_length && index[i_count] == i) {
    			i_count++;
        		result += " primary";
        	}
    	}
    	result += ')';
        return result;
    }
}
