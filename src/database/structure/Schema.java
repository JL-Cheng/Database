package database.structure;

import java.io.*;
import java.util.*;

import database.field.FieldType;
import database.field.IField;
import database.operation.NodeSelect;

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
    public Map<Integer,Integer> string_len;//字符串的最大长度
    private int[] not_null;//有not null约束的列的下标(主键默认not null)
	
    /**
     * 构造函数
     * @param types 数据类型数组
     * @param names 名称数组
     * @param index 主键数组
     * @param string_len 字符串长度
     */
    public Schema(FieldType[] types, String[] names, int[] index, Map<Integer,Integer> string_len, int[] not_null)
    {
    	if (types.length == 0 || types.length != names.length)
    	{
    		System.out.println("Invalid Schema Constructor Param.");
            System.exit(0);
    	}
    	this.index = index;
    	Arrays.sort(this.index);
    	// 从not null中筛除index中的元素
    	Arrays.sort(not_null);
    	ArrayList<Integer> not_null_list = new ArrayList<Integer>();
    	for (int i: not_null)
    	{
    		if (Arrays.binarySearch(this.index, i) < 0)
    		{
    			not_null_list.add(i);
    		}
    	}
    	this.not_null = new int[not_null_list.size()];
        for (int i = 0; i < not_null_list.size(); i++)
        {
        	this.not_null[i] = not_null_list.get(i).intValue();
        }
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
    	this.string_len = string_len;
    }
    
    /**
     * 获取字符串类型的最大长度
     * @param field_id
     * field_id对应的列不是string则返回-1 没有指定则返回默认最大长度
     * @return 
     */
    public int getStringlen(int field_id) 
    {
    	FieldType field = getFieldType(field_id);
		if (field.equals(FieldType.STRING_TYPE))
		{
			if (string_len != null && string_len.containsKey(field_id))
			{
				return string_len.get(field_id);
			}
			else
			{
				return FieldType.STRING_LEN;
			}
		}
		return -1;
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
     * 获取主键对应的列名
     */
    public int[] getRowIndex()
    {
    	return index;
	}
    
    /**
     * 获取列数
     */
    public int getColumnSize()
    {
    	return field_types.length;
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
     * 获取某一列的索引
     * @param name 列的名称
     * @return 列的索引,不存在则返回-1
     */
    public int getFieldIndex(String name)
    {  	
    	if(name != null)
    	{
    		for(int i=0;i<field_names.length;i++)
    		{
    			if(name.equals(field_names[i]))
    			{
    				return i;
    			}
    		}
    	}
        return -1;
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
     * 从str中解析出field_id对应列的一个Field
     * @param field_id
     * @param str
     * @return
     */
    public IField parse(int field_id, String str)
    {
    	FieldType type = getFieldType(field_id);
    	if (type.equals(FieldType.STRING_TYPE))
    	{
    		return type.parse(str, getStringlen(field_id));
    	}
    	else
    	{
    		return type.parse(str);
    	} 	
    }
    
    /**
     * 从instream中解析出field_id对应列的一个Field
     * @param field_id
     * @param instream
     * @return
     */
    public IField parse(int field_id, DataInputStream instream)
    {
    	FieldType type = getFieldType(field_id);
    	if (type.equals(FieldType.STRING_TYPE))
    	{
    		return type.parse(instream, getStringlen(field_id));
    	}
    	else
    	{
    		return type.parse(instream);
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
    		if (field_types[i].equals(FieldType.STRING_TYPE))
    		{
    			size += field_types[i].getLen(getStringlen(i));    			
    		}
    		else 
    		{
    			size += field_types[i].getLen();
    		}
    	}
        return size;
    }
    
    /**
     * 合并两个元数据（用在JOIN操作中）
     * @param schema1 第一个元数据
     * @param schema2 第二个元数据
     * @return 合成之后的元数据
     */
    public static Schema merge(Schema schema1, Schema schema2)
    {
    	FieldType[] new_types = new FieldType[(schema1.field_types.length+schema2.field_types.length)];
    	String[] new_names = new String[(schema1.field_names.length+schema2.field_names.length)];
    	int i = 0;
    	
    	for (int j=0; j<schema1.field_types.length; j++)
    	{
    		new_types[i] = schema1.field_types[j];
    		new_names[i] = schema1.field_names[j];
    		i=i+1;
    	}
    	for (int j=0; j<schema2.field_types.length; j++)
    	{
    		new_types[i] = schema2.field_types[j];
    		new_names[i] = schema2.field_names[j];
    		i=i+1;
    	}
    	Map<Integer, Integer> str_len = null;
    	if (schema1.string_len != null)
    	{
    		str_len = new HashMap<Integer, Integer>(schema1.string_len);
    	}
    	if (schema2.string_len != null)
    	{
    		if (str_len != null)
    		{
    			str_len.putAll(schema2.string_len);
    		}
    		else
    		{    			
    			str_len = new HashMap<Integer, Integer>(schema2.string_len);
    		}
    	}
    	Schema result = new Schema(new_types, new_names, new int[]{}, str_len, new int[]{});
    	return result;
    }

    /**
     * 将该元数据以字符串形式表达
     * @return 表达的字符串
     */
    public String toString()
    {

    	String result = "(";
    	int n = this.field_types.length;
    	
    	result += this.field_names[0] + " "+ this.field_types[0].getName(getStringlen(0));
    	int i_count = 0;
    	int n_count = 0;
    	int index_length = index.length;
    	int nnull_length = not_null.length;
    	if (i_count < index_length && index[i_count] == 0) 
    	{
    		i_count++;
    		result += " primary";
    	} else if (n_count < nnull_length && not_null[n_count] == 0) 
    	{
    		n_count++;
    		result += " not null";
    	}
    	for (int i=1; i<n; i++)
    	{
    		result += ", " + this.field_names[i];
    		result += " "+this.field_types[i].getName(getStringlen(i));
    		if (i_count < index_length && index[i_count] == i) 
    		{
    			i_count++;
        		result += " primary";
        	}
    		else if (n_count < nnull_length && not_null[n_count] == i) 
    		{
    			n_count++;
        		result += " not null";
        	}
    	}
    	result += ')';
        return result;
    }
    
    public boolean equals(Object obj)
    {
    	if (!(obj instanceof Schema))
    	{
    		return false;
    	}
    	Schema schema = (Schema) obj;
    	if (this.toString().equals(schema.toString()))
    	{
    		return true;
    	}
    	return false;
    }
}
