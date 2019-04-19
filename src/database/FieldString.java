package database;

import java.io.*;

import database.FieldCompare.Re;

/**
 * 类型：类
 * 
 * 功能：String数据类型
 * 
 */
public class FieldString implements IField
{

	private static final long serialVersionUID = 1L;//控制序列化版本
	
	private String value;//字符串
	private int max_size;//最大长度
	
    /**
     * 构造函数
     * @param str 字符串
     * @param max_size 字符串最大长度
     */
    public FieldString(String str,int max_size)
    {
		this.max_size = max_size;
		this.value = str.length() > max_size ? str.substring(0, max_size) : str;
    }
    
    public String getValue()
    {
        return value;
    }
    
    public String toString()
    {
        return value;
    }

	/**
	 * 将字符串写入到输出流，写出长度为max_size + 4字节，前四个字节用于保存字符串长度。
	 */
	public void serialize(DataOutputStream outstream) throws IOException
	{
		String s = value;
		int overflow = max_size - s.length();
		if (overflow < 0) {
			s = s.substring(0, max_size);
		}
		outstream.writeInt(s.length());
		outstream.writeBytes(s);
		while (overflow-- > 0)
			outstream.write((byte) 0);
	}

	public boolean compare(Re re, IField operand)
	{
		
		FieldString m_operand = (FieldString) operand;
		int cmp = value.compareTo(m_operand.value);

        switch (re)
        {
        	case Eq:
        		return cmp == 0;
        	case Ne:
        		return cmp != 0;
        	case Gt:
        		return cmp > 0;
        	case Lt:
        		return cmp < 0;
        	case Ge:
        		return cmp >= 0;
        	case Le:
        		return cmp <= 0;
        	default:
        		return false;
        }
	}
	
	public FieldType getType()
	{
		return FieldType.STRING_TYPE;
	}
}
