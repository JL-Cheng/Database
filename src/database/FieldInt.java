package database;

import java.io.*;

import database.FieldCompare.Re;

/**
 * 类型：类
 * 
 * 功能：Int数据类型
 * 
 */
public class FieldInt implements IField
{
	
	private static final long serialVersionUID = 1L;//控制序列化版本
	
	private int value;//数值
	
    /**
     * 构造函数
     * @param value 数值
     */
    public FieldInt(int value)
    {
        this.value = value;
    }
    
    public int getValue()
    {
        return value;
    }
    
    public String toString()
    {
        return Integer.toString(value);
    }

	public void serialize(DataOutputStream outstream) throws IOException
	{
		outstream.writeInt(value);
	}

	public boolean compare(Re re, IField operand)
	{
		
		FieldInt m_operand = (FieldInt) operand;

        switch (re)
        {
        	case Eq:
        		return value == m_operand.value;
        	case Ne:
        		return value != m_operand.value;
        	case Gt:
        		return value > m_operand.value;
        	case Lt:
        		return value < m_operand.value;
        	case Ge:
        		return value >= m_operand.value;
        	case Le:
        		return value <= m_operand.value;
        	default:
        		return false;
        }
	}
	
	public FieldType getType()
	{
		return FieldType.INT_TYPE;
	}

}
