package database;

import java.io.*;

import database.FieldCompare.Re;

/**
 * 类型：类
 * 
 * 功能：Long数据类型
 * 
 * 作者：程嘉梁
 */
public class FieldLong implements IField {
	
	private static final long serialVersionUID = 1L;//控制序列化版本
	
	private long value;//数值
	
    /**
     * 构造函数
     * @param value 数值
     */
    public FieldLong(long value) {
        this.value = value;
    }
    
    public long getValue() {
        return value;
    }
    
    public String toString() {
        return Long.toString(value);
    }

	public void serialize(DataOutputStream outstream) throws IOException {
		outstream.writeLong(value);
	}

	public boolean compare(Re re, IField operand) {
		
		FieldLong m_operand = (FieldLong) operand;

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
	
	public FieldType getType() {
		return FieldType.LONG_TYPE;
	}

}
