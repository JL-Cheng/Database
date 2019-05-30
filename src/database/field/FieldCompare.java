package database.field;

import java.io.Serializable;
import database.structure.Tuple;

/**
 * 类型：类
 * 
 * 功能：对于各种数据类型，比较数据的大小
 * 
 */
public class FieldCompare implements Serializable
{
	
	private static final long serialVersionUID = 1L;//控制序列化版本
	
    private int field_id;//列编号
    private int right_field_id = -1;//第二列编号
    private Re re;//比较关系
    private IField operand;//比较数
	
    /**
     * 表示比较关系的枚举类型 
     *  Eq：相等    Gt：大于    Lt：小于    Ge：大于等于    Le：小于等于    Ne：不等于
     */
    public enum Re implements Serializable
    {
        Eq, Gt, Lt, Ge, Le, Ne;

        /**
         * 根据索引获得关系
         * @param i 索引
         */
        public static Re getRe(int i)
        {
            return values()[i];
        }

        /**
         * 将关系转为字符串表示
         * @return 关系对应的字符串（如：Eq->"="）
         */
        public String toString()
        {
        	switch (this)
        	{
        	case Eq:
        		return "=";
        	case Gt:
        		return ">";
        	case Lt:
        		return "<";
        	case Ge:
        		return ">=";
        	case Le:
        		return "<=";
        	case Ne:
        		return "<>";
        	default:
        		throw new IllegalStateException("wrong relationship");        	
        	}
        }
    }

    /**
     * 构造函数
     * @param field_id 列编号
     * @param re 比较关系
     * @param operand 比较数
     */
    public FieldCompare(int field_id, Re re, IField operand)
    {
    	this.field_id = field_id;
    	this.re = re;
    	this.operand = operand;
    }

    /**
     * 构造函数
     * @param field_id 第一列编号
     * @param re 比较关系
     * @param right_field_id 第二列编号
     */
    public FieldCompare(int field_id, Re re, int right_field_id)
    {
    	this.field_id = field_id;
    	this.re = re;
    	this.right_field_id = right_field_id;
    }

    /**
     * @return 第一列对应编号
     */
    public int getField()
    {
        return field_id;
    }
    /**
     * @return 第二列对应编号
     */
    public int getRightField()
    {
        return right_field_id;
    }
    /**
     * @return 比较关系
     */
    public Re getRe()
    {
        return re;
    }
    
    /**
     * @return 比较数
     */
    public IField getOperand()
    {
        return operand;
    }
    
    /**
     * 筛选满足条件的元组
     * @param tuple 待筛选的元组
     */
    public boolean filter(Tuple tuple)
    {
    	if (right_field_id == -1)
    	{
    		return tuple.getField(field_id).compare(re, operand);    		
    	}
    	else
    	{
    		return tuple.getField(field_id).compare(re, tuple.getField(right_field_id));    
    	}
    }
    
    /**
     * 返回比较关系的字符串表示
     * @return 字符串表示
     */
    public String toString()
    {
        return "field_id = "+field_id+" re = "+re+" operand = "+operand;
    }
    
}
