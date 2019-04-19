package database;

import java.io.Serializable;

/**
 * 类型：类
 * 
 * 功能：对于各种数据类型，比较数据的大小
 * 
 */
public class FieldCompare implements Serializable {
	
	private static final long serialVersionUID = 1L;//控制序列化版本
	
    private int field_id;//数据类型对应编号
    private Re re;//比较关系
    private IField operand;//比较数
	
    /**
     * 表示比较关系的枚举类型 
     *  Eq：相等    Gt：大于    Lt：小于    Ge：大于等于    Le：小于等于    Ne：不等于
     */
    public enum Re implements Serializable {
        Eq, Gt, Lt, Ge, Le, Ne;

        /**
         * 根据索引获得关系
         * @param i 索引
         */
        public static Re getRe(int i) {
            return values()[i];
        }

        /**
         * 将关系转为字符串表示
         * @return 关系对应的字符串（如：Eq->"="）
         */
        public String toString() {
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
     * @param field_id 数据类型对应编号
     * @param re 比较关系
     * @param operand 比较数
     */
    public FieldCompare(int field_id, Re re, IField operand) {
    	this.field_id = field_id;
    	this.re = re;
    	this.operand = operand;
    }

    /**
     * @return 数据类型对应编号
     */
    public int getField() {
        return field_id;
    }

    /**
     * @return 比较关系
     */
    public Re getRe() {
        return re;
    }
    
    /**
     * @return 比较数
     */
    public IField getOperand() {
        return operand;
    }
    
    /**
     * 返回比较关系的字符串表示
     * @return 字符串表示
     */
    public String toString() {
        return "field_id = "+field_id+" re = "+re+" operand = "+operand;
    }
    
}
