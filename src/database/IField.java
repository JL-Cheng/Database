package database;

import java.io.*;

import database.FieldCompare.Re;

/**
 * 类型：接口
 * 
 * 功能：数据库中的所有数据类型应当实现的属性
 * 
 */
public interface IField extends Serializable
{

	/**
	 * 将该数据序列化至输出流中
	 * @param outstream 输出流
	 */
	public void serialize(DataOutputStream outstream) throws IOException;
	
	/**
	 * 将该数据转为字符串表示
	 * @return 该数据对应的字符串
	 */
    public String toString();

    /**
     * 比较该数据与另一个数据
     * @param re 操作符（Eq/Le等）
     * @param operand 另一个数据
     * @return 比较结果是否正确
     */
    public boolean compare(Re re, IField operand);

    /**
     * 获取数据类型
     * @return 数据类型
     */
    public FieldType getType();
}
