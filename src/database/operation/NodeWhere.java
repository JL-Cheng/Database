package database.operation;

import database.field.FieldCompare.Re;

/**
 * 类型：类
 * 
 * 功能：查询操作中where语句中的子节点
 * 如：WHERE person.id = 1则将person.id = 1的数据提取出来
 * 注：对于 WHERE table1.attr1 = table2.attr2，将其变为JOIN节点
 * 
 */

public class NodeWhere
{
	public String table_name;//表名
	public String field_name;//列名，形如table.attr
	public Re re;//关系类型，如“=”
	public String cons;//常量
	
    /**
     * 构造函数
     * @param table_name 表名
     * @param table_field_name 列名，形如table.attr
     * @param re 关系类型，如“=”
     * @param cons 常量
     */
    public NodeWhere(String table_name,String field_name,Re re,String cons)
    {
    	this.table_name = table_name;
    	this.field_name = field_name;
    	this.re = re;
    	this.cons = cons;
    }
}
