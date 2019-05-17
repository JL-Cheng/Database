package database.operation;

import database.field.FieldCompare.Re;

/**
 * 类型：类
 * 
 * 功能：查询操作中join语句中的子节点
 * 1、FROM t1 JOIN t2 ON t1.attr1 = t2.attr2，则将t1，t2，attr1，attr2的数据提取出来；
 * 2、FROM t1 , t2，则将t1，t2提取出来
 * 
 */

public class NodeJoin
{
	public String table1_name;//第一张表表名
	public String table2_name;//第二张表表名
	public String field1_name;//第一张表列名，形如table.attr（可能为空）
	public String field2_name;//第二张表列名，形如table.attr（可能为空）
	public Re re;//关系类型，如“=”（可能为空）
	
    /**
     * 构造函数
     * @param table1_name 第一张表表名
     * @param table2_name 第二张表表名
     * @param field1_name 第一张表列名，形如table.attr
     * @param field2_name 第二张表列名，形如table.attr
     * @param re 关系类型，如“=”
     */
    public NodeJoin(String table1_name,String table2_name,String field1_name,String field2_name,Re re)
    {
    	this.table1_name = table1_name;
    	this.table2_name = table2_name;
    	this.field1_name = field1_name;
    	this.field2_name = field2_name;
    	this.re = re;
    }
    
    public boolean equals(Object obj)
    {
    	if (!(obj instanceof NodeJoin))
    	{
    		return false;
    	}
    	NodeJoin node = (NodeJoin) obj;
    	if(node.table1_name.equals(this.table1_name)&&
    			node.table2_name.equals(this.table2_name)&&
    			node.field1_name.equals(this.field1_name)&&
    			node.field2_name.equals(this.field2_name)&&
    			node.re.equals(this.re))
    	{
    		return true;
    	}
    	return false;
    }
}
