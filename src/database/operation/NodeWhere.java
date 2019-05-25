package database.operation;

import javax.swing.text.StyledEditorKit.BoldAction;

import database.field.FieldCompare.Re;

/**
 * 类型：类
 * 
 * 功能：where语句中的子节点 用于select update delete
 * 如：WHERE person.id = 1则将person.id = 1的数据提取出来
 * 注：查询操作中对于 WHERE table1.attr1 = table2.attr2，将其变为JOIN节点
 * 
 */

public class NodeWhere
{
	public String table_name;//表名
	public String field_name;//列名，形如table.attr
	public String right_table_name;//另一个列名，形如table.attr
	public String right_field_name;//另一个列名，形如table.attr
	public Re re;//关系类型，如“=”
	public String cons;//常量
	boolean isConsType = true;//标注右边是否是常量类型
	
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
    	this.right_field_name = "";
    	this.right_table_name = "";
    }
    
    public NodeWhere(String table_name,String field_name,Re re,String right_table, String right_field)
    {
    	this.table_name = table_name;
    	this.field_name = field_name;
    	this.re = re;
    	this.isConsType = false;
		this.right_field_name = right_field;   
		this.right_table_name = right_table;   
		this.cons = "";
    }
    
    public boolean equals(Object obj)
    {
    	if (!(obj instanceof NodeWhere))
    	{
    		return false;
    	}
    	NodeWhere node = (NodeWhere) obj;
    	if(node.field_name.equals(this.field_name)&&
    			node.table_name.equals(this.table_name)&&
    			node.re.equals(this.re)&&
    			node.cons.equals(this.cons)&&
    			node.isConsType== this.isConsType&&
    			node.right_field_name.equals(this.right_field_name)&&
    			node.right_table_name.equals(this.right_table_name))
    	{
    		return true;
    	}
    	return false;
    }
}
