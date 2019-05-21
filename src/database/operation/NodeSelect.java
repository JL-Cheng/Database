package database.operation;

/**
 * 类型：类
 * 
 * 功能：查询操作中select语句中的子节点
 * 如：SELECT table.attr,则将table.attr的数据提取出来
 * 
 */

public class NodeSelect
{
	public String field_name;//列名，table.attr的形式
	
    /**
     * 构造函数
     * @param field_name 列名
     */
    public NodeSelect(String field_name)
    {
    	this.field_name = field_name;
    }
    
    public boolean equals(Object obj)
    {
    	if (!(obj instanceof NodeSelect))
    	{
    		return false;
    	}
    	NodeSelect node = (NodeSelect) obj;
    	if(node.field_name.equals(this.field_name))
    	{
    		return true;
    	}
    	return false;
    }
}
