package database.operation;

/**
 * 类型：类
 * 
 * 功能：查询操作中from语句中的子节点
 * 如：FROM t 则将t，t的id的数据提取出来
 * 
 */

public class NodeFrom
{
	public String table_name;//表名
	public int table_id;//数据表文件的id
	
    /**
     * 构造函数
     * @param table_name 表名
     * @param table_id 表的id
     */
    public NodeFrom(String table_name,int table_id)
    {
    	this.table_name = table_name;
    	this.table_id = table_id;
    }
}
