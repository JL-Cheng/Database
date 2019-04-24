package database.server;

import database.persist.DBPageBuffer;
import database.persist.DBTableManager;

/**
 * 类型：类
 * 
 * 功能：数据库类
 * 
 * 
 * 
 */
public class Database
{
    private DBTableManager table_manager;//数据表管理器
    private DBPageBuffer page_buffer;//缓冲区管理器

    /**
     * 构造函数
     * @param name 数据库名字
     */
    public Database(DatabaseManager m)
    {
    	table_manager = new DBTableManager(m);
    	page_buffer = new DBPageBuffer(m, DBPageBuffer.DEFAULT_PAGES);
    }
    public void clearAll() {
    	close();
    	table_manager.clearAll();
    	page_buffer.clearAll();
    	page_buffer.createBuffer(DBPageBuffer.DEFAULT_PAGES);
    }
    /**
     * 关闭数据库，持久化表的元信息和数据，退出进程需要显示调用
     */
    public void close() {
    	table_manager.writeSchema();
    	page_buffer.writeOperatedPages();
    }
    public DBPageBuffer getPageBuffer() { return page_buffer; }

    public DBTableManager getTableManager() { return table_manager; }

}
