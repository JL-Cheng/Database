package database.server;

import database.persist.DBPageBuffer;
import database.persist.DBTableManager;

/**
 * 类型：类
 * 
 * 功能：数据库类
 * 
 * (现在该类只对外提供一个静态对象，可后续扩展为可实例化的动态类)
 * 
 */
public class Database
{
	private static Database database = new Database();//数据库实例
    private DBTableManager table_manager;//数据表管理器
    private DBPageBuffer page_buffer;//缓冲区管理器

    /**
     * 构造函数
     * @param name 数据库名字
     */
    private Database()
    {
    	table_manager = new DBTableManager();
    	page_buffer = new DBPageBuffer(DBPageBuffer.DEFAULT_PAGES);
    }
    public static DBPageBuffer getPageBuffer() { return database.page_buffer; }

    public static DBTableManager getTableManager() { return database.table_manager; }

}
