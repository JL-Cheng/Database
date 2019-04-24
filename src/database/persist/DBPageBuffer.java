package database.persist;

import database.persist.DBPage.DBPageId;
import database.server.DatabaseManager;
import database.structure.Tuple;

/**
 * 类型：类
 * 
 * 功能：页的缓冲区，负责内存与磁盘之间的读写操作
 * 
 */
public class DBPageBuffer
{

    private static int PAGE_SIZE = 4096;//每一页的大小（字节数）
    public static int DEFAULT_PAGES = 50;//每一个缓冲区中的默认页数
    private DBPage[] buffer;//缓冲区
    private int old_page_id = 0;//旧的页的id（用于从缓冲区中移去操作）
    private DatabaseManager manager;
    
    /**
     * 构造函数，创建一个缓冲区来容纳一定数量的页
     * @param num_pages 缓冲区中最大页数
     */
    public DBPageBuffer(DatabaseManager m, int num_pages)
    {
    	this.manager = m;
    	createBuffer(num_pages);
    }
    /** 
     * 清空
     */
    public void clearAll() {
    	writeOperatedPages();
    	buffer = null;
    	old_page_id = 0;
    }
   /**
    * 创建一个缓冲区来容纳一定数量的页
    * @param num_pages
    */
    public void createBuffer(int num_pages) {
    	buffer = new DBPage[num_pages];
    }
    /**
     * 获取缓冲区中页的尺寸
     * @return 缓冲区中页的尺寸
     */
    public static int getPageSize()
    {
    	return PAGE_SIZE;
    }

    /**
     * 获取特定的页的内容：
     * 1.若该页在缓冲区中（命中），则直接返回；
     * 2.若该页不在缓冲区中（不命中）且缓冲区剩余空间较大，则将该页加入缓冲区中并返回；
     * 3.若该页不在缓冲区中（不命中）且缓冲区剩余空间无法放入该页，则先去除缓冲区中的一页，再将该页加入缓冲区中并返回；
     * @param page_id 页的id
     * @return 获取的页
     */
    public DBPage getPage(DBPageId page_id)
    {
    	int id = -1;
    	
    	for (int i=0; i<buffer.length; i++)
    	{
    		if (null == buffer[i])
    		{
    			id = i;
    		}
    		else if (page_id.equals(buffer[i].getId()))
    		{
    			return buffer[i];
    		}
    	}
    	if (id < 0)
    	{
    		deletePage();
    		return getPage(page_id);
    	}
    	else
    	{
    		buffer[id] = manager.database.getTableManager().getDatabaseFile(page_id.getTableId()).readPage(page_id);
    		return buffer[id];
    	}
    }
    /**
     * 向数据表中插入元组 
     * @param table_id 操作的表
     * @param tuple 插入的元组
     */
    public void insertTuple(int table_id, Tuple tuple)
    {
    	DBPage page = manager.database.getTableManager().getDatabaseFile(table_id).insertTuple(tuple);
    	page.setOperated(true);
    }

    /**
     * 从缓冲区中删除元组
     * @param tuple 待删除的元组
     */
    public void deleteTuple(Tuple tuple)
    {
    	DBPage page = manager.database.getTableManager().getDatabaseFile(tuple.getTupleId().getPageId().getTableId()).deleteTuple(tuple);
    	page.setOperated(true);
    }

    /**
     * 将缓冲区中所有修改过的页全部写入磁盘
     */
    public void writeOperatedPages()
    {
    	for (int i=0; i<buffer.length; i++)
    	{
    		if (buffer[i]!=null && buffer[i].isOperated()==true)
    		{
    			writePage(buffer[i].getId());
    		}
    	}
    }

    /**
     * 将缓冲区中一页写入磁盘
     * @param page_id 页的id
     */
    private void writePage(DBPageId page_id)
    {
    	for (int i=0; i<buffer.length; i++)
    	{
    		if (buffer[i] != null && buffer[i].getId().equals(page_id))
    		{
    			manager.database.getTableManager().getDatabaseFile(page_id.getTableId()).writePage(buffer[i]);
    			buffer[i].setOperated(false);
    			break;
    		}  		
    	}
    }

    /**
     * 当缓冲区满了之后，从缓冲区中选择一页并删除
     * （若该页之前被修改过，则会将其写回磁盘）
     */
    private void deletePage()
    {
    	for (int i=old_page_id; buffer[old_page_id] != null && buffer[old_page_id].isOperated();)
    	{
    		old_page_id = (old_page_id+1)%buffer.length;
    		if (i == old_page_id)
    		{
    			break;
    		}
    	}
    	if (buffer[old_page_id] != null)
    	{
    		try
    		{
    			writePage(buffer[old_page_id].getId());
				buffer[old_page_id] = null;
				old_page_id = (old_page_id+1)%buffer.length;
			}
            catch(Exception e)
            {
            	System.err.println(e.getMessage());
            }
    	}
    }
}
