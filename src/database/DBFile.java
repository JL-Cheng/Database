package database;

import java.io.*;

import database.DBPage.DBPageId;

/**
 * 类型：类
 * 
 * 功能：
 *     - 存于磁盘的数据库文件，每一张数据表都被表示为一个数据库文件。
 *     - 数据库文件的数据会被先读取分页并存于缓冲区中，而不会直接操作。
 *     - 每一个数据库文件都有一个id与元数据与之对应。
 *     
 */
public class DBFile
{

	private File file;
	private Schema schema;
	
    /**
     * 构造函数
     * @param file 存于磁盘的对应于该表的数据库文件
     * @param schema 该表对应的元数据
     */
    public DBFile(File file, Schema schema)
    {
    	this.file = file;
    	this.schema = schema;
    }

    public int getId() { return file.getAbsoluteFile().hashCode(); }

    public Schema getSchema() { return schema; }
    
    public int numPages() { return (int)(file.length()/DBPageBuffer.getPageSize()); }

    /**
     * 从磁盘中读入某一页
     * @param page_id 页对应的id
     * @return 读入的页
     */
    public DBPage readPage(DBPageId page_id)
    {
    	if (getId() == page_id.getTableId())
    	{
    		int page_no = page_id.getPageNumber();
    		
    		if (page_no>=0 && page_no<numPages())
    		{
    			byte[] bytes = DBPage.createEmptyPageData();
	    			
    			try
    			{
    				RandomAccessFile access_file = new RandomAccessFile(file, "r");
    				access_file.seek(1L*DBPageBuffer.getPageSize()*page_id.getPageNumber());
    				access_file.read(bytes, 0, DBPageBuffer.getPageSize());
    				DBPageId n_page_id = new DBPageId(page_id.getTableId(),page_id.getPageNumber());				
					access_file.close();
					return new DBPage(n_page_id, bytes);
    			}
    			catch (Exception e)
    			{
    				System.err.println(e.getMessage());
    			}
    		}
    	}
    	return null;
    }

    /**
     * 将某一页写入磁盘
     * @param page 要写入的页
     *
     */
    public void writePage(DBPage page)
    {
		int page_no = page.getId().getPageNumber();
		
		if (page_no>=0 && page_no<=numPages())
		{		
			try
			{
				RandomAccessFile access_file = new RandomAccessFile(file, "rw");
				access_file.seek(1L*DBPageBuffer.getPageSize()*page_no);
				access_file.write(page.getPageData(), 0, DBPageBuffer.getPageSize());
				access_file.close();
			}
			catch (Exception e)
			{
				System.err.println(e.getMessage());
			}
		}
    }

    /**
     * 将某一元组插入到数据库文件中
     * @param tuple 将要插入的元组
     * @return 被修改的页
     */
    public DBPage insertTuple(Tuple tuple)
    {
    	DBPage page = null;
    	DBPageBuffer pool = Database.getPageBuffer();
    	int table_id = getId();
    	int page_no = 0;
    	
    	while (page_no<numPages())
    	{
    		page = (DBPage)pool.getPage(new DBPageId(table_id,page_no));
    		
    		if (page.getNumEmptyTuples() > 0)
    		{
    			page.insertTuple(tuple);
    			break;
    		}
    		page_no++;
    	}
    	if (page_no == numPages())
    	{
    		page = new DBPage(new DBPageId(table_id, page_no), DBPage.createEmptyPageData());  		
    		page.insertTuple(tuple);
    		writePage(page);
    	}
        return page;
    }

    /**
     * 将某一元组从数据库文件中删除
     * @param tuple 将要删除的元组
     * @return 被修改的页
     */
    public DBPage deleteTuple(Tuple tuple)
    {
    	DBPageBuffer pool = Database.getPageBuffer();
    	DBPage page = (DBPage)pool.getPage(tuple.getTupleId().getPageId());   	
    	page.deleteTuple(tuple);
        return page;
    }

}

