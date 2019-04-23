package database.structure;

/**
 * 类型：接口
 * 
 * 功能：数据库中的所有需要遍历元组的操作都需要实现该接口
 * 
 */
public interface ITupleIterator
{

	/**
	 * 开启迭代器
	 */
	public void start();

	/**
	 * 判断迭代器中是否还有下一个元素
	 */
	public boolean hasNext();
	
	/**
	 * 获取迭代器中下一个元素
	 */
	public Tuple next();
	
	/**
	 * 重置迭代器
	 */
	public void reset();
	
	/**
	 * 获取迭代器中元组的元数据类型
	 */
	public Schema getSchema();
	
	/**
	 * 停止迭代器
	 */
	public void stop();
}