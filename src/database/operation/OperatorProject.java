package database.operation;

import java.util.ArrayList;

import database.structure.Schema;
import database.structure.Tuple;
import database.structure.ITupleIterator;
import database.field.FieldType;

/**
 * 类型：类
 * 
 * 功能：投影操作
 * 
 */
public class OperatorProject extends Operator
{

	ITupleIterator tuples;//待投影的元组集
	Schema schema;//投影后的元数据
	ArrayList<Integer> fields_id;//需要投影出来的列id
	
	/**
	 * 构造函数
	 * @param tuples 待投影的元组集
	 * @param fields_id 需要投影出来的列的id
	 * @param fields_type 投影出来的列的类型列表，用于构建新的元数据
	 */
	public OperatorProject(ArrayList<Integer> fields_id, ArrayList<FieldType> fields_type,  ITupleIterator tuples)
	{
		this.tuples = tuples;
		this.fields_id = fields_id;
		
		String[] field_names = new String[fields_id.size()];
		Schema temp = tuples.getSchema();
		for(int i=0;i<field_names.length;i++)
		{
			field_names[i] = temp.getFieldName(fields_id.get(i));
		}
		int size = fields_type.size();
		int[] index = {};
		this.schema = new Schema(fields_type.toArray(new FieldType[size]),field_names,index, temp.string_len);
    }
    

	public void start() { tuples.start(); }
	
	public void reset() { tuples.reset(); }
	
	public void stop() { tuples.stop(); }
	
	public Schema getSchema() { return this.schema; }
	
	protected Tuple readNext()
	{
		while(tuples.hasNext())
		{
			Tuple tuple = tuples.next();
			Tuple result = new Tuple(schema);
			result.setTupleId(tuple.getTupleId());
			for(int i=0;i<schema.numFields();i++)
			{
				result.setField(i, tuple.getField(fields_id.get(i)));
			}
			return result;
		}
		return null;
	}

}
