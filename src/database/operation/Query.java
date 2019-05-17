package database.operation;

import java.util.Vector;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;

import database.field.FieldCompare.Re;
import database.server.DatabaseManager;
import database.structure.Schema;
import database.structure.ITupleIterator;
import database.persist.DBFile;
import database.field.*;
import database.operation.OperatorJoin.JoinCompare;

/**
 * 类型：类
 * 
 * 功能：该类表示解析后的查询语句的结构，其中存放了各种node节点
 * 
 */

public class Query
{
	private Vector<NodeFrom> from_nodes;//FROM语句中的节点
	private Vector<NodeJoin> join_nodes;//JOIN语句中的节点
	private Vector<NodeSelect> select_nodes;//SELECT语句中的节点
	private Vector<NodeWhere> where_nodes;//WHERE语句中的节点
	
	private String query_string;//查询语句
	private String error_message;//解析过程中是否出现错误，若是""则说明没有错误
	
	private DatabaseManager manager;
	
	 /**
     * 构造函数
     */
    public Query(DatabaseManager m)
    {
    	from_nodes = new Vector<NodeFrom>();
    	join_nodes = new Vector<NodeJoin>();
    	select_nodes = new Vector<NodeSelect>();
    	where_nodes = new Vector<NodeWhere>();
    	query_string = "";
    	error_message = "";
    	this.manager = m;
    }
    
    public void setQuery(String query_string) { this.query_string = query_string; }
    public String getQuery() { return query_string; }
    public void setErrorMessage(String error_message) { this.error_message = this.error_message + error_message + "\n"; }
    public String getErrorMessage() { return error_message; }
    
    /**
     * 增加一个WHERE节点
     * @param field_name 表列名
     * @param re 关系
     * @param cons 常量
     */
    public void addNodeWhere(String field_name, Re re, String cons) throws Exception
    {
    	String[] result = getTableAndFieldName(field_name);
    	NodeWhere node = new NodeWhere(result[0],result[1], re, cons);
    	if(!where_nodes.contains(node))
    	{
    		where_nodes.addElement(node);
    	}  
    }
    
    /**
     * 增加一个JOIN节点
     * @param name1 第一张表列名（或表名），非空
     * @param name2 第二张表列名（或表名），非空
     * @param re 关系
     */
    public void addNodeJoin(String name1,String name2, Re re) throws Exception
    {
    	NodeJoin node = null;
    	String[] result1 = null;
    	String[] result2 = null;
    	//若只是两张表普通连接
    	if(name1.split("[.]").length==1&&name2.split("[.]").length==1&&re==null)
    	{
    		result1 = getTableAndFieldName(name1+".*");
        	result2 = getTableAndFieldName(name2+".*");
    		node = new NodeJoin(result1[0], result2[0], null,null, null);
    	}
    	//若是两个表的列名关系
    	else if(name1.split("[.]").length==2&&name2.split("[.]").length==2&&re!=null)
    	{
    		result1 = getTableAndFieldName(name1);
        	result2 = getTableAndFieldName(name2);
    		node = new NodeJoin(result1[0], result2[0], result1[1],result2[1], re);
    	}
    	else
    	{
    		throw new Exception("Parse error on TABLE JOIN.");
    	}
    	if(!join_nodes.contains(node))
    	{
    		join_nodes.addElement(node);
    	}  
    }

    /**
     * 增加一个FROM节点
     * @param table_id 表的id
     * @param table_name 表名
     */
    public void addNodeFrom(int table_id,String table_name) throws Exception
    {
    	NodeFrom node = new NodeFrom(table_name, table_id);
    	if(!from_nodes.contains(node))
    	{
    		from_nodes.addElement(node);
    	}  	
    }

    /**
     * 增加一个SELECT节点
     * @param field_name 列名
     */
    public void addNodeSelect(String field_name) throws Exception
    {
    	String result = getTableAndFieldName(field_name)[1];
    	NodeSelect node = new NodeSelect(result);
    	if(!select_nodes.contains(node))
    	{
    		select_nodes.addElement(node);
    	} 
    }
    
    /**
     * 传入的field有四种可能形式：1、table.field 2、table.*（表示该表的所有列）3、null.field（表示某张表的一列，但是还不知道是哪张表）4、null.*（表示所有列）
     * 对于第一种形式，判断table是否在FROM中出现过，以及是否有该field即可
     * 对于第二种形式，判断table是否在FROM中出现过即可
     * 对于第三种形式，则查找有该列的表，转换为table.field形式返回
     * 对于第四种形式，直接返回即可
     * 此方法必须要在已读取完FROM子句之后才可调用
     * @param name 传入字符串
     * @return 长度为2的字符串列表，分别为表名与列名（table.attr）
     */
    private String[] getTableAndFieldName(String name) throws Exception
    {
    	String[] names = name.split("[.]");
    	Iterator<NodeFrom> from_node_it = from_nodes.iterator();
    	if(names.length!=2)
    	{
    		throw new Exception("Field name is wrong: " + name);
    	}
    	//第一种形式table.field
    	if(!(names[1].equals("*")) && !(names[0].equals("null")))
    	{
    		String table_name = null;
    		while(from_node_it.hasNext())
    		{
    			NodeFrom node = from_node_it.next();
    			Schema s = manager.database.getTableManager().getSchema(node.table_id);
    			int id = s.getFieldIndex(name);
    			if(id==-1)
    			{
    				continue;
    			}
    			else if(table_name != null)
    			{
    				throw new Exception("Field " + name + " appears in different tables.");
    			}
    			else
    			{
    				table_name = node.table_name;
    			}
    		}
    		if(table_name != null && table_name.equals(names[0]))
    		{
    			String[] result = {table_name,name};
    			return result;
    		}
    		else
    		{
    			throw new Exception("Field " + name + " does not exist.");
    		}

    	}
    	//第二种形式table.*
    	if(names[1].equals("*") && !(names[0].equals("null")))
    	{
    		String table_name = null;
    		while(from_node_it.hasNext())
    		{
    			NodeFrom node = from_node_it.next();
    			if(names[0].equals(node.table_name))
    			{
    				table_name = names[0];
    				break;
    			}
    		}
    		if(table_name != null)
    		{
    			names[1] = name;
    			return names;
    		}
    		else
    		{
    			throw new Exception("Table " + names[0] + " does not exist.");
    		}
    	}
    	//第三种形式null.field
    	if(!(names[1].equals("*")) && names[0].equals("null"))
    	{
       		String table_name = null;
    		while(from_node_it.hasNext())
    		{
    			NodeFrom node = from_node_it.next();
    			Schema s = manager.database.getTableManager().getSchema(node.table_id);
    			String temp_name = manager.database.getTableManager().getTableName(node.table_id);
    			int id = s.getFieldIndex(temp_name+"."+names[1]);
    			if(id==-1)
    			{
    				continue;
    			}
    			else if(table_name != null)
    			{
    				throw new Exception("Field " + name + " appears in different tables.");
    			}
    			else
    			{
    				table_name = temp_name;
    			}
    		}
    		if(table_name != null)
    		{
    			String[] result = {table_name,table_name+"."+name};
    			return result;
    		}
    		else
    		{
    			throw new Exception("Field " + name + " does not exist.");
    		}

    	}
    	//第四种形式null.*
    	if(names[1].equals("*") && names[0].equals("null"))
    	{
    		String[] result = {null,"null.*"};
    		return result;
    	}

    	return null;
    }

    /**
     * 将NodeJoin中的两张表合并后返回
     * @return 元组迭代器
     */
    public ITupleIterator getJoinTable(NodeJoin join_node,ITupleIterator tuples1, ITupleIterator tuples2) throws Exception
    {
    	JoinCompare jc = null;
    	OperatorJoin join = null;
    	if(join_node.field1_name==null&&join_node.field2_name==null&&join_node.re==null)
    	{
        	jc = new JoinCompare(0,0,null);
        	join = new OperatorJoin(jc,tuples1,tuples2);
    	}
    	else if(join_node.field1_name!=null&&join_node.field2_name!=null&&join_node.re!=null)
    	{
    		int field1_id=0, field2_id=0;
        	
        	field1_id = tuples1.getSchema().getFieldIndex(join_node.field1_name); 
        	field2_id = tuples2.getSchema().getFieldIndex(join_node.field2_name);  	
        	if(field1_id == -1)
        	{
        		throw new Exception("Unknown field " + join_node.field1_name);
        	}
        	if(field2_id == -1)
        	{
        		throw new Exception("Unknown field " + join_node.field2_name);
        	}
        	jc = new JoinCompare(field1_id,field2_id,join_node.re);
        	join = new OperatorJoin(jc,tuples1,tuples2);
    	}     
        return join;
    }
    
    /**
     * 该函数将处理该类中的各种向量节点，进行主要的查询操作之后，返回一个元组迭代器
     * @return 元组迭代器
     */
    public ITupleIterator operateQuery() throws Exception
    {
    	//表名与该表的元组迭代器相匹配的map
    	HashMap<String,ITupleIterator> tables_operation = new HashMap<String,ITupleIterator>();
    	//表名与表名相匹配的map，当两张表合并之后将两张表改为同名
    	HashMap<String,String> tables_equiv = new HashMap<String,String>();
    	
    	//第一步，遍历所有FROM节点，将用到的表加入到tables_operation中
    	Iterator<NodeFrom> from_node_it = from_nodes.iterator();
    	while (from_node_it.hasNext())
    	{
    		NodeFrom from_node = from_node_it.next();
    		DBFile table = manager.database.getTableManager().getDatabaseFile(from_node.table_id);
			if(table == null)
			{
				throw new Exception("Unknown table "+from_node.table_name);
			}
			ITupleIterator table_it = table.iterator();
			String table_name = manager.database.getTableManager().getTableName(from_node.table_id);
			tables_operation.put(table_name,table_it);
		}
        
    	//第二步，遍历所有WHERE节点，形如：table.attr = cons
    	Iterator<NodeWhere> where_node_it = where_nodes.iterator();
    	while(where_node_it.hasNext())
    	{
    		NodeWhere where_node = where_node_it.next();
    		ITupleIterator table_it = tables_operation.get(where_node.table_name);
    		if(table_it == null)
    		{
    			throw new Exception("Unknown table "+where_node.table_name);
    		}
    		
    		//将常数转换为field类型
    		IField field;
    		FieldType type;
    		Schema schema = tables_operation.get(where_node.table_name).getSchema();
    		
    		int field_index = schema.getFieldIndex(where_node.field_name);
    		if(field_index == -1)
    		{
    			throw new Exception("Unknown field "+ where_node.field_name);
    		}
    		type = schema.getFieldType(field_index);
    		if(type == FieldType.DOUBLE_TYPE)
    		{
    			field = new FieldDouble(Double.valueOf(where_node.cons));
    		}
    		else if(type == FieldType.FLOAT_TYPE)
    		{
    			field = new FieldFloat(Float.valueOf(where_node.cons));
    		}
    		else if(type == FieldType.INT_TYPE)
    		{
    			field = new FieldInt(Integer.valueOf(where_node.cons));
    		}
    		else if(type == FieldType.LONG_TYPE)
    		{
    			field = new FieldLong(Long.valueOf(where_node.cons));
    		}
    		else
    		{
    			field = new FieldString(where_node.cons,FieldType.STRING_LEN);
    		}			
    		FieldCompare field_cp = new FieldCompare(field_index,where_node.re,field);
    		
    		//将筛选操作加入到tables_operation中
    		OperatorFilter filter = new OperatorFilter(field_cp,table_it);
    		tables_operation.put(where_node.table_name,filter);  
    	}
    	
    	//第三步，遍历所有JOIN节点
    	Iterator<NodeJoin> join_node_it = join_nodes.iterator();
    	while(join_node_it.hasNext())
    	{
    		NodeJoin join_node = join_node_it.next();
    		ITupleIterator table1_it, table2_it;
    		String table1_name,table2_name;
    		
    		if(tables_equiv.get(join_node.table1_name)!=null)
    		{
    			table1_name = tables_equiv.get(join_node.table1_name);
    		}
    		else
    		{
    			table1_name = join_node.table1_name;
    		}
    		if(tables_equiv.get(join_node.table2_name)!=null)
    		{
    			table2_name = tables_equiv.get(join_node.table2_name);
    		}
    		else
    		{
    			table2_name = join_node.table2_name;
    		}
    		table1_it = tables_operation.get(table1_name);
    		table2_it = tables_operation.get(table2_name);
    		if(table1_it == null)
    		{
    			throw new Exception("Unknown table "+join_node.table1_name);
    		}
    		if(table2_it == null)
    		{
    			throw new Exception("Unknown table "+join_node.table2_name);
    		}
        	
    		ITupleIterator temp = getJoinTable(join_node,table1_it,table2_it);
    		tables_operation.put(table1_name,temp);
        	 		
    		//将所有出现过表2名字的地方都替换为表1的名字
    		tables_operation.remove(table2_name);
    		tables_equiv.put(table2_name,table1_name);
    		for(Map.Entry<String, String> s:tables_equiv.entrySet())
    		{
    			String str_val = s.getValue();
    			if(str_val.equals(table2_name))
    			{
    				s.setValue(table1_name);
    			}
    		}
    	}
    	//第三步结束之后，此时的表应当已经被合成一张表了，若不是则说明出现错误
    	if(tables_operation.size()>1)
    	{
    		throw new Exception("Something is wrong when QUERY");
    	}
    	
    	//第四步，遍历所有SELECT节点，决定最终合成的表应当输出哪几列（投影操作）
    	ITupleIterator final_table = (ITupleIterator)(tables_operation.entrySet().iterator().next().getValue());
    	final_table.start();
    	ArrayList<Integer> final_fields_id = new ArrayList<Integer>();
        ArrayList<FieldType> final_types = new ArrayList<FieldType>();
        Iterator<NodeSelect> select_node_it = select_nodes.iterator();
    	while(select_node_it.hasNext())
    	{
    		NodeSelect select_node = select_node_it.next();
    		String[] names = select_node.field_name.split("[.]");
    		//第一种情况：table.field形式
    		if(!(names[0].equals("null"))&&!(names[1].equals("*")))
    		{
    			Schema schema = final_table.getSchema();
    			int id = schema.getFieldIndex(select_node.field_name);
    			if(id == -1)
    			{
    				throw new Exception("Unknown field "+ select_node.field_name + " in SELECT");
    			}
    			else
    			{
    				final_fields_id.add(id);
    				final_types.add(schema.getFieldType(id));
    			}
    		}
    		//第二种情况：table.*形式
    		else if(!(names[0].equals("null"))&&names[1].equals("*"))
    		{
    			Schema schema = final_table.getSchema();
    			Iterator<Schema.SchemaItem> schema_it = schema.iterator();
    			while(schema_it.hasNext())
    			{
    				Schema.SchemaItem schema_item = schema_it.next();
    				if(schema_item.field_name.split("[.]")[0].equals(names[0]))
    				{
    	    			int id = schema.getFieldIndex(schema_item.field_name);
    	    			if(id == -1)
    	    			{
    	    				throw new Exception("Unknown field "+ schema_item.field_name + " in SELECT");
    	    			}
    	    			else
    	    			{
    	    				final_fields_id.add(id);
    	    				final_types.add(schema.getFieldType(id));
    	    			}
    				}
    			}
    		}
    		//第三种情况：null.*形式
    		else if(names[0].equals("null")&&names[1].equals("*"))
    		{
    			Schema schema = final_table.getSchema();
    			for(int i = 0;i<schema.numFields();i++)
    			{
    				final_fields_id.add(i);
    				final_types.add(schema.getFieldType(i));
    			}
    		}
    		else
    		{
    			throw new Exception("Something is wrong when QUERY");
    		}
    	}
    	
    	return new OperatorProject(final_fields_id, final_types, final_table);
    }

}
