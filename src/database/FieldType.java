package database;

import java.io.*;

/**
 * 类型：枚举类
 * 
 * 功能：枚举所有的数据类型，统一管理
 * 
 */
public enum FieldType implements Serializable
{
	//Int类型
    INT_TYPE()
    {
        public int getLen()
        {
            return 4;
        }

        public IField parse(DataInputStream instream)
        {
            try
            {
            	return new FieldInt(instream.readInt());
            }
            catch (IOException e)
            {
            	System.err.println(e.getMessage());
            	return null;
            }
        }
    },
	//Long类型
    LONG_TYPE()
    {
        public int getLen()
        {
            return 8;
        }

        public IField parse(DataInputStream instream)
        {
            try
            {
            	return new FieldLong(instream.readLong());
            }
            catch (IOException e)
            {
            	System.err.println(e.getMessage());
            	return null;
            }
        }
    },
	//Float类型
    FLOAT_TYPE()
    {
        public int getLen()
        {
            return 4;
        }

        public IField parse(DataInputStream instream)
        {
            try
            {
            	return new FieldFloat(instream.readFloat());
            }
            catch (IOException e)
            {
            	System.err.println(e.getMessage());
            	return null;
            }
        }
    },
	//Double类型
    DOUBLE_TYPE()
    {
        public int getLen()
        {
            return 8;
        }

        public IField parse(DataInputStream instream)
        {
            try
            {
            	return new FieldDouble(instream.readDouble());
            }
            catch (IOException e)
            {
            	System.err.println(e.getMessage());
            	return null;
            }
        }
    },
    //String类型
    STRING_TYPE()
    {
	    public int getLen()
	    {
	        return STRING_LEN + 4;
	    }
	
	    public IField parse(DataInputStream instream)
	    {
	        try
	        {
	            int str_len = instream.readInt();
	            byte bs[] = new byte[str_len];
	            instream.read(bs);
	            instream.skipBytes(STRING_LEN-str_len);
	            return new FieldString(new String(bs), STRING_LEN);
	        }
	        catch (IOException e)
	        {
            	System.err.println(e.getMessage());
            	return null;
	        }
	    }
    };
	
    public static int STRING_LEN = 128;//规定的字符串最大长度
	
	/**
	 * @return 这种数据类型所需的字节数.
	 */
	public abstract int getLen();
	
	/**
	 * @param instream 输入数据流
	 * @return 从数据流中读入的某数据类型对象
	 */
	 public abstract IField parse(DataInputStream instream);
}
