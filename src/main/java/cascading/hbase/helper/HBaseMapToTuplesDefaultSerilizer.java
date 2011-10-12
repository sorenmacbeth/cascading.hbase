package cascading.hbase.helper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;

import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("serial")
class HBaseMapToTuplesDefaultSerilizer	<T> implements HBaseMapToTuplesSerilizer<T> {
	private Method methodToBytes;
	private Method methodToT;
	
	public HBaseMapToTuplesDefaultSerilizer	(Class<T> paramTypeClass) throws SecurityException, NoSuchMethodException	{
        Class<T> classType = paramTypeClass;
		methodToBytes = Bytes.class.getMethod("toBytes", classType);
		methodToT = Bytes.class.getMethod("to" + classType.getSimpleName(), byte[].class);
		
	}
	
	@Override
    public byte[] toBytes(T obj)	{
		
		try
        {
	        return (byte[]) methodToBytes.invoke(null, obj);
        }
        catch (Exception exception)
        {
	        return null;
        }
	}

	@SuppressWarnings("unchecked")
    @Override
    public T toT(byte[] bytes)
    {
		try
        {
	        return (T) methodToT.invoke(null, bytes);
        }
        catch (Exception exception)
        {
	        return null;
        }
    }
	
	private void readObject(
		    ObjectInputStream aStream
		  ) throws IOException, ClassNotFoundException {
			String methodToBytesName = (String) aStream.readObject();
			@SuppressWarnings("rawtypes")
            Class[] methodToBytesParams = (Class[]) aStream.readObject();
			String methodToTName = (String) aStream.readObject();
			@SuppressWarnings("rawtypes")
            Class[] methodToTParams = (Class[]) aStream.readObject();
			try	{
				methodToBytes = Bytes.class.getMethod(methodToBytesName, methodToBytesParams);
				methodToT = Bytes.class.getMethod(methodToTName, methodToTParams);
			} catch (Exception e)	{
				throw new IOException(e);
			}
		  }

		  /**
		  * Custom serialization is needed.
		  */
		  private void writeObject(ObjectOutputStream aStream) throws IOException {
			aStream.writeObject(methodToBytes.getName());
			aStream.writeObject(methodToBytes.getParameterTypes());
			aStream.writeObject(methodToT.getName());
			aStream.writeObject(methodToT.getParameterTypes());
			
		  }
		
}