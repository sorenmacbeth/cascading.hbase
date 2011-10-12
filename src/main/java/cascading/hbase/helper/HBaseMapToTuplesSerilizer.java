package cascading.hbase.helper;

import java.io.Serializable;

public interface HBaseMapToTuplesSerilizer<T> extends Serializable
{
	public byte[] toBytes(T obj);
	public T toT(byte[] bytes);
}
