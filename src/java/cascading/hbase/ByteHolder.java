package cascading.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class ByteHolder implements Comparable<ByteHolder> {
  private byte[] value;

  public ByteHolder(byte[] value) {
    this.value = value;
  }

  public byte[] getBytes() {
    return value;
  }

  @Override
  public String toString() {
    return Bytes.toString(value);
  }

  public int compareTo(ByteHolder o) {
    if (o instanceof ByteHolder) {
      return Bytes.compareTo(value, o.value);
    }
    return -1; // TODO Revisit this, unlikely to be correct.
  }
}
