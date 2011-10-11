package cascading.hbase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class HBaseDynamicScheme extends HBaseGenScheme
{
	/** Field LOG */
	private static final Logger LOG = LoggerFactory.getLogger(HBaseDynamicScheme.class);
	
	private Fields keyField;
	private Fields valueField;
	private String[] familyNames;
	
	public HBaseDynamicScheme(Fields keyField, Fields valueField, String... familyNames)	{
		setSourceFields(Fields.join(keyField, valueField));
		setSinkFields(Fields.join(keyField, valueField));
		
		this.familyNames = familyNames;
		this.keyField = keyField;
		this.valueField = valueField;
		this.familyNames = familyNames;
		
		if (keyField.size() != 1)	{
			throw new IllegalArgumentException("may only have one key field, found: " + keyField.print());
		}
		
		if (valueField.size() != 1)	{
			throw new IllegalArgumentException("may only have one value field, found: " + valueField.print());
		}
	}
	
	private String getTableFromTap(Tap tap)		{
		Path tapPath = tap.getPath();
		String tapPathStr = tapPath.toString();
		// TODO: redefine exception
		return tapPathStr.split("://")[1];
	}
	
	public String[] getFamilyNames() {
		return familyNames;
	}
	
	/**
	 * COPIED FROM HBASE SOURCE - NOT A BEST PRACTISE
	 * 
	   * Writes the given scan into a Base64 encoded string.
	   *
	   * @param scan  The scan to write out.
	   * @return The scan saved in a Base64 encoded string.
	   * @throws IOException When writing the scan fails.
	   */
	  private static String convertScanToString(Scan scan) throws IOException {
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    DataOutputStream dos = new DataOutputStream(out);
	    scan.write(dos);
	    return Base64.encodeBytes(out.toByteArray());
	  }
	  
	@Override
    public void sourceInit(Tap tap, JobConf conf) throws IOException
    {

		conf.setInputFormat(TableInputFormat.class);

		String columns = "testCF:c_1";
		LOG.debug("sourcing from columns: {}", columns);

		conf.set(TableInputFormat.COLUMN_LIST, " ");
	    
    }

	@Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException
    {
		conf.setOutputFormat(TableOutputFormat.class);
		
		conf.set(TableOutputFormat.OUTPUT_TABLE, getTableFromTap(tap));
		
		conf.setOutputKeyClass(ImmutableBytesWritable.class);
		conf.setOutputValueClass(Put.class);
	    
    }

	@Override
    public Tuple source(Object key, Object value)
    {
		Tuple result = new Tuple();

		ImmutableBytesWritable keyWritable = (ImmutableBytesWritable) key;
		Result row = (Result) value;

		result.add(Bytes.toString(keyWritable.get()));
		result.add(row.getNoVersionMap());
		return result;
    }

	@SuppressWarnings("unchecked")
    @Override
    public void sink(TupleEntry tupleEntry, @SuppressWarnings("rawtypes") OutputCollector outputCollector) throws IOException
    {
		Tuple keyTuple= tupleEntry.selectTuple(keyField);
		Tuple valueTuple = tupleEntry.selectTuple(valueField);
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> values = (NavigableMap<byte[], NavigableMap<byte[], byte[]>>) valueTuple.getObject(0); 
		byte[] keyBytes = Bytes.toBytes(keyTuple.getString(0));
		Put put = new Put(keyBytes);
		
		for (Entry<byte[], NavigableMap<byte[], byte[]>>  keyValue : values.entrySet())	{
			for (Entry<byte[], byte[]>  value: keyValue.getValue().entrySet())	{
				put.add(keyValue.getKey(), value.getKey(), value.getValue());
			}
		}
		
		outputCollector.collect(null, put);
	    
    }

}
