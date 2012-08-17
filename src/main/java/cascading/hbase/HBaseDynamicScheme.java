package cascading.hbase;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public class HBaseDynamicScheme extends HBaseAbstractScheme {
    /** Field LOG */
    private static final Logger LOG = LoggerFactory
	    .getLogger(HBaseDynamicScheme.class);

    private Fields valueField;
    private String[] familyNames;

    public HBaseDynamicScheme(Fields keyField, Fields valueField,
    		String... familyNames) {
		setSourceSink(keyField, valueField);

		this.familyNames = familyNames;
		this.keyField = keyField;
		this.valueField = valueField;
		this.familyNames = familyNames;

		validate();

		if (valueField.size() != 1) {
		    throw new IllegalArgumentException(
			    "may only have one value field, found: "
				    + valueField.print());
		}
    }

    @SuppressWarnings("rawtypes")
	private String getTableFromTap(HBaseTap tap) {
    	Path tapPath = tap.getPath();
    	String tapPathStr = tapPath.toString();
    	// TODO: redefine exception
    	return tapPathStr.split("://")[1];
    }

    @Override
    public String[] getFamilyNames() {
	return familyNames;
    }

    /**
     * COPIED FROM HBASE SOURCE - NOT A BEST PRACTICE
     *
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan
     *            The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException
     *             When writing the scan fails.
     */
    /*
     * private static String convertScanToString(Scan scan) throws IOException {
     * ByteArrayOutputStream out = new ByteArrayOutputStream(); DataOutputStream
     * dos = new DataOutputStream(out); scan.write(dos); return
     * Base64.encodeBytes(out.toByteArray()); }
     */

	@Override
	public void sourceConfInit (
			FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap,
			JobConf conf) {
		setSourceInitFields(conf, " ");
	}

	@Override
	public void sinkConfInit(
			FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap,
			JobConf conf) {
		setSinkInitFields(conf);
		conf.set(TableOutputFormat.OUTPUT_TABLE, getTableFromTap((HBaseTap)tap));
	}

	@Override
	public boolean source(
			FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall
			) throws IOException {

		Object key = sourceCall.getContext()[0];
	    Object value = sourceCall.getContext()[1];
	    boolean hasNext = sourceCall.getInput().next(key, value);
	    if (!hasNext) { return false; }

		Tuple result = sourceGetTuple(key);
		Result row = (Result) value;
		result.add(row.getNoVersionMap());
		sourceCall.getIncomingEntry().setTuple(result);

		return true;
	}


	@Override
	public void sink(
			FlowProcess<JobConf> flowProcess,
			SinkCall<Object[], OutputCollector> sinkCall
			) throws IOException {

		TupleEntry tupleEntry = sinkCall.getOutgoingEntry();

		Put put = sinkGetPut(tupleEntry);

	    Tuple valueTuple = tupleEntry.selectTuple(valueField);
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> values =
			(NavigableMap<byte[], NavigableMap<byte[], byte[]>>)
			valueTuple.getObject(0);

		for (Entry<byte[], NavigableMap<byte[], byte[]>> keyValue : values
		.entrySet()) {
			for (Entry<byte[], byte[]> value : keyValue.getValue().entrySet()) {
				byte[] kv = check_null(keyValue.getKey());
				byte[] vk = check_null(value.getKey());
				byte[] v  = check_null(value.getValue());
				put.add(kv, vk, v);
			}
		}

		OutputCollector collector = sinkCall.getOutput();
		collector.collect(null, put);

	}

	private byte[] check_null(byte[] in) {
		if(null == in) {
			return HConstants.EMPTY_BYTE_ARRAY;
		} else
			return in;
	}
}
