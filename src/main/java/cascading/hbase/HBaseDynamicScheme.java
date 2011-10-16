package cascading.hbase;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private String getTableFromTap(Tap tap) {
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
     * COPIED FROM HBASE SOURCE - NOT A BEST PRACTISE
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
    public void sourceInit(Tap tap, JobConf conf) throws IOException {
	setSourceInitFields(conf, " ");

    }

    @Override
    public void sinkInit(Tap tap, JobConf conf) throws IOException {
	setSinkInitFields(conf);

	conf.set(TableOutputFormat.OUTPUT_TABLE, getTableFromTap(tap));

    }

    @Override
    public Tuple source(Object key, Object value) {
	Result row = (Result) value;

	Tuple tuple = sourceGetTuple(key);
	tuple.add(row.getNoVersionMap());

	return tuple;

    }

    @SuppressWarnings("unchecked")
    @Override
    public void sink(TupleEntry tupleEntry,
	    @SuppressWarnings("rawtypes") OutputCollector outputCollector)
	    throws IOException {
	Tuple valueTuple = tupleEntry.selectTuple(valueField);
	NavigableMap<byte[], NavigableMap<byte[], byte[]>> values = (NavigableMap<byte[], NavigableMap<byte[], byte[]>>) valueTuple
		.getObject(0);
	Put put = sinkGetPut(tupleEntry);

	for (Entry<byte[], NavigableMap<byte[], byte[]>> keyValue : values
		.entrySet()) {
	    for (Entry<byte[], byte[]> value : keyValue.getValue().entrySet()) {
		put.add(keyValue.getKey(), value.getKey(), value.getValue());
	    }
	}

	outputCollector.collect(null, put);

    }

}
