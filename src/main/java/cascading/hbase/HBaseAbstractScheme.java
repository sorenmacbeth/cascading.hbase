package cascading.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public abstract class HBaseAbstractScheme extends Scheme {
    /** Field keyFields */
    protected Fields keyField;

    protected void validate() {
	if (keyField.size() != 1)
	    throw new IllegalArgumentException(
		    "may only have one key field, found: " + keyField.print());
    }

    protected void setSourceSink(Fields keyFields, Fields... columnFields) {
	Fields allFields = Fields.join(keyFields, Fields.join(columnFields)); // prepend

	setSourceFields(allFields);
	setSinkFields(allFields);
    }

    protected void setSourceInitFields(JobConf conf, String columns)
	    throws IOException {
	conf.setInputFormat(TableInputFormat.class);

	conf.set(TableInputFormat.COLUMN_LIST, columns);
    }

    protected void setSinkInitFields(JobConf conf) throws IOException {
	conf.setOutputFormat(TableOutputFormat.class);

	conf.setOutputKeyClass(ImmutableBytesWritable.class);
	conf.setOutputValueClass(Put.class);
    }

    protected Tuple sourceGetTuple(Object key) {
	Tuple result = new Tuple();

	ImmutableBytesWritable keyWritable = (ImmutableBytesWritable) key;

	result.add(Bytes.toString(keyWritable.get()));

	return result;
    }

    protected Put sinkGetPut(TupleEntry tupleEntry) {
	Tuple keyTuple = tupleEntry.selectTuple(keyField);

	byte[] keyBytes = Bytes.toBytes(keyTuple.getString(0));

	return new Put(keyBytes);

    }

    public abstract String[] getFamilyNames();
}
