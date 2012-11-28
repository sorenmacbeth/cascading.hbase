package cascading.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SourceCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("serial")
public abstract class HBaseAbstractScheme extends
	Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
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

    protected void setSourceInitFields(JobConf conf, String columns) {
    	JobConf jobconf = (JobConf)conf;
    	jobconf.set("mapred.input.format.class", TableInputFormat.class.getName());
    	jobconf.set(TableInputFormat.COLUMN_LIST, columns);
    }

	protected void setSinkInitFields(JobConf conf) {
    	conf.set("mapred.output.format.class", TableOutputFormat.class.getName());
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

    @Override
    public void sourcePrepare(FlowProcess<JobConf> flowProcess,
        SourceCall<Object[], RecordReader> sourceCall) {
      Object[] pair =
          new Object[]{sourceCall.getInput().createKey(), sourceCall.getInput().createValue()};

      sourceCall.setContext(pair);
    }

    @Override
    public void sourceCleanup(FlowProcess<JobConf> flowProcess,
        SourceCall<Object[], RecordReader> sourceCall) {
      sourceCall.setContext(null);
    }


}
