package cascading.hbase.helper;

import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.util.Bytes;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class HBaseMapToTuples extends BaseOperation<Void> implements
	Function<Void> {

    Fields inputFields;

    public HBaseMapToTuples(Fields declaredFields, Fields inputFields) {
		super(2, declaredFields);

		this.inputFields = inputFields;
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
		String row = functionCall.getArguments().getString(inputFields.get(0));
		@SuppressWarnings("unchecked")
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> keyValueMaps = (NavigableMap<byte[], NavigableMap<byte[], byte[]>>) functionCall
			.getArguments().getObject(inputFields.get(1));

		for (Entry<byte[], NavigableMap<byte[], byte[]>> keyValue : keyValueMaps
			.entrySet()) {
		    for (Entry<byte[], byte[]> value : keyValue.getValue().entrySet()) {
			functionCall.getOutputCollector().add(
				new Tuple(row, Bytes.toString(keyValue.getKey()), Bytes
					.toString(value.getKey()), Bytes.toString(value
					.getValue())));
		    }
		}
    }

}
