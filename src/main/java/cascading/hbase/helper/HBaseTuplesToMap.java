package cascading.hbase.helper;

import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class HBaseTuplesToMap<CF, C, V> extends BaseOperation<HBaseTuplesToMap.AggregatorWriterTuplesListContext>
        implements Aggregator<HBaseTuplesToMap.AggregatorWriterTuplesListContext>
{

	static class AggregatorWriterTuplesListContext
	{
		public NavigableMap<byte[], NavigableMap<byte[], byte[]>> keyValueMap = new TreeMap<byte[], NavigableMap<byte[], byte[]>>(Bytes.BYTES_COMPARATOR);

		public String key;
		
		public void addElements(byte[] cf, byte[] column, byte[] value)	{ 
			try	{
				keyValueMap.get(cf).put(column, value);
			} catch (NullPointerException exeption)	{
				if (null != cf)		{
					keyValueMap.put(cf, new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR));
					addElements(cf, column, value);
				}
			}
		}

	};

	private Fields rowField;

	private Fields columnField;

	private Fields valueField;

	private Fields cfName;

	private HBaseMapToTuplesSerilizer<CF> hBaseMapToTuplesSerilizerCF;
	private HBaseMapToTuplesSerilizer<C> hBaseMapToTuplesSerilizerC;
	private HBaseMapToTuplesSerilizer<V> hBaseMapToTuplesSerilizerV;

	public HBaseTuplesToMap(Fields declaredFields, Fields cfName, Fields rowField, Fields columnField,
	        Fields valueField, 
	        HBaseMapToTuplesSerilizer<CF> hBaseMapToTuplesSerilizerCF,  
	        HBaseMapToTuplesSerilizer<C> hBaseMapToTuplesSerilizerC,
	        HBaseMapToTuplesSerilizer<V> hBaseMapToTuplesSerilizerV)
	{
		super(declaredFields);

		this.rowField = rowField;
		this.cfName = cfName;
		this.columnField = columnField;
		this.valueField = valueField;
		
		this.hBaseMapToTuplesSerilizerCF = hBaseMapToTuplesSerilizerCF;
		this.hBaseMapToTuplesSerilizerC = hBaseMapToTuplesSerilizerC;
		this.hBaseMapToTuplesSerilizerV = hBaseMapToTuplesSerilizerV;
		
		if (1 != rowField.size() || 1 !=cfName.size() || 1 != columnField.size() || 1 != valueField.size())	{
			throw new IllegalArgumentException("Fields should be 1 element size");
		}
		
	}

	public HBaseTuplesToMap(Fields declaredFields, Fields cfName, Fields rowField, Fields columnField,
	        Fields valueField, Class<CF> cfClass, Class<C> cClass, Class<V> vClass) throws SecurityException, NoSuchMethodException	{
		this(declaredFields, cfName, rowField, columnField, valueField, 
				new HBaseMapToTuplesDefaultSerilizer<CF>(cfClass),
				new HBaseMapToTuplesDefaultSerilizer<C>(cClass),
				new HBaseMapToTuplesDefaultSerilizer<V>(vClass));
		
	}

	@Override
	public void start(FlowProcess flowProcess,
	        AggregatorCall<HBaseTuplesToMap.AggregatorWriterTuplesListContext> aggregatorCall)
	{
		AggregatorWriterTuplesListContext aggregatorContext = new AggregatorWriterTuplesListContext();
		aggregatorCall.setContext(aggregatorContext);

	}



	@SuppressWarnings("unchecked")
    @Override
	public void aggregate(FlowProcess flowProcess,
	        AggregatorCall<HBaseTuplesToMap.AggregatorWriterTuplesListContext> aggregatorCall)
	{
		String rowFieldStr = aggregatorCall.getArguments().getString(rowField);
		byte[] cfFieldBytes = hBaseMapToTuplesSerilizerCF.toBytes((CF)aggregatorCall.getArguments().getObject(cfName));
		byte[] columnFieldBytes = hBaseMapToTuplesSerilizerC.toBytes((C)aggregatorCall.getArguments().getObject(columnField));
		byte[] valueFieldBytes = hBaseMapToTuplesSerilizerV.toBytes((V)aggregatorCall.getArguments().getObject(valueField));

		aggregatorCall.getContext().key = rowFieldStr;
		
		aggregatorCall.getContext().addElements(cfFieldBytes, columnFieldBytes, valueFieldBytes);

	}



	@Override
	public void complete(FlowProcess flowProcess,
	        AggregatorCall<HBaseTuplesToMap.AggregatorWriterTuplesListContext> aggregatorCall)
	{

		aggregatorCall.getOutputCollector().add(new Tuple(aggregatorCall.getContext().key, aggregatorCall.getContext().keyValueMap));

	}
}
