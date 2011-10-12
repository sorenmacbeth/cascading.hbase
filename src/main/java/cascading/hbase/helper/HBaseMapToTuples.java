package cascading.hbase.helper;

import java.util.Map.Entry;
import java.util.NavigableMap;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class HBaseMapToTuples<CF, C, V>
extends BaseOperation<Void > implements Function<Void>	{
	
	Fields inputFields; 
	private HBaseMapToTuplesSerilizer<CF> hBaseMapToTuplesSerilizerCF;
	private HBaseMapToTuplesSerilizer<C> hBaseMapToTuplesSerilizerC;
	private HBaseMapToTuplesSerilizer<V> hBaseMapToTuplesSerilizerV;
	
	
	public HBaseMapToTuples(Fields declaredFields, Fields inputFields, 
			HBaseMapToTuplesSerilizer<CF> hBaseMapToTuplesSerilizerCF,
			HBaseMapToTuplesSerilizer<C> hBaseMapToTuplesSerilizerC,
			HBaseMapToTuplesSerilizer<V> hBaseMapToTuplesSerilizerV)	{
		super(2, declaredFields);
		
		this.inputFields = inputFields;
		this.hBaseMapToTuplesSerilizerCF =  hBaseMapToTuplesSerilizerCF;
		this.hBaseMapToTuplesSerilizerC =  hBaseMapToTuplesSerilizerC;
		this.hBaseMapToTuplesSerilizerV =  hBaseMapToTuplesSerilizerV;
		
	}
	
	public HBaseMapToTuples(Fields declaredFields, Fields inputFields, Class<CF> CFClass, 
			Class<C> CClass, Class<V> VClass) throws SecurityException, NoSuchMethodException	{
		this(declaredFields, inputFields, new HBaseMapToTuplesDefaultSerilizer<CF>(CFClass), 
				new HBaseMapToTuplesDefaultSerilizer<C>(CClass),
				new HBaseMapToTuplesDefaultSerilizer<V>(VClass));
	}

	@Override
    public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall)
    {
        String row = functionCall.getArguments().getString(inputFields.get(0));
        @SuppressWarnings("unchecked")
        NavigableMap<byte[], NavigableMap<byte[], byte[]>> keyValueMaps = 
        		(NavigableMap<byte[], NavigableMap<byte[], byte[]>>) 
     functionCall.getArguments().getObject(inputFields.get(1));
        
        for (Entry<byte[], NavigableMap<byte[], byte[]>>  keyValue : keyValueMaps.entrySet())	{
			for (Entry<byte[], byte[]>  value: keyValue.getValue().entrySet())	{
				functionCall.getOutputCollector().add(new Tuple(row, hBaseMapToTuplesSerilizerCF.toT(keyValue.getKey()),
						hBaseMapToTuplesSerilizerC.toT(value.getKey()), hBaseMapToTuplesSerilizerV.toT(value.getValue())));
			}
		}
    }
	
}
