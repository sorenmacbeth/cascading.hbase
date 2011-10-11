package cascading.hbase;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.TreeMap;

import javax.swing.text.Keymap;

import junitx.framework.FileAssert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Lfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

@RunWith(org.junit.runners.JUnit4.class)
public class HBaseDynamicSchemeTests 
{
	private static final String TEST_TABLE = "testTable";
	private static final String TEST_CF = "testCF";
	
	/** The hbase cluster. */
	private static LocalHBaseCluster hbaseCluster;

	/** The zoo keeper cluster. */
	private static MiniZooKeeperCluster zooKeeperCluster;
	
	/** The configuration. */
	private static Configuration configuration;
	
	private static final String inputDataFile = "src/test/resources/data/small.txt";
	
	private HTable table;


	
	@BeforeClass
	public static void before() throws IOException, InterruptedException
	{
		configuration = HBaseConfiguration.create();
		
		zooKeeperCluster = new MiniZooKeeperCluster(configuration);
		zooKeeperCluster.setClientPort(21818);
		
		// int clientPort =
		zooKeeperCluster.startup(new File("target/zookeepr"));

		// start the mini cluster
		hbaseCluster = new LocalHBaseCluster(configuration, 1);

		hbaseCluster.startup();
		
		
		
		
	}
	
	private static void fillTable(Configuration configuration) throws IOException	{ 
		HBaseAdmin hbase = new HBaseAdmin(configuration);
		if (hbase.tableExists(Bytes.toBytes(TEST_TABLE)))	{
			hbase.disableTable(Bytes.toBytes(TEST_TABLE));
			hbase.deleteTable(Bytes.toBytes(TEST_TABLE));
		}

		HTable table = HBaseUtils.openTable(TEST_TABLE, TEST_CF);
		
		//HTable table = hBaseTestingUtility.createTable(Bytes.toBytes(TEST_TABLE), Bytes.toBytes(TEST_CF));
		
		Put put1 = new Put(Bytes.toBytes("row_1"));
		put1.add(Bytes.toBytes(TEST_CF), Bytes.toBytes("c_1"), Bytes.toBytes(1));
		
		Put put2 = new Put(Bytes.toBytes("row_2"));
		put2.add(Bytes.toBytes(TEST_CF), Bytes.toBytes("c_2"), Bytes.toBytes(2));
		
		table.put(put1);
		table.put(put2);
	}

	
	@AfterClass
	  public static void afterClass() throws IOException {
		
		hbaseCluster.shutdown();
		hbaseCluster.waitOnMaster(0);
		zooKeeperCluster.shutdown();
	  }
	
	@SuppressWarnings("serial")
    static public class FunctionTuplesList 
	extends BaseOperation<Void > implements Function<Void>	{
		
		Fields inputFields;
		
		public FunctionTuplesList(Fields declaredFields, Fields inputFields)	{
			super(2, declaredFields);
			
			this.inputFields = inputFields;
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
					functionCall.getOutputCollector().add(new Tuple(row, Bytes.toString(keyValue.getKey()),
							Bytes.toString(value.getKey()), Bytes.toString(value.getValue())));
				}
			}
        }
		
	}
	
	@SuppressWarnings("serial")
    static public class StringAppender 
	extends BaseOperation<Void > implements Function<Void>	{
			
		public StringAppender(Fields declaredFields)	{
			super(declaredFields);
			
		}

		@Override
        public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall)
        {
			StringBuffer stringBuffer = new StringBuffer();
	        for (int i = 0; i<functionCall.getArgumentFields().size(); i++)	{
	        	stringBuffer.append(functionCall.getArgumentFields().get(i).toString() + ":" + functionCall.getArguments().getString(functionCall.getArgumentFields().get(i)) +" ");
	        }
	        
	        functionCall.getOutputCollector().add(new Tuple(stringBuffer.toString()));
			
        }
		
	}
	
	@Before
	public void beforeInstance() throws IOException	{
		Configuration config = HBaseConfiguration.create();
		
		fillTable(config);
	}
	
	@Test
	public void testRead() {
		
		Tap source = new HBaseTap(TEST_TABLE, new HBaseDynamicScheme(new Fields("row"), new Fields("value"), TEST_CF));
		Tap sink = new Lfs(new TextLine(new Fields("line")), "build/test/hbasedynamicread", SinkMode.REPLACE);
		
		Pipe pipe = new Pipe("hbasedynamicschemepipe");
		
		pipe = new Each(pipe, new Fields("row", "value"), new FunctionTuplesList(new Fields("row", "cf", "column", "value"), new Fields ("row", "value")));
		pipe = new Each(pipe, new StringAppender(new Fields("line")));
		
		Flow flow = new FlowConnector(new Properties()).connect(source, sink, pipe);
		
		flow.complete();
		
		FileAssert.assertBinaryEquals(new File("src/test/resources/data/fileDynamicExpected"), new File("build/test/hbasedynamicread/part-00000"));
		
	}
	
	@SuppressWarnings("serial")
    static public class AggregatorWriterTuplesList 
	extends BaseOperation<AggregatorWriterTuplesList.AggregatorWriterTuplesListContext> implements 
		Aggregator<AggregatorWriterTuplesList.AggregatorWriterTuplesListContext>	{
		
		private static class AggregatorWriterTuplesListContext	{
			public NavigableMap<byte[], byte[]> keyValueMap = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
			public String CF;
			public String key;
			
			
		};
		
		private Fields rowField;
		private Fields columnField;
		private Fields valueField;
		private String cfName;
		
		public AggregatorWriterTuplesList(Fields declaredFields, String cfName, Fields rowField, Fields columnField, Fields valueField)	{
			super(declaredFields);
			
			this.rowField = rowField;
			this.columnField = columnField;
			this.valueField = valueField;
			this.cfName = cfName;
		}

		@Override
        public void start(FlowProcess flowProcess,
                AggregatorCall<AggregatorWriterTuplesList.AggregatorWriterTuplesListContext> aggregatorCall)
        {
			AggregatorWriterTuplesListContext aggregatorContext = new AggregatorWriterTuplesListContext();
			aggregatorContext.CF = cfName;
			aggregatorCall.setContext(aggregatorContext);
			
        }

		@Override
        public void aggregate(FlowProcess flowProcess,
                AggregatorCall<AggregatorWriterTuplesList.AggregatorWriterTuplesListContext> aggregatorCall)
        {
	        String rowFieldStr = aggregatorCall.getArguments().getString(rowField);
	        byte[] rowFieldBytes = Bytes.toBytes(rowFieldStr);
	        String columnFieldStr = aggregatorCall.getArguments().getString(columnField);
	        byte[] columnFieldBytes = Bytes.toBytes(columnFieldStr);
	        String valueFieldStr = aggregatorCall.getArguments().getString(valueField);
	        byte[] valueFieldBytes = Bytes.toBytes(valueFieldStr);
	        
	        aggregatorCall.getContext().key = rowFieldStr;
	        aggregatorCall.getContext().keyValueMap.put(columnFieldBytes, valueFieldBytes);
	        
	        
        }

		@Override
        public void complete(FlowProcess flowProcess,
                AggregatorCall<AggregatorWriterTuplesList.AggregatorWriterTuplesListContext> aggregatorCall)
        {
			
			NavigableMap<byte[], NavigableMap<byte[], byte[]>> cfMap = new TreeMap<byte[], NavigableMap<byte[], byte[]>>();
			cfMap.put(Bytes.toBytes(aggregatorCall.getContext().CF), aggregatorCall.getContext().keyValueMap);
			
	        aggregatorCall.getOutputCollector().add(new Tuple(aggregatorCall.getContext().key, 
	        		cfMap));
	        
        }
		
	}
	
	static private boolean isSameValue(String row, String cf, String column, String value, int size, TupleEntry entry)	{
		if (!entry.getString(0).equals(row))	{
			return false;
		}
		
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> mapmap = (NavigableMap<byte[], NavigableMap<byte[], byte[]>>) entry.getObject(1); 
		if (!mapmap.containsKey(Bytes.toBytes(cf)))	{
			return false;
		}
		
		NavigableMap<byte[], byte[]> map = mapmap.get(Bytes.toBytes(cf));
		
		if (map.size() != size)	{
			return false;
		}
		
		if (!map.containsKey(Bytes.toBytes(column)))	{
			return false;
		}
		 
		byte[] mapValue = map.get(Bytes.toBytes(column));
		
		return Arrays.equals(mapValue, Bytes.toBytes(value));
	}
	
	@Test
	public void writeTest() throws IOException	{
		Tap source = new Lfs( new TextLine(new Fields("line")), inputDataFile );

	    Pipe parsePipe = new Each( "insert", new RegexSplitter( new Fields( "num", "lower", "upper" ), " " ) );
	    parsePipe = new GroupBy(parsePipe, new Fields("num"));
	    parsePipe = new Every( parsePipe, new AggregatorWriterTuplesList( new Fields("key", "value"), "cf", new Fields("num"), new Fields("lower"), new Fields("upper") ) ) ;

	    Tap hBaseTap = new HBaseTap( "multitable", new HBaseDynamicScheme( new Fields("key"), new Fields("value"), "cf" ), SinkMode.REPLACE );
	    
	    Flow parseFlow = new FlowConnector( new Properties() ).connect( source, hBaseTap, parsePipe );

	    parseFlow.complete();
	    
	    int count = 0;

		TupleEntryIterator iterator = parseFlow.openSink();
		assertTrue(isSameValue("1","cf","a","A",3,iterator.next()));
		assertTrue(isSameValue("2","cf","b","B",3,iterator.next()));
		assertTrue(isSameValue("3","cf","c","C",1,iterator.next()));
		assertTrue(isSameValue("4","cf","d","D",3,iterator.next()));
		assertTrue(isSameValue("5","cf","e","E",3,iterator.next()));

		iterator.close();
	    
	}
}
