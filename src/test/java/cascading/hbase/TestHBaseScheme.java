package cascading.hbase;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;

import junitx.framework.FileAssert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.hbase.helper.HBaseMapToTuples;
import cascading.hbase.helper.HBaseTuplesToMap;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.expression.ExpressionFunction;
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
public class TestHBaseScheme
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



	private static void deleteTable(Configuration configuration, String tableName) throws IOException
	{
		HBaseAdmin hbase = new HBaseAdmin(configuration);
		if (hbase.tableExists(Bytes.toBytes(tableName)))
		{
			hbase.disableTable(Bytes.toBytes(tableName));
			hbase.deleteTable(Bytes.toBytes(tableName));
		}
	}



	private static void fillTable(Configuration configuration) throws IOException
	{
		deleteTable(configuration, TEST_TABLE);

		HTable table = HBaseUtils.openTable(TEST_TABLE, TEST_CF);

		// HTable table = hBaseTestingUtility.createTable(Bytes.toBytes(TEST_TABLE), Bytes.toBytes(TEST_CF));

		Put put1 = new Put(Bytes.toBytes("row_1"));
		put1.add(Bytes.toBytes(TEST_CF), Bytes.toBytes("c_1"), Bytes.toBytes(1));

		Put put2 = new Put(Bytes.toBytes("row_2"));
		put2.add(Bytes.toBytes(TEST_CF), Bytes.toBytes("c_2"), Bytes.toBytes(2));

		table.put(put1);
		table.put(put2);
	}



	@AfterClass
	public static void afterClass() throws IOException
	{

		hbaseCluster.shutdown();
		hbaseCluster.waitOnMaster(0);
		zooKeeperCluster.shutdown();
	}

	@SuppressWarnings("serial")
	static public class StringAppender extends BaseOperation<Void> implements Function<Void>
	{

		public StringAppender(Fields declaredFields)
		{
			super(declaredFields);

		}



		@Override
		public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall)
		{
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < functionCall.getArgumentFields().size(); i++ )
			{
				stringBuffer.append(functionCall.getArgumentFields().get(i).toString() + ":" +
				        functionCall.getArguments().getString(functionCall.getArgumentFields().get(i)) + " ");
			}

			functionCall.getOutputCollector().add(new Tuple(stringBuffer.toString()));

		}

	}



	@Before
	public void beforeInstance() throws IOException
	{
		Configuration config = HBaseConfiguration.create();

		fillTable(config);
	}



	@Test
	public void testRead() throws SecurityException, NoSuchMethodException
	{

		Tap source = new HBaseTap(TEST_TABLE, new HBaseDynamicScheme(new Fields("row"), new Fields("value"), TEST_CF));
		Tap sink = new Lfs(new TextLine(new Fields("line")), "build/test/hbasedynamicread", SinkMode.REPLACE);

		Pipe pipe = new Pipe("hbasedynamicschemepipe");

		pipe =
		        new Each(pipe, new Fields("row", "value"), new HBaseMapToTuples(new Fields("row", "cf", "column",
		                "value"), new Fields("row", "value")));
		pipe = new Each(pipe, new StringAppender(new Fields("line")));

		Flow flow = new FlowConnector(new Properties()).connect(source, sink, pipe);

		flow.complete();

		FileAssert.assertBinaryEquals(new File("src/test/resources/data/fileDynamicExpected"), new File(
		        "build/test/hbasedynamicread/part-00000"));

	}



	protected void verifySink(Flow flow, int expects) throws IOException
	{
		int count = 0;

		TupleEntryIterator iterator = flow.openSink();

		while (iterator.hasNext())
		{
			count++ ;
			System.out.println("iterator.next() = " + iterator.next());
		}

		iterator.close();

		assertTrue("wrong number of values in " + flow.getSink().toString(), expects == count);
	}



	protected void verify(String tableName, String family, String charCol, int expected) throws IOException
	{
		byte[] familyBytes = Bytes.toBytes(family);
		byte[] qulifierBytes = Bytes.toBytes(charCol);

		HTable table = new HTable(configuration, tableName);
		ResultScanner scanner = table.getScanner(familyBytes, qulifierBytes);

		int count = 0;
		for (Result rowResult : scanner)
		{
			count++ ;
			System.out.println("rowResult = " + rowResult.getValue(familyBytes, qulifierBytes));
		}

		scanner.close();

		assertTrue("wrong number of rows", expected == count);
	}



	@Test
	public void testHBaseMultiFamily() throws IOException
	{
		deleteTable(configuration, "multitable");

		Map<Object, Object> properties = new HashMap<Object, Object>();
		String inputFile = "src/test/resources/data/small.txt";
		// create flow to read from local file and insert into HBase
		Tap source = new Lfs(new TextLine(), inputFile);

		Pipe parsePipe =
		        new Each("insert", new Fields("line"), new RegexSplitter(new Fields("num", "lower", "upper"), " "));

		Fields keyFields = new Fields("num");
		String[] familyNames = { "left", "right" };
		Fields[] valueFields = new Fields[] { new Fields("lower"), new Fields("upper") };
		Tap hBaseTap =
		        new HBaseTap("multitable", new HBaseScheme(keyFields, familyNames, valueFields), SinkMode.REPLACE);

		Flow parseFlow = new FlowConnector(properties).connect(source, hBaseTap, parsePipe);

		parseFlow.complete();

		verifySink(parseFlow, 5);

		// create flow to read from hbase and save to local file
		Tap sink = new Lfs(new TextLine(), "build/test/multifamily", SinkMode.REPLACE);

		Pipe copyPipe = new Each("read", new Identity());

		Flow copyFlow = new FlowConnector(properties).connect(hBaseTap, sink, copyPipe);

		copyFlow.complete();

		verifySink(copyFlow, 5);

	}



	@Test
	public void testHBaseMultiFamilyCascade() throws IOException
	{
		deleteTable(configuration, "multitable");

		Map<Object, Object> properties = new HashMap<Object, Object>();

		String inputFile = "src/test/resources/data/small.txt";
		// create flow to read from local file and insert into HBase
		Tap source = new Lfs(new TextLine(), inputFile);

		Pipe parsePipe =
		        new Each("insert", new Fields("line"), new RegexSplitter(new Fields("ignore", "lower", "upper"), " "));
		parsePipe =
		        new Each(parsePipe, new ExpressionFunction(new Fields("num"),
		                "(int) (Math.random() * Integer.MAX_VALUE)"), Fields.ALL);
		// parsePipe = new Each( parsePipe, new Debug() );

		Fields keyFields = new Fields("num");
		String[] familyNames = { "left", "right" };
		Fields[] valueFields = new Fields[] { new Fields("lower"), new Fields("upper") };
		Tap hBaseTap = new HBaseTap("multitable", new HBaseScheme(keyFields, familyNames, valueFields));

		Flow parseFlow = new FlowConnector(properties).connect(source, hBaseTap, parsePipe);

		// create flow to read from hbase and save to local file
		Tap sink = new Lfs(new TextLine(), "build/test/multifamilycascade", SinkMode.REPLACE);

		Pipe copyPipe = new Each("read", new Identity());

		Flow copyFlow = new FlowConnector(properties).connect(hBaseTap, sink, copyPipe);

		Cascade cascade = new CascadeConnector().connect(copyFlow, parseFlow); // reversed order intentionally

		parseFlow.deleteSinks(); // force the hbase tap to be deleted

		cascade.complete();

		verify("multitable", "left", "lower", 13);

		verifySink(parseFlow, 13);
		verifySink(copyFlow, 13);

		parseFlow = new FlowConnector(properties).connect(source, hBaseTap, parsePipe);
		copyFlow = new FlowConnector(properties).connect(hBaseTap, sink, copyPipe);

		cascade = new CascadeConnector().connect(copyFlow, parseFlow); // reversed order intentionally

		cascade.complete();

		verify("multitable", "left", "lower", 26);

		verifySink(parseFlow, 26);
		verifySink(copyFlow, 26);

	}



	static private boolean isSameValue(String row, String cf, String column, String value, int size, TupleEntry entry)
	{
		if (!entry.getString(0).equals(row))
		{
			return false;
		}

		NavigableMap<byte[], NavigableMap<byte[], byte[]>> mapmap =
		        (NavigableMap<byte[], NavigableMap<byte[], byte[]>>) entry.getObject(1);
		if (!mapmap.containsKey(Bytes.toBytes(cf)))
		{
			return false;
		}

		NavigableMap<byte[], byte[]> map = mapmap.get(Bytes.toBytes(cf));

		if (map.size() != size)
		{
			return false;
		}

		if (!map.containsKey(Bytes.toBytes(column)))
		{
			return false;
		}

		byte[] mapValue = map.get(Bytes.toBytes(column));

		return Arrays.equals(mapValue, Bytes.toBytes(value));
	}



	@Test
	public void writeTest() throws IOException, SecurityException, NoSuchMethodException
	{
		Tap source = new Lfs(new TextLine(new Fields("line")), inputDataFile);

		Pipe parsePipe = new Each("insert", new RegexSplitter(new Fields("num", "lower", "upper"), " "));
		parsePipe = new Each(parsePipe, new Insert(new Fields("cf"), "cf"), Fields.ALL);
		parsePipe = new GroupBy(parsePipe, new Fields("num"));
		// parsePipe = new Every( parsePipe, new AggregatorWriterTuplesList( new Fields("key", "value"), "cf", new
		// Fields("num"), new Fields("lower"), new Fields("upper") ) ) ;
		parsePipe =
		        new Every(parsePipe, new HBaseTuplesToMap(new Fields("key", "value"), new Fields("cf"), new Fields(
		                "num"), new Fields("lower"), new Fields("upper")));

		Tap hBaseTap =
		        new HBaseTap("multitable", new HBaseDynamicScheme(new Fields("key"), new Fields("value"), "cf"),
		                SinkMode.REPLACE);

		Flow parseFlow = new FlowConnector(new Properties()).connect(source, hBaseTap, parsePipe);

		parseFlow.complete();

		TupleEntryIterator iterator = parseFlow.openSink();
		assertTrue(isSameValue("1", "cf", "a", "A", 3, iterator.next()));
		assertTrue(isSameValue("2", "cf", "b", "B", 3, iterator.next()));
		assertTrue(isSameValue("3", "cf", "c", "C", 1, iterator.next()));
		assertTrue(isSameValue("4", "cf", "d", "D", 3, iterator.next()));
		assertTrue(isSameValue("5", "cf", "e", "E", 3, iterator.next()));

		iterator.close();

	}
}
