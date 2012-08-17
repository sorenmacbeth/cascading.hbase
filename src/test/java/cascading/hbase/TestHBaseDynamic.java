package cascading.hbase;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.NavigableMap;
import java.util.Properties;

import junitx.framework.FileAssert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mortbay.log.Log;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.hbase.helper.HBaseMapToTuples;
import cascading.hbase.helper.HBaseTuplesToMap;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Insert;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

@RunWith(org.junit.runners.JUnit4.class)
public class TestHBaseDynamic extends HBaseTests {

	private static final String TEST_TABLE = "testTable";

	private static final String TEST_CF = "testCF";

	private static final String inputDataFile = "src/test/resources/data/small.txt";

	private static void fillTable(Configuration configuration)
			throws IOException {
		deleteTable(configuration, TEST_TABLE);

		HTable table = HBaseUtils.openTable(TEST_TABLE, TEST_CF);

		// HTable table =
		// hBaseTestingUtility.createTable(Bytes.toBytes(TEST_TABLE),
		// Bytes.toBytes(TEST_CF));

		Put put1 = new Put(Bytes.toBytes("row_1"));
		put1.add(Bytes.toBytes(TEST_CF), Bytes.toBytes("c_1"), Bytes.toBytes(1));

		Put put2 = new Put(Bytes.toBytes("row_2"));
		put2.add(Bytes.toBytes(TEST_CF), Bytes.toBytes("c_2"), Bytes.toBytes(2));

		table.put(put1);
		table.put(put2);
		Log.info("created table: {} with cf: {}", TEST_TABLE, TEST_CF);
	}

	@SuppressWarnings("serial")
	static public class StringAppender extends BaseOperation<Void> implements
			Function<Void> {

		public StringAppender(Fields declaredFields) {
			super(declaredFields);

		}

		@Override
		public void operate(FlowProcess flowProcess,
				FunctionCall<Void> functionCall) {
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < functionCall.getArgumentFields().size(); i++) {
				stringBuffer.append(functionCall.getArgumentFields().get(i)
						.toString()
						+ ":"
						+ functionCall.getArguments().getString(
								functionCall.getArgumentFields().get(i)) + " ");
			}

			functionCall.getOutputCollector().add(
					new Tuple(stringBuffer.toString()));

		}

	}

	@Before
	public void beforeInstance() throws IOException {
		fillTable(configuration);
	}

	@Test
	public void testRead() throws SecurityException, NoSuchMethodException {

		Tap source = new HBaseTap(TEST_TABLE, new HBaseDynamicScheme(
				new Fields("row"), new Fields("value"), TEST_CF));
		Tap sink = new Lfs(new TextLine(new Fields("line")),
				"build/test/hbasedynamicread", SinkMode.REPLACE);

		Pipe pipe = new Pipe("hbasedynamicschemepipe");

		pipe = new Each(pipe, new Fields("row", "value"), new HBaseMapToTuples(
				new Fields("row", "cf", "column", "value"), new Fields("row",
						"value")));
		pipe = new Each(pipe, new StringAppender(new Fields("line")));

		Flow flow = flowConnector.connect(source, sink, pipe);

		flow.complete();

		FileAssert.assertBinaryEquals(new File(
				"src/test/resources/data/fileDynamicExpected"), new File(
				"build/test/hbasedynamicread/part-00000"));

	}

	static private boolean isSameValue(String row, String cf, String column,
			String value, int size, TupleEntry entry) {
		if (!entry.getString(0).equals(row)) {
			return false;
		}

		@SuppressWarnings("unchecked")
		NavigableMap<byte[], NavigableMap<byte[], byte[]>> mapmap = (NavigableMap<byte[], NavigableMap<byte[], byte[]>>) entry
				.getObject(1);
		if (!mapmap.containsKey(Bytes.toBytes(cf))) {
			return false;
		}

		NavigableMap<byte[], byte[]> map = mapmap.get(Bytes.toBytes(cf));

		if (map.size() != size) {
			return false;
		}

		if (!map.containsKey(Bytes.toBytes(column))) {
			return false;
		}

		byte[] mapValue = map.get(Bytes.toBytes(column));

		return Arrays.equals(mapValue, Bytes.toBytes(value));
	}

	@Test
	public void writeTest() throws IOException, SecurityException,
			NoSuchMethodException {
		Tap source = new Lfs(new TextLine(new Fields("line")), inputDataFile);

		Pipe parsePipe = new Each("insert", new RegexSplitter(new Fields("num",
				"lower", "upper"), " "));
		parsePipe = new Each(parsePipe, new Insert(new Fields("cf"), "cf"),
				Fields.ALL);
		parsePipe = new GroupBy(parsePipe, new Fields("num"));
		// parsePipe = new Every( parsePipe, new AggregatorWriterTuplesList( new
		// Fields("key", "value"), "cf", new
		// Fields("num"), new Fields("lower"), new Fields("upper") ) ) ;
		parsePipe = new Every(parsePipe, new HBaseTuplesToMap(new Fields("key",
				"value"), new Fields("cf"), new Fields("num"), new Fields(
				"lower"), new Fields("upper")));

		Tap hBaseTap = new HBaseTap("multitable", new HBaseDynamicScheme(
				new Fields("key"), new Fields("value"), "cf"), SinkMode.REPLACE);

		Flow parseFlow = flowConnector.connect(
				source, hBaseTap, parsePipe);

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
