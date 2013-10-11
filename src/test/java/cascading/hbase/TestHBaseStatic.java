package cascading.hbase;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.operation.Identity;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;

@SuppressWarnings("rawtypes")
public class TestHBaseStatic extends HBaseTestsStaticScheme {

	@Test
	public void testHBaseMultiFamily() throws IOException {

		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, TestHBaseStatic.class);

		deleteTable(configuration, "multitable");

		String inputFile = "src/test/resources/data/small.txt";
		// create flow to read from local file and insert into HBase
		Tap source = new Lfs(new TextLine(), inputFile);

		Pipe parsePipe = new Pipe("parse");
		parsePipe = new Each(parsePipe, new Fields("line"), new RegexSplitter(
				new Fields("num", "lower", "upper"), " "));

		Fields keyFields = new Fields("num");
		String[] familyNames = { "left", "right" };
		Fields[] valueFields = new Fields[] { new Fields("lower"),
				new Fields("upper") };
		Tap hBaseTap = new HBaseTap("multitable", new HBaseScheme(keyFields,
				familyNames, valueFields), SinkMode.REPLACE);

		FlowConnector flowConnector = createHadoopFlowConnector();
		Flow parseFlow = flowConnector.connect(source, hBaseTap, parsePipe);

		parseFlow.complete();

		verifySink(parseFlow, 5);

		// create flow to read from hbase and save to local file
		Tap sink = new Lfs(new TextLine(), "build/test/multifamily",
				SinkMode.REPLACE);

		Pipe copyPipe = new Each("read", new Identity());

		Flow copyFlow = flowConnector.connect(hBaseTap, sink, copyPipe);

		copyFlow.complete();

		verifySink(copyFlow, 5);

	}

	@Test
	public void testHBaseMultiFamilyCascade() throws IOException {

		deleteTable(configuration, "multitable");

		String inputFile = "src/test/resources/data/small.txt";
		// create flow to read from local file and insert into HBase
		Tap source = new Lfs(new TextLine(), inputFile);

		Pipe parsePipe = new Each("insert", new Fields("line"),
				new RegexSplitter(new Fields("ignore", "lower", "upper"), " "));
		parsePipe = new Each(parsePipe,
				new ExpressionFunction(new Fields("num"),
						"(int) (Math.random() * Integer.MAX_VALUE)"),
				Fields.ALL);

		Fields keyFields = new Fields("num");
		String[] familyNames = { "left", "right" };
		Fields[] valueFields = new Fields[] { new Fields("lower"),
				new Fields("upper") };
		Tap hBaseTap = new HBaseTap("multitable", new HBaseScheme(keyFields,
				familyNames, valueFields));

    FlowConnector flowConnector = createHadoopFlowConnector();
		Flow parseFlow = flowConnector.connect(source, hBaseTap, parsePipe);

		// create flow to read from hbase and save to local file
		Tap sink = new Lfs(new TextLine(), "build/test/multifamilycascade",
				SinkMode.REPLACE);

		Pipe copyPipe = new Each("read", new Identity());

		Flow copyFlow = flowConnector.connect(hBaseTap, sink, copyPipe);

		// reversed order intentionally
		Cascade cascade = new CascadeConnector().connect(copyFlow, parseFlow);
		cascade.complete();

		verify("multitable", "left", "lower", 13);

		verifySink(parseFlow, 13);
		verifySink(copyFlow, 13);

		parseFlow = flowConnector.connect(source, hBaseTap, parsePipe);
		copyFlow = flowConnector.connect(hBaseTap, sink, copyPipe);

		// reversed order intentionally
		cascade = new CascadeConnector().connect(copyFlow, parseFlow);
		cascade.complete();

		verify("multitable", "left", "lower", 26);

		verifySink(parseFlow, 26);
		verifySink(copyFlow, 26);

	}
}
