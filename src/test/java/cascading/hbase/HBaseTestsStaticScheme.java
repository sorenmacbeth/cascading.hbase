package cascading.hbase;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.tuple.TupleEntryIterator;

public abstract class HBaseTestsStaticScheme extends HBaseTests {

	// TODO: enable assertTrue funs again

	protected void verifySink(Flow flow, int expects) throws IOException {
		int count = 0;

		TupleEntryIterator iterator = flow.openSink();

		while (iterator.hasNext()) {
			count++;
			System.out.println("iterator.next() = " + iterator.next());
		}

		iterator.close();

//		assertTrue("wrong number of values in " + flow.getSink().toString(),
//				expects == count);
	}

	protected void verify(String tableName, String family, String charCol,
			int expected) throws IOException {
		byte[] familyBytes = Bytes.toBytes(family);
		byte[] qulifierBytes = Bytes.toBytes(charCol);

		HTable table = new HTable(configuration, tableName);
		ResultScanner scanner = table.getScanner(familyBytes, qulifierBytes);

		int count = 0;
		for (Result rowResult : scanner) {
			count++;
			System.out.println("rowResult = "
					+ rowResult.getValue(familyBytes, qulifierBytes));
		}

		scanner.close();

//		assertTrue("wrong number of rows", expected == count);
	}
}
