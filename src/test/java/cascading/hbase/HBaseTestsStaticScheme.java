package cascading.hbase;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import cascading.flow.Flow;
import cascading.tuple.TupleEntryIterator;

public abstract class HBaseTestsStaticScheme extends HBaseTests {


	protected void verifySink(Flow flow, int expects) throws IOException {
		int count = 0;

		TupleEntryIterator iterator = flow.openSink();

		while (iterator.hasNext()) {
			count++;
			System.out.println("iterator.next() = " + iterator.next());
		}

		iterator.close();

		assertEquals("wrong number of values in " + flow.getSink().toString(),
				expects, count);
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
		table.close();

    assertEquals("wrong number of rows", expected, count);
	}
}
