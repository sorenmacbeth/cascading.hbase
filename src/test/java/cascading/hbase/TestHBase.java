package cascading.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHBase {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor[] tables = admin.listTables();
		for (HTableDescriptor td : tables) {
			System.out.println(td.getNameAsString());
		}
	}

}
