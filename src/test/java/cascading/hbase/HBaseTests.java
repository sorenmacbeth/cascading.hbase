package cascading.hbase;

import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;

import cascading.flow.hadoop.HadoopFlowConnector;

abstract public class HBaseTests {

	// TODO: enable testing clusters and go back to port 21818

//	/** The hbase cluster. */
//	protected static LocalHBaseCluster hbaseCluster;
//
//	/** The zoo keeper cluster. */
//	protected static MiniZooKeeperCluster zooKeeperCluster;

	/** The configuration. */
	protected static Configuration configuration;

	private static Properties properties = new Properties();
	protected static HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

	@BeforeClass
	public static void before() {
		configuration = HBaseConfiguration.create();
//		configuration.set("hbase.zookeeper.property.clientPort", "21818");
	}

//      throws IOException, InterruptedException
//
//		zooKeeperCluster = new MiniZooKeeperCluster(configuration);
//		zooKeeperCluster.setDefaultClientPort(21818);
//
//		zooKeeperCluster.startup(new File("target/zookeepr"));
//
//		// start the mini cluster
//		hbaseCluster = new LocalHBaseCluster(configuration, 1);
//
//		hbaseCluster.startup();
//
//	}

	protected static void deleteTable(Configuration configuration,
			String tableName) throws IOException {
		HBaseAdmin hbase = new HBaseAdmin(configuration);
		if (hbase.tableExists(Bytes.toBytes(tableName))) {
			hbase.disableTable(Bytes.toBytes(tableName));
			hbase.deleteTable(Bytes.toBytes(tableName));
		}
	}

//	@AfterClass
//	public static void afterClass() throws IOException {
//
//		hbaseCluster.shutdown();
//		hbaseCluster.waitOnMaster(0);
//		zooKeeperCluster.shutdown();
//	}

}
