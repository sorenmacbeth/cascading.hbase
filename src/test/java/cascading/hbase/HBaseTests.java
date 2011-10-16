package cascading.hbase;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

abstract public class HBaseTests {
    /** The hbase cluster. */
    protected static LocalHBaseCluster hbaseCluster;

    /** The zoo keeper cluster. */
    protected static MiniZooKeeperCluster zooKeeperCluster;

    /** The configuration. */
    protected static Configuration configuration;
    
    @BeforeClass
    public static void before() throws IOException, InterruptedException {
	configuration = HBaseConfiguration.create();

	zooKeeperCluster = new MiniZooKeeperCluster(configuration);
	zooKeeperCluster.setClientPort(21818);

	// int clientPort =
	zooKeeperCluster.startup(new File("target/zookeepr"));

	// start the mini cluster
	hbaseCluster = new LocalHBaseCluster(configuration, 1);

	hbaseCluster.startup();

    }
    
    protected static void deleteTable(Configuration configuration,
	    String tableName) throws IOException {
	HBaseAdmin hbase = new HBaseAdmin(configuration);
	if (hbase.tableExists(Bytes.toBytes(tableName))) {
	    hbase.disableTable(Bytes.toBytes(tableName));
	    hbase.deleteTable(Bytes.toBytes(tableName));
	}
    }

     @AfterClass
    public static void afterClass() throws IOException {

	hbaseCluster.shutdown();
	hbaseCluster.waitOnMaster(0);
	zooKeeperCluster.shutdown();
    }
     
     
    
    
}
