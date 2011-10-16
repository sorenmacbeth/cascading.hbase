package cascading.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Class HBaseUtils.
 */
public class HBaseUtils
{

	private static Logger log = LoggerFactory.getLogger(HBaseUtils.class);

	/**
	 * creating a table with the given tabel name and the given coulmnFamilyArray.
	 * if there is already a table with this name:
	 * 		1. if the table disabled => delete the table and create a new one
	 * 		2. if the table enabled =? check if the all the columns are there. if not => add the ones that missing
	 *
	 * @param tableName the table name
	 * @param coulmnFamilyArray the coulmn family name
	 * @return the htable
	 * @throws IOException 
	 */
	public static HTable openTable(String tableName, String... coulmnFamilyArray) throws IOException 
	{
		Configuration config = HBaseConfiguration.create();
		

			HBaseAdmin hbase = new HBaseAdmin(config);
			byte[] tableNameByte = tableName.getBytes();
			
			if (hbase.tableExists(tableNameByte))
			{
				// table exists
				log.debug("Table: " + tableName + " already exists!");
				if (hbase.isTableEnabled(tableNameByte))
				{
					// table enabled
					
					HTable hTable =  new HTable(config, tableNameByte);
					
					HColumnDescriptor[]  hColumnDescriptorList = hTable.getTableDescriptor().getColumnFamilies();
					List<String> existColumnNamesList = new ArrayList<String>();
					for (HColumnDescriptor hColumnDescriptor : hColumnDescriptorList)
					{
						existColumnNamesList.add(hColumnDescriptor.getNameAsString());
					}
					
					// checking if all the column family are in the table, adding it if not.
					
					List<String> missingColumnFamilies = new ArrayList<String>();
					for (String coulmnFamily : coulmnFamilyArray)
					{
						if (!existColumnNamesList.contains(coulmnFamily))
						{
							log.warn(coulmnFamily + " does not exist in " + tableName);
							missingColumnFamilies.add(coulmnFamily);
						}
                    }
					
					if (!missingColumnFamilies.isEmpty())
					{
						hbase.disableTable(tableNameByte);
						for (String coulmnFamily : missingColumnFamilies)
                        {
							hbase.addColumn(tableNameByte, new HColumnDescriptor(coulmnFamily.getBytes()));
							log.info(coulmnFamily + " added to " + tableName);
                        }
						hbase.enableTable(tableNameByte);
					}
					
					return hTable;
				}
				
				//table exists but disabled
				log.info("Table: " + tableName + " exists but disabled, deleting the table.");
				hbase.deleteTable(tableNameByte);
			}
			
			//creating a new table
			HTableDescriptor hTableDescriptor = new HTableDescriptor(tableNameByte);
			for (String coulmnFamily : coulmnFamilyArray)
			{
				HColumnDescriptor meta = new HColumnDescriptor(coulmnFamily.getBytes());
				hTableDescriptor.addFamily(meta);
			}
			
			hbase.createTable(hTableDescriptor);
			log.info("New hBase table created with name: " + tableName);

			return new HTable(config, tableNameByte);
		}
	
		

	}
	
	

