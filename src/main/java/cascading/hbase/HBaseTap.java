/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
 */

package cascading.hbase;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.hadoop.TapCollector;
import cascading.tap.hadoop.TapIterator;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HBaseTap class is a {@link Tap} subclass. It is used in conjunction with
 * the {@HBaseFullScheme} to allow for the reading and writing
 * of data to and from a HBase cluster.
 */
public class HBaseTap extends Tap {
    /** Field LOG */
    private static final Logger LOG = LoggerFactory.getLogger(HBaseTap.class);

    /** Field SCHEME */
    public static final String SCHEME = "hbase";

    /** Field hBaseAdmin */
    private transient HBaseAdmin hBaseAdmin;

    private String tableName;

    private boolean isUsedAsBothSourceAndSink;

    /**
     * Constructor HBaseTap creates a new HBaseTap instance.
     * 
     * @param tableName
     *            of type String
     * @param HBaseFullScheme
     *            of type HBaseFullScheme
     */
    public HBaseTap(String tableName, HBaseAbstractScheme HBaseFullScheme) {
	this(tableName, HBaseFullScheme, SinkMode.APPEND);
    }
    
    /**
     * Instantiates a new h base tap.
     *
     * @param tableName the table name
     * @param HBaseFullScheme the h base full scheme
     * @param isUsedAsBothSourceAndSink the tap could be used as both source and sink
     */
    public HBaseTap(String tableName, HBaseAbstractScheme HBaseFullScheme, boolean isUsedAsBothSourceAndSink) {
	this(tableName, HBaseFullScheme, SinkMode.APPEND, isUsedAsBothSourceAndSink);
    }
    
    /**
     * Instantiates a new h base tap.
     *
     * @param tableName the table name
     * @param HBaseFullScheme the h base full scheme
     * @param sinkMode the sink mode
     */
    public HBaseTap(String tableName, HBaseAbstractScheme HBaseFullScheme,
	    SinkMode sinkMode) {
	this(tableName, HBaseFullScheme, sinkMode, false);
    }

    /**
     * Constructor HBaseTap creates a new HBaseTap instance.
     *
     * @param tableName of type String
     * @param HBaseFullScheme of type HBaseFullScheme
     * @param sinkMode of type SinkMode
     * @param isUsedAsBothSourceAndSink the tap could beused as both source and sink
     */
    public HBaseTap(String tableName, HBaseAbstractScheme HBaseFullScheme,
	    SinkMode sinkMode, boolean isUsedAsBothSourceAndSink) {
	super(HBaseFullScheme, sinkMode);
	this.tableName = tableName;
	this.isUsedAsBothSourceAndSink = isUsedAsBothSourceAndSink;
    }

    private URI getURI() {
	try {
	    return new URI(SCHEME, tableName, null);
	} catch (URISyntaxException exception) {
	    throw new TapException("unable to create uri", exception);
	}
    }

    public Path getPath() {
	return new Path(SCHEME + "://" + tableName);
    }

    public TupleEntryIterator openForRead(JobConf conf) throws IOException {
	return new TupleEntryIterator(getSourceFields(), new TapIterator(this,
		conf));
    }

    public TupleEntryCollector openForWrite(JobConf conf) throws IOException {
	return new TapCollector(this, conf);
    }

    private HBaseAdmin getHBaseAdmin() throws MasterNotRunningException,
	    ZooKeeperConnectionException {
	if (hBaseAdmin == null)
	    hBaseAdmin = new HBaseAdmin(HBaseConfiguration.create());

	return hBaseAdmin;
    }

    public static HTable openTable(String tableName,
	    String... coulmnFamilyArray) throws IOException {
	Configuration config = HBaseConfiguration.create();

	HBaseAdmin hbase = new HBaseAdmin(config);
	byte[] tableNameByte = tableName.getBytes();

	if (hbase.tableExists(tableNameByte)) {
	    // table exists
	    LOG.debug("Table: " + tableName + " already exists!");
	    if (hbase.isTableEnabled(tableNameByte)) {
		// table enabled

		HTable hTable = new HTable(config, tableNameByte);

		HColumnDescriptor[] hColumnDescriptorList = hTable
			.getTableDescriptor().getColumnFamilies();
		List<String> existColumnNamesList = new ArrayList<String>();
		for (HColumnDescriptor hColumnDescriptor : hColumnDescriptorList) {
		    existColumnNamesList.add(hColumnDescriptor
			    .getNameAsString());
		}

		// checking if all the column family are in the table, adding it
		// if not.

		List<String> missingColumnFamilies = new ArrayList<String>();
		for (String coulmnFamily : coulmnFamilyArray) {
		    if (!existColumnNamesList.contains(coulmnFamily)) {
			LOG.warn(coulmnFamily + " does not exist in "
				+ tableName);
			missingColumnFamilies.add(coulmnFamily);
		    }
		}

		if (!missingColumnFamilies.isEmpty()) {
		    hbase.disableTable(tableNameByte);
		    for (String coulmnFamily : missingColumnFamilies) {
			hbase.addColumn(tableNameByte, new HColumnDescriptor(
				coulmnFamily.getBytes()));
			LOG.info(coulmnFamily + " added to " + tableName);
		    }
		    hbase.enableTable(tableNameByte);
		}

		return hTable;
	    }

	    // table exists but disabled
	    LOG.info("Table: " + tableName
		    + " exists but disabled, deleting the table.");
	    hbase.deleteTable(tableNameByte);
	}

	// creating a new table
	HTableDescriptor hTableDescriptor = new HTableDescriptor(tableNameByte);
	for (String coulmnFamily : coulmnFamilyArray) {
	    HColumnDescriptor meta = new HColumnDescriptor(
		    coulmnFamily.getBytes());
	    hTableDescriptor.addFamily(meta);
	}

	hbase.createTable(hTableDescriptor);
	LOG.info("New hBase table created with name: " + tableName);

	return new HTable(config, tableNameByte);

    }

    public boolean makeDirs(JobConf conf) throws IOException {

	openTable(tableName,
		((HBaseAbstractScheme) getScheme()).getFamilyNames());
	return true;
    }

    public boolean deletePath(JobConf conf) throws IOException {
	// eventually keep table meta-data to source table create
	HBaseAdmin hBaseAdmin = getHBaseAdmin();

	if (!hBaseAdmin.tableExists(tableName))
	    return true;

	LOG.debug("deleting hbase table: {}", tableName);

	hBaseAdmin.disableTable(tableName);
	hBaseAdmin.deleteTable(tableName);

	return true;
    }

    public boolean pathExists(JobConf conf) throws IOException {
	return getHBaseAdmin().tableExists(tableName);
    }

    public long getPathModified(JobConf conf) throws IOException {
	return System.currentTimeMillis(); // currently unable to find last mod
					   // time on a table
    }

    @Override
    public void sinkInit(JobConf conf) throws IOException {
	LOG.debug("sinking to table: {}", tableName);

	// do not delete if initialized from within a task
	if (isReplace() && conf.get("mapred.task.partition") == null)
	    deletePath(conf);

	makeDirs(conf);

	conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
	super.sinkInit(conf);
    }

    @Override
    public void sourceInit(JobConf conf) throws IOException {
	LOG.debug("sourcing from table: {}", tableName);

	FileInputFormat.addInputPaths(conf, tableName);
	super.sourceInit(conf);
    }

    @Override
    public boolean equals(Object object) {
	// HACK - the tap could be used as source and as sink
	if (isUsedAsBothSourceAndSink)
	    return false;
	
	if (object == null)
	    return false;
	if (this == object)
	    return true;
	if (!(object instanceof HBaseTap))
	    return false;
	if (!super.equals(object))
	    return false;

	HBaseTap tap = (HBaseTap) object;

	if (tableName == null ? tap.tableName != null : !tableName
		.equals(tap.tableName))
	    return false;

	return true;
    }

    @Override
    public int hashCode() {
	int result = super.hashCode();
	result = 31 * result + (tableName == null ? 0 : tableName.hashCode());
	return result;
    }

    @Override
    public String toString() {
	return getPath().toString();
    }
}
