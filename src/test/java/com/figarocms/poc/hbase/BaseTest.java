package com.figarocms.poc.hbase;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Base class starting an Hbase Mini Cluster.
 * 
 * @author nhuray
 * 
 */
public abstract class BaseTest {

	private final static HBaseTestingUtility Hbase = new HBaseTestingUtility();
	protected HTable hTable;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Hbase.startMiniCluster(1);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		Hbase.shutdownMiniCluster();
	}

}
