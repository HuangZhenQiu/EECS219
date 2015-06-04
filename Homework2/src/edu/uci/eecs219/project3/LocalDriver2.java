package edu.uci.eecs219.project3;

import edu.uci.eecs219.project3.cluster.KMeansCluster;
import edu.uci.eecs219.project3.load.DataLoader;
import edu.uci.eecs219.util.HbaseUtil;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LocalDriver2 extends Configured implements Tool{
	
	// Used for counter key word
	public static enum State {
		UPDATED;
	}

	@Override
	public int run(String[] args) throws Exception {

		if(args.length != 4) {
			System.err.println("Usage: Kmeans Cluster on Hbase <input file> <input table name> <center table name> <K points>");
			System.exit(-1);
		}
		
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		// Clean data generated of last operation
		deleteTable(admin, args[1]);
		deleteTable(admin, args[2]);
		// Create input and center table
		createTable(admin, args[1]);
		createTable(admin, args[2]);
		
		int k = Integer.parseInt(args[3]);
		
		HTable center = new HTable(conf, args[2]);
		putInitialPoints(center, k);
		
		Job loadJob = new Job(conf, "Loading data to hbase table");
		loadJob.setJarByClass(DataLoader.class);
		FileInputFormat.setInputPaths(loadJob, new Path(args[0]));
		loadJob.setInputFormatClass(SequenceFileInputFormat.class);
		loadJob.setMapperClass(DataLoader.DataLoaderMapper.class);
		
		TableMapReduceUtil.initTableReducerJob(args[1], null, loadJob);
		loadJob.setNumReduceTasks(0); // no reducer is needed.
		loadJob.waitForCompletion(true);
		
		// Set
		boolean hasUpdates = true;
		while (hasUpdates) {
			
			Job clusterJob = new Job(conf, "Clustering data");
			clusterJob.setJarByClass(KMeansCluster.class);
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);  // Can't be true for MR jobs
			TableMapReduceUtil.initTableMapperJob(
					args[1],
					scan, 
					KMeansCluster.KMeansClusterMapper.class, 
					Text.class, Result.class, clusterJob);
			
			TableMapReduceUtil.initTableReducerJob(
					args[1], 
					KMeansCluster.KMeansClusterReducer.class,
					clusterJob);
			
			clusterJob.setNumReduceTasks(k);
			if (!clusterJob.waitForCompletion(true)) {
				return -1;
			}
			
			//Check the counter value;
			long updatedCenter = loadJob.getCounters().findCounter(State.UPDATED).getValue();
			hasUpdates = updatedCenter > 0;
		}
	
		return 0;
	}
	
	private void deleteTable(HBaseAdmin admin, String tableName) throws IOException {
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
	}
	
	private void createTable(HBaseAdmin admin, String tableName) throws IOException {
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		tableDesc.addFamily(new HColumnDescriptor("Area"));
		tableDesc.addFamily(new HColumnDescriptor("Property"));
		admin.createTable(tableDesc);
	}
	
	private void putInitialPoints(HTable table, Integer k) throws IOException {
		Random random = new Random();
		random.setSeed(100);
		for (int i = 0; i < k; i++) {
			HbaseUtil.addRecord(table, "center" + k, "Area", "X1", new Long(random.nextLong()).toString());
			HbaseUtil.addRecord(table, "center" + k, "Area", "X5", new Long(random.nextLong()).toString());
			HbaseUtil.addRecord(table, "center" + k, "Area", "X6", new Long(random.nextLong()).toString());
			HbaseUtil.addRecord(table, "center" + k, "Area", "Y1", new Long(random.nextLong()).toString());
			HbaseUtil.addRecord(table, "center" + k, "Area", "Y2", new Long(random.nextLong()).toString());
			HbaseUtil.addRecord(table, "center" + k, "Property", "X2", new Long(random.nextLong()).toString());
			HbaseUtil.addRecord(table, "center" + k, "Property", "X3", new Long(random.nextLong()).toString());
			HbaseUtil.addRecord(table, "center" + k, "Property", "X4", new Long(random.nextLong()).toString());
			HbaseUtil.addRecord(table, "center" + k, "Property", "X7", new Long(random.nextLong()).toString());
			HbaseUtil.addRecord(table, "center" + k, "Property", "X8", new Long(random.nextLong()).toString());
		}
	}

	public static void main(String[] args) throws Exception {
		LocalDriver2 driver = new LocalDriver2();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}
