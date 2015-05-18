package eecs.uci.eecs219.project3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LocalDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {

		if(args.length != 2) {
			System.err.println("Usage: Kmeans Cluster on Hbase  <input table name> <center table name> <K points>");
			System.exit(-1);
		}
		
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		// Clean data generated of last operation
		deleteTable(admin, args[0]);
		deleteTable(admin, args[1]);
		createTable(admin, args[0]);
		createTable(admin, args[1]);
		
		HTable center = new HTable(conf, args[1]);
		putInitialPoints(center, Integer.parseInt(args[2]));
		
		
		
		
		
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
	
	private void putInitialPoints(HTable table, Integer k) {
		for (int i = 0; i < k; i++) {
			
		}
	}

	public static void main(String[] args) throws Exception {
		LocalDriver driver = new LocalDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}
