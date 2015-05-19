package eecs.uci.eecs219.project3.cluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import edu.uci.eecs219.util.HbaseUtil;
import eecs.uci.eecs219.project3.LocalDriver.State;


/***
 * 
 * This map reduce file read from input table and write to center table. The content of original centers
 * will be loaded into memory for both mapper and reducer for calculating the distance.
 * 
 * Given a row of data, the mapper will find the nearest center point, then write the row of data to the key of center point.
 * Therefore, points from input will be grouped together to its nearest center point.
 * 
 * Given a set of data, the reducer will find the new center point k and compare with original center point k,
 * If the distance of them is bigger than a threshold, then increase the UPDATED counter, and write the new center data
 * into the center table.
 * 
 * 
 * @author hadoop
 *
 */
public class KMeansCluster {

	public static class KMeansClusterMapper extends TableMapper<Text, Result> {
		private Map<String, KeyValue[]> centers;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			centers = HbaseUtil.loadFromHTable("center");
			super.setup(context);
		}
		
		public void map(ImmutableBytesWritable row, Result value, Context context)
			throws IOException, InterruptedException {
			String pointId = HbaseUtil.findNearestPoint(centers, value.raw());
			
			// Map to the nearest centroid
			context.write(new Text(pointId), value);
		}
	}
	
	public static class KMeansClusterReducer extends TableReducer<Text, Result, ImmutableBytesWritable> {
		private Map<String, KeyValue[]> centers;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			centers = HbaseUtil.loadFromHTable("center");
			super.setup(context);
		}
		
		public void reduce(Text key, Iterable<Result> results, Context context)
				throws IOException, InterruptedException {
			List<Double> center = HbaseUtil.findCenter(results);
			KeyValue[] oldCenter = centers.get(key.toString());
			Double distance = HbaseUtil.distance(center, oldCenter);
			if (distance > 0.1) {
				context.getCounter(State.UPDATED).increment(1);
				//Write to center table
				Put put = new Put(Bytes.toBytes(key.toString()));
				HbaseUtil.buildPutForRow(put, center);
				context.write(null, put);
			}
		}
	}
}
