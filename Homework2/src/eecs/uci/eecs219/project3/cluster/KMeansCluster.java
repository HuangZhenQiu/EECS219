package eecs.uci.eecs219.project3.cluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class KMeansCluster {

	public static class KMeansClusterMapper extends TableMapper<Text, LongWritable> {
		private HTable currentCenter;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			Configuration config = HBaseConfiguration.create();
			currentCenter = new HTable(config, "center");
			super.setup(context);
		}
	}
}
