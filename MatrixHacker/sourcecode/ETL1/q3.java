 
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.ByteBuffer;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
/*
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.HTablePool;  
import org.apache.hadoop.hbase.client.Put;
*/
public class q3 {
	                      
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		private Text word2 = new Text();
		private final static IntWritable one = new IntWritable(1);
		static HashSet<String> stopSet = new   HashSet<String>();
          
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			reporter.progress();
			String line = value.toString().split("\t")[1];
			JsonParser parser = new JsonParser();
			JsonObject jsonObject = (JsonObject)parser.parse(line);
			jsonObject = jsonObject.get("user").getAsJsonObject();
			
			word.set(jsonObject.get("id").toString());
			output.collect(word, one);
			/*
			try {
				JsonParser parser = new JsonParser();
				JsonObject jsonObject = (JsonObject)parser.parse(line);
				word.set(jsonObject.get("userid").toString());
				output.collect(word, one);    
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			*/ 
		}                
	}                    
                         
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		/*
		private Configuration hconf=null;
		HTablePool pool = null;
		HTable table = null;
		public void configure(JobConf conf){
			hconf=HBaseConfiguration.create();
			pool = new HTablePool(hconf, 1000);  
			table = (HTable) pool.getTable("q3");  
		}
		*/
		public byte[] longToBytes(long x) {
			ByteBuffer buffer = ByteBuffer.allocate(8);
			buffer.putLong(x);
			return buffer.array();
		}
		public byte[] intToBytes(int x) {
			ByteBuffer buffer = ByteBuffer.allocate(4);
			buffer.putInt(x);
			return buffer.array();
		}
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			reporter.progress();
			IntWritable out = new IntWritable(0);
			int count = 0;
			while (values.hasNext()) {
				count += values.next().get();
				
			}
			out.set(count);
			output.collect(key, out);
		}                
	}                    
                         
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(q3.class);
		conf.setJobName("q3");
                         
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
                         
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
                         
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
                         
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
                         
		JobClient.runJob(conf);
	}                    
}                        