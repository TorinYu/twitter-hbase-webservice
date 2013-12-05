 
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map.Entry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonNull;
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

public class q4 {
	                      
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text word2 = new Text();
		static HashSet<String> stopSet = new   HashSet<String>();
		
          
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			reporter.progress();
			String line = value.toString();
			if (line.length() != 0){
				JsonParser parser = new JsonParser();
				JsonObject jsonObject = (JsonObject)parser.parse(line);
				word.set(jsonObject.getAsJsonObject("user").get("id").getAsString() );
				JsonElement j2 = jsonObject.get("retweeted_status");
				
				if (!(j2 == null)){
					jsonObject = j2.getAsJsonObject();
					j2 = jsonObject.get("retweeted_status");
					while (!(j2 == null)){
						jsonObject = j2.getAsJsonObject();
						j2 = jsonObject.get("retweeted_status");
					}
					word2.set(jsonObject.getAsJsonObject("user").get("id").getAsString());
					output.collect(word2, word); 
				}
			}
		}                
	}                    
                         
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		/*
		private Configuration hconf=null;
		public void configure(JobConf conf){
			hconf=HBaseConfiguration.create();
		}
		public void insertData(String tableName, String key, String data) {  
			HTablePool pool = new HTablePool(hconf, 1000);  
			HTable table = (HTable) pool.getTable(tableName);  
			Put put = new Put(key.getBytes());
			put.add("cf1".getBytes(), data.getBytes(), data.getBytes());
			try {  
				table.put(put);  
			} catch (IOException e) {  
				e.printStackTrace();  
			}  
		}  
		*/
		private Text word = new Text();
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			//reporter.progress();
			//HashSet<String> set = new HashSet<String>();
			TreeMap<Long,String> map = new TreeMap<Long,String>();
			StringBuilder sb = new StringBuilder();
			int count = 0;
			while (values.hasNext()) {
				String[] s = values.next().toString().split(",");
				for (int i = 0; i < s.length; i++){
					String tmp = s[i];
					map.put(Long.parseLong(tmp),tmp);
				}
			}  
			Iterator it = map.entrySet().iterator();
			while (it.hasNext()){
				Entry e = (Entry)it.next();
				if (count > 0)
					sb.append(",");
				sb.append((String)e.getValue());
				count = 1;
			}
			word.set(sb.toString());
			output.collect(key, word);
		}                
	}                    
                         
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(q4.class);
		conf.setJobName("q4");
                         
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
                         
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