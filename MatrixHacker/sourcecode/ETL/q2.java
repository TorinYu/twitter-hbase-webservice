 
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
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
public class q2 {
	                      
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
		private Text word2 = new Text();
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString().split("\t")[1];
			if (line.length() != 0){
				
				JsonParser parser = new JsonParser();
				JsonObject jsonObject = (JsonObject)parser.parse(line);
				String k = jsonObject.get("created_at").getAsString() ;
				
				String tid = jsonObject.get("id").getAsString() ;
				//String text = jsonObject.get("text").getAsString() ;
				int i = line.indexOf("text");
				int j = line.indexOf("\"",i+7);
				while (line.charAt(j-1)=='\\'){
					j = line.indexOf("\"",j+1);
				}
				String text = line.substring(i+7,j);
				word.set(k);
				word2.set(tid+":"+text);
				output.collect(word, word2);
			}
		}                
	}                    
                         
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private Text word = new Text();
		private Text word2 = new Text();
		
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int count = 0;
			StringBuilder sb = new StringBuilder();
			while (values.hasNext()){
				String s = values.next().toString();
				if (count != 0)
					sb.append("\t");
				sb.append(s);
				count = 1;
			}
			word2.set(sb.toString());
			output.collect(key, word2);
		}                
	}                    
                         
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(q2.class);
		conf.setJobName("q2");
                         
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