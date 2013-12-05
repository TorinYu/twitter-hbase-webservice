 
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class removeDup2 {
	                      
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
				word.set(jsonObject.get("id").getAsString() );
				/*
				int i = line.indexOf("text");
				int j = line.indexOf("\"",i+7);
				while (line.charAt(j-1)=='\\'){
					j = line.indexOf("\"",j+1);
				}
				String text1 = line.substring(i+7,j);
				String text = jsonObject.get("created_at").getAsString()+"\t"+text1;
				*/
				word2.set(line);
				output.collect(word, value); 
			}
		}                
	}                    
                         
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		Text word = new Text();
		Text word2 = new Text();
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			reporter.progress();
			//Text result = null;
			String out = values.next().toString();
			/*
			JsonParser parser = new JsonParser();
			JsonObject jsonObject = (JsonObject)parser.parse(out);
			word.set(jsonObject.get("created_at").getAsString() );
			String tid = jsonObject.get("id").toString();
			*/
			/*
			int i1 = out.indexOf("created_at");
			int j1 = out.indexOf("\"",i1+13);
			if (j1 == -1){
				word2.set(out);
				output.collect(key,word2);
			}
			else{
				String s = out.substring(i1+13,j1);
				int i = out.indexOf("text");
				int j = out.indexOf("\"",i+7);
				if (j == -1){
					output.collect(word,word2);
				}
				while (out.charAt(j-1)=='\\'){
					j = out.indexOf("\"",j+1);
				}
				String text1 = out.substring(i+7,j);
				word.set(s);
				word2.set(key.toString()+":"+text1);
				output.collect(word,word2);
			}
			*/
			word2.set(out);
			output.collect(key,word2);
		}                
	}                    
                         
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(removeDup2.class);
		conf.setJobName("rd");
        //conf.setNumReduceTasks(2);   
		//conf.setNumMapTasks(4); 
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