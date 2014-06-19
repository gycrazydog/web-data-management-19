package org.myorg;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.mahout.text.wikipedia.XmlInputFormat;
public class WC   {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	
		//conf.set("xmlinput.start", "<movie>");
		  //conf.set("xmlinput.end", "</movie>");
		  conf.set("xmlinput.start", "<movie>");
		  conf.set("xmlinput.end", "</movie>");
		// Use programArgs array to retrieve program arguments.
		Job job = new Job(conf);
		job.setJarByClass(WC.class);
		job.setMapperClass(ActorMapper.class);
		job.setNumReduceTasks(0);
		//job.setCombinerClass(Reduce.class);
		//job.setReducerClass(Reduce.class);
		
		//  job.setMapOutputKeyClass(Text.class);
		 // job.setMapOutputValueClass(Text.class);         
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(XmlInputFormat.class);//
		
		job.setOutputFormatClass(TextOutputFormat.class);//
		job.setNumReduceTasks(0);
//		DistributedCache.addFileToClassPath(new Path("/lib/mahout-integration-0.7.jar"), conf, fs);
//		DistributedCache.addFileToClassPath(new Path("/lib/jdom-2.0.5.jar"), conf, fs);
		
		//job.setInputFormatClass(TextInputFormat.class);//
		//job.setOutputFormatClass(TextOutputFormat.class);//
		// TODO: Update the input path for the location of the inputs of the map-reduce job.
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// TODO: Update the output path for the output directory of the map-reduce job.
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Submit the job and wait for it to finish.
		job.waitForCompletion(true);
		// Submit and return immediately: 
		// job.submit();
		//JobClient.runJob(conf);
	}
	

}
