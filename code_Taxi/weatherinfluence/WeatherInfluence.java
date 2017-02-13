import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeatherInfluence extends Configured implements Tool{
	/**
	 * Main function which calls the run method and passes the args using ToolRunner
	 * @param args Two arguments input and output file paths
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new WeatherInfluence(), args);
		System.exit(exitCode);
	}

	/**
	 * Run method which schedules the Hadoop Job
	 * @param args Arguments passed in main function
	 */

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: <input path> <output path> <weatherdata path>");
			System.exit(-1);
		}
		//Initialize the Hadoop job and set the jar as well as the name of the Job
		Job job = new Job();
		job.setJarByClass(WeatherInfluence.class);
		job.setJobName("WeatherInfluence");
		job.setNumReduceTasks(1);
		//Add input and output file paths to job based on the arguments passed
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//Set the Mapper Class and Reducer Class in the job
		job.setMapperClass(WeatherInfluenceMapper.class);
		job.setReducerClass(WeatherInfluenceReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());

		//Wait for the job to complete and print if the job was successful or not
		int returnValue = job.waitForCompletion(true) ? 0:1;
		
		if(job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");			
		}
		return returnValue;
	}
}