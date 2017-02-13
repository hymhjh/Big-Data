import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class analysis {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		if(args.length != 2){
			System.err.println("analysis <input path> <output path>");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(analysis.class);
		job.setJobName("analysis");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(analysisMapper.class);
		job.setReducerClass(analysisReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

