import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Analyzer{
    
    public static void main(String[] args) throws Exception {
        
        if(args.length != 4){
            System.err.println("Usage : Analyzer <input path 1> <input path 2> <input path 3> <output path>");
            System.exit(-1);
        }
        
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        
        Path p1=new Path(files[0]);
        Path p2=new Path(files[1]);
        Path p3=new Path(files[2]);
        Path p4=new Path(files[3]);

        Job job = new Job(c, "job1");
        job.setJarByClass(Analyzer.class);
        job.setJobName("Analyzer");

	job.setMapperClass(OriginMapper.class);        
        job.setMapperClass(DestMapper.class);
        job.setReducerClass(AnalyzerReducer.class);
        
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, OriginMapper.class);
        MultipleInputs.addInputPath(job, p2, TextInputFormat.class, DestMapper.class);
        FileOutputFormat.setOutputPath(job, p3);
        
        job.waitForCompletion(true);
	
	//second job
        Job job2 = new Job(c, "job2");
        job2.setJarByClass(Analyzer.class);
        job2.setJobName("Analyzer");
        
        
        job2.setMapperClass(CountMapper.class);
        job2.setReducerClass(CountReducer.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        

        FileInputFormat.addInputPath(job2, p3);
	FileOutputFormat.setOutputPath(job2, p4);
	System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
