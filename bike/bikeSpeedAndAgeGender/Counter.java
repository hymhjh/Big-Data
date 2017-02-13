import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Counter{
    
    public static void main(String[] args) throws Exception {
        
        if(args.length != 2){
            System.err.println("Usage : Counter <input path 1> <output path>");
            System.exit(-1);
        }
        
        
        Job job = new Job();
        job.setJarByClass(Counter.class);
        job.setJobName("Counter");
        
        job.setMapperClass(CounterMapper.class);
        job.setReducerClass(CounterReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
