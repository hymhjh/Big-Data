import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, Text, Text, Text>{
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        int sum = 0;
        int count = 0;
        for(Text value : values){
                sum += Integer.valueOf(value.toString());
                count++;
            
        }
        context.write(key, new Text(String.valueOf(sum/count)));
    }
}
