import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalyzerReducer extends Reducer<Text, Text, Text, Text>{
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        double sum = 0.0;
	int count = 0;
	String weather = null;
        for(Text value : values){
            if(value.toString().charAt(0) == '#'){
                sum += Double.valueOf(value.toString().substring(1));
		count++;
            }
	    else{
	       weather = value.toString();   
	    }
        }
        context.write(new Text(key.toString() + " " + weather), new Text(String.valueOf(sum/(double)count)));
    }
}
