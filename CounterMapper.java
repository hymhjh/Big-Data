import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CounterMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] line = value.toString().split(",");
        if(!line[0].equals("OBJECTID")){
        int h = Integer.valueOf(line[4].split(" ")[1].split(":")[0]);
        if(h != 12)
	    h += 12;
        h -= 4;
        String hour = null;
        
        if(h < 10){
            hour = "0" + String.valueOf(h);
        }
        
        else{
            hour = String.valueOf(h);
        }
        
        String boroct2010 = line[13];
        String ntaname = line[16];
        
        context.write(new Text(hour + "\t" + boroct2010), new IntWritable(1));
        context.write(new Text(hour + "\t" + ntaname), new IntWritable(1));

        }
      }

	
}


