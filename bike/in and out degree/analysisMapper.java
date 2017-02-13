import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class analysisMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
		
		String line = value.toString();
		String[] sp = line.split(",");
        String inn = null;//in station id
        String outn = null;//out station id
        
        if(sp[0].charAt(0) == '\"'){
		 inn = sp[7].substring(1, sp[7].length()-1);//in station id
		 outn = sp[3].substring(1, sp[3].length()-1);//out station id
        }
        else{
             inn = sp[7];//in station id
             outn = sp[3];//out station id
        }
		context.write(new Text(inn), new Text(1+","+0+","+sp[9]+","+sp[10]));
		context.write(new Text(outn), new Text(0+","+1+","+sp[5]+","+sp[6]));
	}
}

