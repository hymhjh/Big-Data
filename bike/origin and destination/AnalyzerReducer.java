import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalyzerReducer extends Reducer<Text, Text, Text, Text>{
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        String startbo = "";
         String startnta = "";
        String endbo = "";
        String endnta = "";
        String hour = null;
        for(Text value : values){
	    int i = 1;
	    String[] tmp = value.toString().split(" ");
	    if(value.toString().substring(0, 2).equals("sb")){
               	startbo = tmp[1];
            }
	    
            else if(value.toString().substring(0, 2).equals("sn")){
		while(i < tmp.length){
                        startnta = startnta + " " + tmp[i];
                        i++;
                }
            }
            if(value.toString().substring(0, 2).equals("eb")){
                endbo = tmp[1];
            }
           else if(value.toString().substring(0, 2).equals("en")){
		while(i < tmp.length){
                        endnta = endnta + " " + tmp[i];
                        i++;
                }
            }
           if(hour == null){
               hour = value.toString().substring(2, 4);
           }
        }
	if(startbo != "" && endbo != ""){
        context.write(new Text(hour + " " + startbo + "," + endbo), new Text("1"));
       context.write(new Text(hour + " " + startnta + "," + endnta), new Text("1"));
	}
    }
}
