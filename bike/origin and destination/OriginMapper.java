import java.io.IOException;
import java.util.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OriginMapper extends Mapper<LongWritable, Text, Text, Text>{
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] line = value.toString().split(",");
           if(!line[0].equals("OBJECTID")){
	    String stime = line[4];
            String etime = line[5];
            String bikeid = line[8];
            int h = Integer.valueOf(line[4].split(" ")[1].split(":")[0]);
            if(h != 12){
                h += 12;
            }
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
            
            context.write(new Text(stime + etime + bikeid), new Text("sb" + hour + " " + boroct2010));
            context.write(new Text(stime + etime + bikeid), new Text("sn" + hour + " " + ntaname));
            
            
        }
    }
}
