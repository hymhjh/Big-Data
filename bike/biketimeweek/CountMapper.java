import java.io.IOException;
import java.util.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] line = value.toString().split(",");
        
        String month = null;
        
        if(!line[0].equals("\"tripduration\"") && !line[0].equals("tripduration")){
            
            if(line[1].charAt(0) == '\"'){
                month = line[1].split(" ")[0].split("/")[0].substring(1);
            }
            else{
                month = line[1].split(" ")[0].split("/")[0];
            }
                
            if(month.length() < 2){
                month = "0" + month;
            }
                
            String date =  line[1].split(" ")[0].split("/")[1];

            if(date.length() < 2){
                date = "0" + date;
            }
            
            String year = line[1].split(" ")[0].split("/")[2];
            
            if(year.length() < 4){
                year = "20" + year;
            }

            String hour = line[1].split(" ")[1].split(":")[0];
            
            if(hour.length() < 2){
                hour = "0" + hour;
            }
                
            String input_date= date + "/" + month + "/" + year;
            try{
                SimpleDateFormat format1=new SimpleDateFormat("dd/MM/yyyy");
                Date dt1=format1.parse(input_date);
                Calendar c = Calendar.getInstance();
                c.setTime(dt1);
                int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
                if(dayOfWeek == 1 || dayOfWeek == 7){
                    context.write(new Text("weekend" + " " + hour), new IntWritable(1));
                }
                else{
                    context.write(new Text("weekday" +" " +  hour), new IntWritable(1));
                }
            }catch (ParseException e){ }
        
	   }
	
    }
}
