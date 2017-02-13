import java.io.IOException;
import java.util.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, Text, Text>{
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] line = value.toString().split("\t+");
	String year = line[0].substring(0, 4);
	String month = line[0].substring(4, 6);
	String date = line[0].substring(6, 8);
	String hour = line[0].substring(9, 11);
	String input_date= date + "/" + month + "/" + year;
	String weather = line[0].split(" ")[2];
	try{
			    	  SimpleDateFormat format1=new SimpleDateFormat("dd/MM/yyyy");
			    	  Date dt1=format1.parse(input_date);
//			    	  System.out.println(input_date);
			    	  Calendar c = Calendar.getInstance();
			    	  c.setTime(dt1);
			    	  int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
			    	  if(dayOfWeek == 1 || dayOfWeek == 7){
context.write(new Text("weekend" + " " + hour + " " + weather), new Text(line[1]));			    	 
 }
			    	  else{
context.write(new Text("weekday" +" " +  hour + " " + weather), new Text(line[1]));
			    	  }}catch (ParseException e){
}}
}
