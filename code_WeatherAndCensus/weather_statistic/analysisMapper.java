import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class analysisMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
		
		String line = value.toString();
		String[] sp = line.split(" ");
		int year = Integer.parseInt(sp[0].substring(0,4));
		int month = Integer.parseInt(sp[0].substring(4,6));
		int day = Integer.parseInt(sp[0].substring(6,8));
		if(month < 3) {
			year--;
			month += 12;
		}
		int century = year/100;
		int yearInCentury = year % 100;
		int w = (day + yearInCentury + (yearInCentury/4) + (century/4) + (5*century) + ((month+1)*26/10))%7;
		String classify = "";
		if(w == 0 || w == 1) classify = "Weekends";
		else if(w == 2 || w == 3 || w ==4 || w == 5 || w == 6) classify = "Weekdays";
		else classify = "Nothing";
		if(sp.length == 3) context.write(new Text(classify+" "+sp[1]), new Text(sp[2]));
		else context.write(new Text(classify+" "+sp[1]), new Text(sp[2]+" "+sp[3]));
	}
}

