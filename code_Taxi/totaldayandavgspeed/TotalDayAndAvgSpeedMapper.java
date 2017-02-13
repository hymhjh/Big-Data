import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalDayAndAvgSpeedMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineArray = line.split(",");
		String pickupDateTime = lineArray[0];
		String[] pickupDateTimeArray = pickupDateTime.split(" ");
		String pickupDate = pickupDateTimeArray[0];
		String pickupTime = pickupDateTimeArray[1];
		int year = Integer.valueOf(pickupDate.substring(0, 4));
		int month = Integer.valueOf(pickupDate.substring(4, 6));
		int day = Integer.valueOf(pickupDate.substring(6, 8));
		//4 weeks in May, so we need only 28 days;
		if (month < 3) {
			month += 12;
		    year--;
		}
		int century = year / 100;
		int yearInCentury = year % 100;
		int w = (day + yearInCentury + (yearInCentury / 4) + (century / 4) + (5 * century) + ((month + 1) * 26 / 10)) % 7;
		//w = 0,1 means Saturday or Sunday,
		switch (w) {
		    case 0: context.write(new Text("Saturday"), new Text("1"));
		        	break;
		    case 1: context.write(new Text("Sunday"), new Text("1"));
		        	break;
		    case 2: context.write(new Text("Monday"), new Text("1"));
		        	break;
		    case 3: context.write(new Text("Tuesday"), new Text("1"));
		        	break;
		    case 4: context.write(new Text("Wednesday"), new Text("1"));
		        	break;
		    case 5: context.write(new Text("Thursday"), new Text("1"));
		        	break;
		    case 6: context.write(new Text("Friday"), new Text("1"));
		        	break;
		    default: context.write(new Text("Wrong result"), new Text("1"));
		        	break;
		}
		int hour = Integer.valueOf(pickupTime.substring(0, 2));
		String hourKey = new String();
		if(w <= 1) {
			hourKey = "weekends" + " " + hour + "-" + (hour+1);
		} else if(w >= 2 && w <=6) {
			hourKey = "weekdays" + " " + hour + "-" + (hour+1);
		}
		context.write(new Text(hourKey), new Text("1"));
		//calculate the speed;
		String[] dropoffDateTimeArray = lineArray[1].split(" ");
		String dropoffTime = dropoffDateTimeArray[1];
		double pickupHour = Double.valueOf(pickupTime.substring(0,2)) + Double.valueOf(pickupTime.substring(2,4)) / 60 + Double.valueOf(pickupTime.substring(4,6)) / 3600;
		double dropoffHour = Double.valueOf(dropoffTime.substring(0,2)) + Double.valueOf(dropoffTime.substring(2,4)) / 60 + Double.valueOf(dropoffTime.substring(4,6)) / 3600;
		double distance = Double.valueOf(lineArray[3]);
		if(dropoffHour > pickupHour && distance > 0) {
			double hourelapsed = dropoffHour - pickupHour;
			double speed = distance / hourelapsed;
			String speedKey = "speed" + " " + hourKey;
			if(speed <= 50) {
				context.write(new Text(speedKey), new Text(String.valueOf(speed)));
			}
		}
	}
}
