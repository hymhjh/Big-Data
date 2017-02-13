import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.text.DecimalFormat;

public class TotalDayAndAvgSpeedReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String keyString = key.toString();
		if(keyString.startsWith("speed")) {
			double totalSpeed = 0;
			int count = 1;
			for(Text value : values) {
				count++;
				totalSpeed += Double.valueOf(value.toString());
			}
			double avgSpeed = totalSpeed / count;
			DecimalFormat df = new DecimalFormat("#.##");      
			avgSpeed = Double.valueOf(df.format(avgSpeed));
			String result = keyString + ": " + String.valueOf(avgSpeed);
			context.write(null, new Text(result));
		} else {
			int totalNumber = 0;
			for(Text value : values) {
				totalNumber += 1;
			}
			String result = keyString + ": " + totalNumber;
			context.write(null, new Text(result));
		}
	}
}