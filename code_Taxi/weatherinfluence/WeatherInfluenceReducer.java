import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherInfluenceReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int totalNumber = 0;
		for(Text value : values) {
			totalNumber += 1;
		}
		String keyString = key.toString();
		String result = keyString + " " + totalNumber;
		context.write(null, new Text(result));
	}
}