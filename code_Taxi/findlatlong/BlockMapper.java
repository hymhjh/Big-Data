import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BlockMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineArray = line.split(",");
		String pickupDateTime = lineArray[0];
		String pickupDate = pickupDateTime.split("\\s+")[0];
		String pickupHour = pickupDateTime.split("\\s+")[1].substring(0,2);
		//2015-05-02 08-10, 17-19，1012,1517
		if(pickupDate.equals("20150502") && (pickupHour.equals("15") || pickupHour.equals("16"))) {
			context.write(new Text(line), new Text("1"));
		}
	}
}