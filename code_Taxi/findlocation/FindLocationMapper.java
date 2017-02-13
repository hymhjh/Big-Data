import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FindLocationMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		if(!line.startsWith("start")) {
			String[] lineArray = line.split(",");
			String[] dateTime = lineArray[0].split("\\s+");
			String date = dateTime[0];
			String hour = dateTime[1].substring(0,2);
			String dateHour = date + hour;
			String censusCode = lineArray[10];
			String censusKey = dateHour + "\t" + censusCode;
			String ntaName = "NTAName" + "\t" + lineArray[13];
			String ntaNameKey = dateHour + "\t" + ntaName;
			context.write(new Text(censusKey), new Text("1"));
			context.write(new Text(ntaNameKey), new Text("1"));
		}
	}
}