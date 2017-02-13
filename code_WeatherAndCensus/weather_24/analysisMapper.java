import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class analysisMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
		
		String line = value.toString();
		String[] sp = line.split(",");
		String out = "";
		out = sp[2] + "," + sp[4] + "," + sp[8] + "," + sp[40]; 
		context.write(new Text(sp[1]), new Text(out));
		
	}
}

