import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class analysisReducer extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
		int in = 0;
		int out = 0;
		String name = "";
		for(Text value : values){
			String[] sp = value.toString().split(",");
			if(Integer.parseInt(sp[0]) == 1){
				in++;
			}else{
				out++;
			}
			name = sp[2]+"\t"+sp[3];
		}
		int diff = (in - out)/365;
		in = in / 365;
		out = out / 365;
		context.write(key, new Text(diff+"\t"+name + "\t" + in + "\t" + out));
	}
}
