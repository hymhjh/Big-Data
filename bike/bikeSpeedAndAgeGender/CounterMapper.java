import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CounterMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] line = value.toString().split(",");
        int birth = 0;
        int age = 0;
        double slat = 0.0;
        double slong = 0.0;
        double elat = 0.0;
        double elong = 0.0;
        double timeSpent = 0.0;
        
     if(!line[0].equals("\"tripduration\"") && line[13].length() != 2 && !line[0].equals("tripduration") && line[13].length() != 0){
 	
         if(line[0].charAt(0) == '\"'){
              birth = Integer.valueOf(line[13].substring(1, line[13].length()-1));
              age = 2016 - birth;
              slat = Double.valueOf(line[5].substring(1, line[5].length()-1));
              slong = Double.valueOf(line[6].substring(1, line[6].length()-1));
              elat = Double.valueOf(line[9].substring(1, line[9].length()-1));
              elong = Double.valueOf(line[10].substring(1, line[10].length()-1));
              timeSpent = (Double.valueOf(line[0].substring(1, line[0].length()-1))) / 3600;
         }
         else{
             birth = Integer.valueOf(line[13]);
             age = 2016 - birth;
             slat = Double.valueOf(line[5]);
             slong = Double.valueOf(line[6]);
             elat = Double.valueOf(line[9]);
             elong = Double.valueOf(line[10]);
             timeSpent = (Double.valueOf(line[0])) / 3600;
         }
        
	double distance = distance(slat, slong, elat, elong);
	double speed = distance / (double)timeSpent;
        //context.write(new Text(String.valueOf(age)), new IntWritable(1));

	String gender = line[14];
	if(gender.equals("\"1\"") || gender.equals("1")){
		//context.write(new Text("male"), new IntWritable(1));
		context.write(new Text(age + " " + "male"), new DoubleWritable(speed));
	}
	else if(gender.equals("\"2\"") || gender.equals("2")){
		//context.write(new Text("female"), new IntWritable(1));
		context.write(new Text(age + " " + "female"), new DoubleWritable(speed));
	}
      }
}
	public double distance(double lat1, double long1, double lat2, double long2){
		double dis = 0.0;
		double EARTH_RADIUS = 6378.137;
		double radLat1 = rad(lat1);
		double radLat2 = rad(lat2);
		double a = radLat1 - radLat2;
		double b = rad(long1) - rad(long2);
		dis = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a/2),2) + Math.cos(radLat1)*Math.cos(radLat2)*Math.pow(Math.sin(b/2),2)));
		dis = dis * EARTH_RADIUS;
		dis = Math.round(dis * 10000) / 10000;
		return dis*0.621371;
	}
	public double rad(double lat1) {
		return lat1 * Math.PI / 180.0;
	}
}


