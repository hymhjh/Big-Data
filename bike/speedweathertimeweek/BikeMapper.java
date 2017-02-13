import java.io.IOException;
import java.util.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BikeMapper extends Mapper<LongWritable, Text, Text, Text>{
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] line = value.toString().split(",");
        
        String month = null;
        double slat = 0;
        double slong = 0;
        double elat = 0;
        double elong = 0;
        double timeSpent = 0;
        
        if(!line[0].equals("\"tripduration\"") && !line[0].equals("tripduration")){
            
            if(line[1].charAt(0) == '\"'){
                month = line[1].split(" ")[0].split("/")[0].substring(1);
                slat = Double.valueOf(line[5].substring(1, line[5].length()-1));
                slong = Double.valueOf(line[6].substring(1, line[6].length()-1));
                elat = Double.valueOf(line[9].substring(1, line[9].length()-1));
                elong = Double.valueOf(line[10].substring(1, line[10].length()-1));
                timeSpent = (Double.valueOf(line[0].substring(1, line[0].length()-1))) / 3600;
            }
            else{
                month = line[1].split(" ")[0].split("/")[0];
                slat = Double.valueOf(line[5]);
                slong = Double.valueOf(line[6]);
                elat = Double.valueOf(line[9]);
                elong = Double.valueOf(line[10]);
                timeSpent = (Double.valueOf(line[0])) / 3600;
            }
            
            double distance = distance(slat, slong, elat, elong);
            double speed = distance / (double)timeSpent;
                
            if(month.length() < 2){
                month = "0" + month;
            }
                
            String date =  line[1].split(" ")[0].split("/")[1];

            if(date.length() < 2){
                date = "0" + date;
            }
            
            String year = line[1].split(" ")[0].split("/")[2];
            
            if(year.length() < 4){
                year = "20" + year;
            }

            String hour = line[1].split(" ")[1].split(":")[0];
            
            if(hour.length() < 2){
                hour = "0" + hour;
            }

             context.write(new Text(year +month + date +" " + hour), new Text("#" + String.valueOf(speed)));
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
