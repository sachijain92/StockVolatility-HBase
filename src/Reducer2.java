import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public  class Reducer2 extends TableReducer<Text, Text,ImmutableBytesWritable>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
	
			double x_bar=0;
			String s="";
			int N=0;
			double total_dob=0;
			//double volatility=0;
			
			ArrayList<String> al=new ArrayList<String>();
			for(Text d:values)
				{
			
				al.add(d.toString());
				N++;
				x_bar+=Double.parseDouble(d.toString());
				}
			if(N!=0 && N!=1)
			{
			x_bar=(double)x_bar/N;
			for(String dob:al)
			{
			double d=(Double.parseDouble(dob)-x_bar)*(Double.parseDouble(dob)-x_bar);
			total_dob+=d;

			}
			//System.out.println("total_dob" +total_dob);
			
			total_dob=(double)total_dob/(N-1);
			double volatility=Math.sqrt(total_dob);
			//System.out.println(key.toString()+"meri vol" +volatility);
			if(volatility!=0)
			{
				byte[] sname=Bytes.toBytes(key.toString());
		        Put p=new Put(sname);
		        //System.out.println(sname+"hello"+volatility);
		        p.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), sname);
		        p.add(Bytes.toBytes("price"), Bytes.toBytes("volatility"),Bytes.toBytes(volatility));
				
				
				
				
				context.write(new ImmutableBytesWritable(sname) ,p);
			}
			}
		}
}

