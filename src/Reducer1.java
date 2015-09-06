import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public  class Reducer1 extends TableReducer<Text, Text,ImmutableBytesWritable>
{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		int min=32;
		int max=0;
		double min_ad=0;
		double max_ad=0;
		int count=0;
		double d;
			
		for(Text t:values)
		{
			String s=t.toString();
			String temp[]=s.split("\\%");
			if(min>Integer.parseInt(temp[0]))
			{
				min=Integer.parseInt(temp[0]);
				min_ad=Double.parseDouble(temp[1]);
			}
			
			if(max<Integer.parseInt(temp[0]))
			{
				max=Integer.parseInt(temp[0]);
				max_ad=Double.parseDouble(temp[1]);
			}
		}		
		Put p=new Put(Bytes.toBytes(key.toString()));
		String[] tempkey=key.toString().split("\\%");
		System.out.println(tempkey[0]+"abcde"+max_ad+" "+min_ad);
		byte[] sname=Bytes.toBytes(tempkey[0]);
		String[] temp=tempkey[1].split(",");
		byte[] year=Bytes.toBytes(temp[0]);
		byte[] month=Bytes.toBytes(temp[1]);
		double xi=(max_ad-min_ad)/min_ad;
		System.out.println(tempkey[0]+"abcd"+xi);
		p.add(Bytes.toBytes("stock"),Bytes.toBytes("name") , sname);
		p.add(Bytes.toBytes("time"),Bytes.toBytes("yr"),year);
		p.add(Bytes.toBytes("time"),Bytes.toBytes("mm"),month);
		p.add(Bytes.toBytes("price"),Bytes.toBytes("xi"),Bytes.toBytes(xi));
			
		context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())) ,p);
			
	}
}