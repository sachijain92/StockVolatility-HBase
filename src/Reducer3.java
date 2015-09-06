import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Reducer3 extends TableReducer<Text,Text, ImmutableBytesWritable>  
{
	 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	 {
		int count=0;
		TreeSet<String> tm=new TreeSet<String>();
		for(Text t:values)
		{
			String s=t.toString();
			tm.add(t.toString());
		}
		int size = tm.size();
		Iterator<String> iterator = tm.iterator();
		while (iterator.hasNext()) 
		{
			if(count<10 || count>=(size-10))
			{
				String s=iterator.next();
				String temp[]=s.split(",");
				double d=Double.parseDouble(temp[0]);
				if(count<10)
				{
					String s1= temp[1]+ "--> "+temp[0];
					byte[] sname=Bytes.toBytes(s1.toString());
			        Put p=new Put(sname);
			        p.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), sname);
			        context.write(new ImmutableBytesWritable(sname) ,p);
				}
			        
				if(count>size-11)
				{
					String s1= temp[1]+ "--> "+temp[0];
					byte[] sname=Bytes.toBytes(s1.toString());
				    Put p=new Put(sname);
				    p.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), sname);
				    context.write(new ImmutableBytesWritable(sname) ,p);
				}
						
				count++;
			}
			else
			{
				iterator.next();
				count++;
			}
					
		}
	}
}