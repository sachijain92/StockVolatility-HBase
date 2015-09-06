import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class Mapper2 extends TableMapper<Text, Text>
{
	private Text outputkey=new Text();
	private Text outputval=new Text();
	 
	public void map(ImmutableBytesWritable key, Result value, Context context)
	{	
		String sname=new String(value.getValue(Bytes.toBytes("stock"),Bytes.toBytes("name"))); 
		String adj_close=Double.toString(Bytes.toDouble(value.getValue( Bytes.toBytes("price"), Bytes.toBytes("xi") ) ) );
		String[] spit = sname.split("\\%");
	    outputkey.set(spit[0]);
	    outputval.set(adj_close);
		try 
		{
			context.write(outputkey,outputval);
		} catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 }
}