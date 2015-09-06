import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Mapper3 extends TableMapper<Text, Text>
{
	private Text outputkey=new Text();
	private Text outputval=new Text();
			
	public void map(ImmutableBytesWritable key, Result value, Context context)
	{
		String sname = new String(value.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name")));
	    String price = Double.toString(Bytes.toDouble( value.getValue( Bytes.toBytes("price"), Bytes.toBytes("volatility") ) ) );
	    double d= Double.parseDouble(price);
	    DecimalFormat df = new DecimalFormat("##.#####");
	  	String s=df.format(d);
	    outputkey.set("mystocks");    
	    outputval.set(s+","+sname);
	    try 
	    {
			context.write(outputkey, outputval);
		} 
	    catch (Exception  e) 
	    {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
			
