import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


	
public class Mapper1 extends TableMapper<Text, Text>
{
		
	private Text outputkey=new Text();
	private Text outputval=new Text();
			
	public void map(ImmutableBytesWritable key, Result value, Context context)
	{
		String sname=new String(value.getValue(Bytes.toBytes("stock"),Bytes.toBytes("name"))); 
		String year=new String(value.getValue(Bytes.toBytes("time"),Bytes.toBytes("yr"))); 
		String month=new String(value.getValue(Bytes.toBytes("time"),Bytes.toBytes("mm"))); 
		String day=new String(value.getValue(Bytes.toBytes("time"),Bytes.toBytes("dd"))); 
		String adj_close=new String(value.getValue(Bytes.toBytes("price"),Bytes.toBytes("price"))); 
		outputkey.set(sname+"%"+year+","+month);
		outputval.set(day+"%"+adj_close);
		try 
		{
			context.write(outputkey,outputval);
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (InterruptedException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
		
		

	
