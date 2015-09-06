
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.Text;


public class Main
{
	public static void main(String[] args)
	{
		Configuration conf = HBaseConfiguration.create();
		try 
		{
			long start = new Date().getTime();
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("raw"));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("time"));
			tableDescriptor.addFamily(new HColumnDescriptor("price"));
			if ( admin.isTableAvailable("raw"))
			{
				admin.disableTable("raw");
				admin.deleteTable("raw");
			}
			admin.createTable(tableDescriptor);


			Job job = Job.getInstance();
			job.setJarByClass(Main.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(Job1.Map.class);
			TableMapReduceUtil.initTableReducerJob("raw", null, job);
			job.setNumReduceTasks(0);
			job.waitForCompletion(true);
			
			HTableDescriptor tableDescriptor1 = new HTableDescriptor(TableName.valueOf("t1"));
			tableDescriptor1.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor1.addFamily(new HColumnDescriptor("time"));
			tableDescriptor1.addFamily(new HColumnDescriptor("price"));
			if ( admin.isTableAvailable("t1"))
			{
				admin.disableTable("t1");
				admin.deleteTable("t1");
			}
			admin.createTable(tableDescriptor1);

			Job job2 = Job.getInstance();
			job2.setJarByClass(Main.class);
			Scan s2 = new Scan();
			s2.setCaching(500);        
			s2.setCacheBlocks(false);  
			TableMapReduceUtil.initTableMapperJob("raw",s2,Mapper1.class, Text.class,Text.class, job2);
			TableMapReduceUtil.initTableReducerJob("t1",Reducer1.class,job2);
			job2.waitForCompletion(true);
			boolean status1 = job2.waitForCompletion(true);
			if (!status1) 
			{
				throw new IOException("error");
			}
			
			HTableDescriptor tableDescriptor2 = new HTableDescriptor(TableName.valueOf("t2"));
			tableDescriptor2.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor2.addFamily(new HColumnDescriptor("price"));
			if ( admin.isTableAvailable("t2"))
			{
				admin.disableTable("t2");
				admin.deleteTable("t2");
			}
			admin.createTable(tableDescriptor2);

			Job job3 = Job.getInstance();
			job3.setJarByClass(Main.class);
			Scan s3 = new Scan();
			s3.setCaching(500);        
			s3.setCacheBlocks(false);  
			TableMapReduceUtil.initTableMapperJob("t1",s3,Mapper2.class, Text.class,Text.class, job3);
			TableMapReduceUtil.initTableReducerJob("t2",Reducer2.class,job3);
			job3.waitForCompletion(true);
			boolean status2 = job3.waitForCompletion(true);
			if (status2==false) 
			{
				throw new IOException("error");
			}
			
			HTableDescriptor tableDescriptor3 = new HTableDescriptor(TableName.valueOf("t3"));
			tableDescriptor3.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor3.addFamily(new HColumnDescriptor("price"));
			if ( admin.isTableAvailable("t3"))
			{
				admin.disableTable("t3");
				admin.deleteTable("t3");
			}
			admin.createTable(tableDescriptor3);


			Job job4 = Job.getInstance();
			job4.setJarByClass(Main.class);
			Scan s4 = new Scan();
			s3.setCaching(500);        
			s3.setCacheBlocks(false);  
			TableMapReduceUtil.initTableMapperJob("t2",s3,Mapper3.class, Text.class,Text.class, job4);
			TableMapReduceUtil.initTableReducerJob("t3",Reducer3.class,job4);
			job4.waitForCompletion(true);
			boolean status3 = job4.waitForCompletion(true);
			if (status3==false) 
			{
				throw new IOException("error");
			}
			if (status3 == true) 
			{
				long end = new Date().getTime();
				System.out.println("\nJob took " + (double)(end-start)/60000 + " minutes\n");
			}
			admin.close();
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
}

