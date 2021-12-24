package com.edureka.mongo.brandbson;

import javax.security.auth.login.Configuration;

public class BrandProductCountJob {

	public static void main(String[] args)
	throws IOException, ClassNotFoundException, InterruptedException, SplitFailedException{
		final Configuration conf = new Configuration();
		conf.setConfiguration("mapred.input.dir","file///Users/ThaboP/Documents/Mongo/Products_Turtorial_DB_index/product.bson");
		//conf.SetBoolean("mongo.input.split.create_input_splits", false);
		MongoConfigUtil.setInputFormat(conf, BSONFileInputFormat.class);
		MongoConfigUtil.setCreateINoutSplits(conf, false);
		MongoConfUtil.setBSONReadSplits(conf, false);
		MongoConfigUtil.setBSONpathFilter(conf, BSONPathFilter.class);
		MongoConfigUtil.setOutputURI(conf, BSONPathFilter.class);
		MongoConfigUtil.setOutput(conf, "mongo://localhost/Products_Turtorial_DB_index.test");
		
		MongoConfigUtil.setOutputKey();
		MongoConfigUtil.setOutputValue(conf, IntWritable.class);
		
		@SuppressWarnings("deprecation")
		final Job job = new Job(conf, "submit counter");
		job.setJarByClass(BrandProductCOuntJob.class);
		job.setMapperClass(BrandMapper.class);
		job.setReducerClass(BrandProductReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(MongoOutputFormat.class);
		job.setInputFormatClass(MongoConfigUtil.getInputFormat(conf));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		

	}

}
