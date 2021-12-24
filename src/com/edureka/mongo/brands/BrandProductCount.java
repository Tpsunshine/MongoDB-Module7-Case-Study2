package com.edureka.mongo.brands;

public class BrandProductCount {
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		
		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "monngodb://localhost/Products_DB_index.product");
		MongoConfigUtil.setOutputURI(conf, "mongodb://localhost/Products_DB_index.brand_product_count");
		System.out.println("Conf: " + conf);
		
		final Job job = new Job(conf, "Brand wise product count");
		
		job.setJarByClass(BrandProductCount.class);
		
		job.setMapperClass(BrandMapper.class);
		
		job.setCombinerClass(BrandProductReducer.class);
		job.setReducerClass(BrandProductReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputValueClass(MongoOutputFormat.class);
		
		System.out.println(job.waitForCompletion(true) ? 0 : 1);

	}

}
