package com.edureka.mongo.brands;

import org.w3c.dom.Text;

public class BrandMapper extends Mapper<Object, BSONObject, Text, IntWritable> {
	
	private static final Log log = LogFactory.getLog(BrandMapper.class);
	
	private final static IntWritable one = new IntWritable(1);
	private final Text word = new Text();
	
	public void map(Object key, BSONObject value, Context context) throws IOExceptio, InterruptedException {
		
		log.info("key: "+ key);
		log.info("value: " + value);
		
		word.setData(value.get("brand").toString());
		context.write(word, one);
	}

}
