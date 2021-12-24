package com.edureka.mongo.brandbson;

import org.w3c.dom.Text;

public class BrandMapper extends Mapper<Object, BSONObect, Text, IntWritable> {
	
	private static final Log log = LogFactory.getLog(BrandMapper.class);
	
	private final static IntWritable one = new IntWritable(1);
	private final Text word = new Text();
	
	public void map(Object Key, BSONObject value, Context context) throws IOException, InterruptedException {
		
		log.info("key: " + key);
		log.info("value: " value);
		
		word.set(value.get("Brand").toString());
		context.write(word, one);
	}

}
