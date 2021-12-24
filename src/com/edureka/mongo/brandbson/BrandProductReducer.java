package com.edureka.mongo.brandbson;

import java.io.IOException;

public class BrandProductReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
					throws IOException, InterruptedException {
		context.write(key, new IntWritable(1));
	}

}
