package com.edureka.mongo.brands;

public class BrandProductReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private final IntWritable result = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
					throws IOException, InterruptedExcepetion {
		
		int sum = 0;
		for (final IntWritable val : values) {
			sum += val.get();
		}
		
		result.set(sum);
		context.write(key, result);
	}

}
