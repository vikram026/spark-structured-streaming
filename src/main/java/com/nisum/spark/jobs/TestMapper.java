package com.nisum.spark.jobs;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class TestMapper implements FlatMapFunction<String, String> {
	@Override
	public Iterator<String> call(String line) throws Exception {
		return flattenWord(line);

	}

	private Iterator<String> flattenWord(String line) {
		if(line.contains("error"))
			throw new RuntimeException("Bad word****************");
		return Arrays.asList(line.split(" ") ).iterator();
	}

	private Stream<String> cartesianProduct(String line,Supplier<Stream<String>> lookupEntties, Supplier<Stream<String>> stores) {
		return lookupEntties.get()
				.flatMap(le -> stores.get().map(s-> line+le+s))
				.peek(e -> System.out.println("Filtered value: " + e));
	}

//	private Stream<String> cartesianProduct(String line,Supplier<Stream<String>> lookupEntties, Supplier<Stream<String>> stores) {
//		return lookupEntties.get()
//				.flatMap(le -> stores.get().map(s-> line+le+s))
//				.peek(e -> System.out.println("Filtered value: " + e));
//	}
}