package com.dexels.kafka.webapi;

import io.reactivex.functions.Predicate;

import java.io.IOException;
import java.util.List;

public class TopicDelete {
	public static void deleteTopics(String bootstrap, String path, String generation, String tenant, String deployment) throws IOException {
	}
	

	public static Predicate<String> isRelevant(String tenant, String deployment, List<String> generationList) {
		return in->{
			for (String generation : generationList) {
				String snip = tenant+"-"+deployment+"-generation-"+generation+"-";
				boolean valid = in.indexOf(snip)!=-1;
				if(valid) {
					return true;
				}
			}
			return false;
		};
	}
}
