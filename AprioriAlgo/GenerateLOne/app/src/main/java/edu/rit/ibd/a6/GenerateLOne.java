package edu.rit.ibd.a6;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Sorts;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.Arrays;

public class GenerateLOne  {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColTrans = args[2];
		final String mongoColL1 = args[3];
		final int minSup = Integer.valueOf(args[4]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> transactions = db.getCollection(mongoColTrans);
		MongoCollection<Document> l1 = db.getCollection(mongoColL1);


		AggregateIterable<Document> output = transactions.aggregate(Arrays.asList(
				Aggregates.unwind("$items"),
				Aggregates.group("$items", Accumulators.sum("count", 1)),
				Aggregates.sort(Sorts.ascending("_id"))
		));
		for (Document querydoc : output)
		{
			if(querydoc.getInteger("count")>=minSup){
				Document d = new Document();

				Document dinside = new Document();
				dinside.append("pos_0",querydoc.getInteger("_id"));
				d.append("items",dinside);
				d.append("count",querydoc.getInteger("count"));
				l1.insertOne(d);
			}

		}








		
		// TODO Your code here!
		
		/*
		 * 
		 * Extract single items from the transactions. Only single items that are present in at least minSup transactions should survive.
		 * 
		 * You need to compose the new documents to be inserted in the L1 collection as {_id: {pos_0:iid}, count:z}.
		 * 
		 */
		
		// You can implement this "by hand" using Java, an aggregation query, or a mix.
		// Be mindful of main memory and use batchSize when you request documents from MongoDB.
		
		// TODO End of your code!
		
		client.close();
	}
	
	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}

}
