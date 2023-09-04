package edu.rit.ibd.a6;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mongodb.client.AggregateIterable;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class GenerateLK {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColTrans = args[2];
		final String mongoColCK = args[3];
		final String mongoColLK = args[4];
		final int minSup = Integer.valueOf(args[5]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> transactions = db.getCollection(mongoColTrans);
		MongoCollection<Document> ck = db.getCollection(mongoColCK);
		MongoCollection<Document> lk = db.getCollection(mongoColLK);


		List<Document> ckdoc = ck.find().into(new ArrayList<Document>());

		Document first = ckdoc.get(0);
		Document items  = (Document) first.get("items");
		int k = items.size();


		int j=1;
		Document d =new Document("itemid0", "$items.pos_0");
		while(j<k){
			d.append("itemid"+j, "$items.pos_"+j);
			j++;
		}
		j=0;
		List<Document> ll=new ArrayList<>();
		while(j<k){
			ll.add(new Document("$in", Arrays.asList("$$itemid"+j, "$items")));
			j++;
		}

		AggregateIterable<Document> ot = ck.aggregate(Arrays.asList(new Document("$lookup",
										   new Document("from", "Transaction2")
            .append("let",d)
			.append("pipeline", Arrays.asList(new Document("$match",
                new Document("$expr",
                new Document("$and", ll)))))
			.append("as", "lk")),
			new Document("$project",
								 new Document("count",
    new Document("$size", "$lk"))
			.append("items", 1)),
			new Document("$match",
								 new Document("count",
    new Document("$gte", minSup)))));


		//int j=0;
		for (Document querydoc : ot)
		{
				Document ddoc = new Document();

				Document dinside =  (Document) querydoc.get("items");
				//dinside.append("pos_0",querydoc.getInteger("_id"));
				ddoc.append("items",dinside);
				ddoc.append("count",querydoc.getInteger("count"));
				lk.insertOne(d);


		}

		
		// TODO Your code here!
		
		/*
		 * 
		 * For each transaction t, check whether the items of a document c in ck are contained in the items of t. If so, increment by one the count of c.
		 * 
		 * All the documents in ck that meet the minimum support will be copied to lk.
		 * 
		 * You can use $inc to update the count of a document.
		 * 
		 * Alternatively, you can also copy all documents in ck to lk first and, then, perform the previous computations.
		 * 
		 */
		
		// You must figure out the value of k.
		
		// For each document in Ck, check the items are present in the transactions at least minSup times.
		
		// You can implement this "by hand" using Java, an aggregation query, or a mix.
		
		// TODO End of your code here!
		
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
