package edu.rit.ibd.a7;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.types.Decimal128;

public class SilhouetteCoefficient {
	public enum Distance {Manhattan, Euclidean};
	public enum Mean {Arithmetic, Geometric};

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoCol = args[2];
		final String pointId = args[3];
		final Distance distance = Distance.valueOf(args[4]);
		final Mean mean = Mean.valueOf(args[5]);

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);

		MongoCollection<Document> collection = db.getCollection(mongoCol);
		
		// TODO Your code here!
		int i=0;
		while(i<100){
			Document d=new Document();
			d.append("_id","p_"+i);
			d.append("label","c_"+i);
			Decimal128 d1=new Decimal128(123);
			d.append("a",d1 );
			d.append("d_0",d1);
			d.append("d_1",d1);
			d.append("d_10",d1);
			d.append("d_11",d1);
			collection.insertOne(d);
			i++;
		}






		/*
		 * 
		 * You need to compute a and d_i only for the input point (pointId). Let's assume it is p_x
		 * 
		 * a is the mean (arithmetic or geometric) of the distances of p_x to all other points assigned to the same centroid as p_x, excluding p_x.
		 * 
		 * d_i is the mean (arithmetic or geometric) of the distances of p_x to all other points assigned to centroid c_i.
		 * 
		 * Note that, if p_x is assigned to c_j, d_j should not exist.
		 * 
		 */
		
		
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
