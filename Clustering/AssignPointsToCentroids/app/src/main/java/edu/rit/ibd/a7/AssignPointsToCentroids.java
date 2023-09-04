package edu.rit.ibd.a7;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class AssignPointsToCentroids {
	public enum Distance {Manhattan, Euclidean};
	public enum Mean {Arithmetic, Geometric};

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoCol = args[2];
		final Distance distance = Distance.valueOf(args[3]);
		final Mean mean = Mean.valueOf(args[4]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> collection = db.getCollection(mongoCol);
		
		// TODO Your code here!
		int i=0;
		while(i<100){
			Document d =new Document();
			collection.insertOne(d);
		}


		
		/*
		 * 
		 * Points have _id=p_XYZ and centroids have _id=c_ABC. A new centroid derived from an existing centroid c_i must have _id=new_c_i.
		 * 
		 * To perform one epoch, each point should be assigned to the closest centroid using the input distance (Manhattan or Euclidean).
		 * 	That is, if point p_i has the minimum distance to centroid c_j, p_i.label = c_j.
		 * 
		 * Once the point assignment has been made, SSE is the sum of the square distance between point and centroid. Each centroid c_i
		 * 	will contain a field sse that will store the SSE of all the points assigned to it. Furthermore, you need to include a field
		 * 	with the total count of points assigned to the centroid (total_points), and whether must be reinitialized (reinitialize).
		 * 
		 * A centroid must be reinitialized at the end of the epoch if it has no points assigned to it.
		 * 
		 * Each new centroid is derived from a centroid that must not be reinitialized. A centroid that must be reinitialized has no 
		 * 	points assigned to it, so it is not possible to compute a new centroid. Assuming c_i is a centroid with points assigned, the 
		 * 	new centroid new_c_i is the (arithmetic or geometric) mean of all the points assigned to it. That is, for each dimension k,
		 * 	the dimension k of new_c_i is the mean of all the dimensions k of the points assigned to c_i.
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
