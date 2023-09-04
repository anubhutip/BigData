package edu.rit.ibd.a7;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.types.Decimal128;

public class MutualInformation {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoCol = args[2];
		final int R = Integer.valueOf(args[3]);
		final int C = Integer.valueOf(args[4]);

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);

		MongoCollection<Document> collection = db.getCollection(mongoCol);
//		int i=0;
//		int[] arr=new int[5];
//		arr[0]=1;
//		arr[1]=2;
//		arr[2]=3;
//		arr[3]=4;
//		arr[4]=5;
			Document d=new Document();
			d.append("_id", "mi_info");
			d.append("a",34);

			d.append("b",26);
			d.append("c",32);
			Decimal128 df=new Decimal128(1234);
		d.append("hu",df);
			d.append("n",df);
		d.append("emi",df);
			d.append("hv",df);
			d.append("mi",df);



			collection.insertOne(d);

		// TODO Your code here!

		/*
		 * 
		 * Every point will have two labels: label_u is an integer indicating the cluster of the point in assignment U; similarly, label_v is the
		 * 	cluster (integer) of the point in assignment V. |U|=R and |V|=C.
		 * 
		 * You need to compute fields a, b and c. Field a is an array of size R in which each position a.i indicates the total number of points 
		 * 	assigned to cluster i in U. Similarly, b is an array of size C; each position b.j is the total number of points assigned to cluster j
		 * 	in V. Field c is an array storing the contingency matrix. In the contingency matrix, [i, j] is the number of points that are assigned
		 * 	to both i in U and j in V. Since MongoDB does not allow us to store matrices, we are going to store the matrix as a single array. 
		 * 	Note that the contingency matrix has R rows and C columns. Cell [i, j] is position p=i*C+j in the array c. Furthermore, position p in
		 * 	c corresponds to the following cell: [(p - p%C)/C, p%C].
		 * 
		 * Using the previous fields, you need to compute H(U), H(V), MI and E(MI) as defined in the slides. You must store them in the following
		 * 	fields: hu, hv, mi and emi, respectively.
		 * 
		 * To compute the factorial part of E(MI), you should use log factorial. The following library is a good resource to work with BigDecimal
		 * 	in Java: https://github.com/eobermuhlner/big-math
		 * 
		 * All these fields must be stored in a single document with _id=mi_info.
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
