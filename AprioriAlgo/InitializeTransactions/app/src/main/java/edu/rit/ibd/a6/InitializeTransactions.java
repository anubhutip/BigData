package edu.rit.ibd.a6;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class InitializeTransactions {

	public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String sqlQuery = args[3];
		final String mongoDBURL = args[4];
		final String mongoDBName = args[5];
		final String mongoCol = args[6];
		
		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> transactions = db.getCollection(mongoCol);

		PreparedStatement st = con.prepareStatement(sqlQuery);

		int batch=1000;
		st.setFetchSize(batch);
		ResultSet rs = st.executeQuery();
		List<Integer> list = new ArrayList<>();
		rs.next();
		int tid = rs.getInt("tid");

		int iid = rs.getInt("iid");
		list.add(iid);
		Document d = new Document();
		while (rs.next()) {
			int idnew = rs.getInt("tid");
			iid = rs.getInt("iid");
			// When id changes
			if( idnew!=tid ){
				d = new Document();
				d.append("_id",tid);
				d.append("items",list);
				transactions.insertOne(d);
				list.clear();
			}
			list.add(iid);
			tid=idnew;
		}

		d = new Document();
		d.append("_id",tid);
		d.append("items",list);
		transactions.insertOne(d);
		rs.close();
		st.close();
		list.clear();
		
		// TODO Your code here!!!
		
		/*
		 * 
		 * Run the input SQL query over the input URL. Remember to use the fetch size to only retrieve a certain number of tuples at a time (useCursorFetch=true will
		 * 	be part of the URL).
		 * 
		 * For each transaction (tid), you need to create a new document and store it in the MongoDB collection specified as input. Such document must contain an array
		 * 	in which the elements are iid lexicographically sorted.
		 * 
		 */
		
		// Run the SQL query to retrieve the data. Recall that it contains two attributes iid and tid, and it is always sorted by tid and, then, iid.
		//	Be mindful of main memory and use an appropriate batch size.
		
		// TODO End of your code!
		
		client.close();
		con.close();
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
