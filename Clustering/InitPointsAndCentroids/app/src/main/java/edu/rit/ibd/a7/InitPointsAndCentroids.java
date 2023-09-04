package edu.rit.ibd.a7;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.types.Decimal128;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import javax.print.attribute.standard.DocumentName;

public class InitPointsAndCentroids {
	public enum Scaling {None, MinMax, Mean, ZScore}

	public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String sqlQuery = args[3];
		final String mongoDBURL = args[4];
		final String mongoDBName = args[5];
		final String mongoCol = args[6];
		final Scaling scaling = Scaling.valueOf(args[7]);
		final int k = Integer.valueOf(args[8]);
		
		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		MongoCollection<Document> collection = db.getCollection(mongoCol);
		
		// TODO Your code here!


		//MongoCollection<Document> transactions = db.getCollection(mongoCol);

		PreparedStatement st = con.prepareStatement(sqlQuery);

		//int batch=1000;
		//st.setFetchSize(batch);
		ResultSet rs = st.executeQuery();
		ResultSet rsm = rs;
//		ResultSet rs1= st.executeQuery();
//		ResultSet rs2 = st.executeQuery();
		//List<Integer> list = new ArrayList<>();
		//rs.next();
		ResultSetMetaData rsmd = rsm.getMetaData();
		//getting the column type
		int column_count = rsmd.getColumnCount();
		int noofdim = column_count-1;


		Document d = new Document();

		Decimal128[] min = new Decimal128[noofdim];
		Decimal128[] max = new Decimal128[noofdim];
		BigDecimal[] mean = new BigDecimal[noofdim];
		BigDecimal[] std = new BigDecimal[noofdim];
		int m=0;
		while(m<noofdim){
			min[m]=new Decimal128(100000);
			max[m]=new Decimal128(0);
			mean[m]=BigDecimal.ZERO;
			//std[m]=BigDecimal.ZERO;
			m++;
		}

		int c=0;
		while (rs.next()) {

			//Long id = rs.getLong("_id");
			c++;
			int dim = 0;
			while(dim<noofdim){
				Decimal128 l = readAttribute(rs,"dim_"+dim);
						//(Long) rs.getObject("dim_"+dim);
				if(min[dim].compareTo(l)>0){
					min[dim]=l;
				}
				if(max[dim].compareTo(l)<0){
					max[dim]=l;
				}
				mean[dim]=mean[dim].add(l.bigDecimalValue());
				dim++;
			}

		}
		int dim=0;


		while(dim<noofdim){
			mean[dim]=mean[dim].divide(BigDecimal.valueOf(c),MathContext.DECIMAL128);
			dim++;
		}

		//find std
		rs.beforeFirst();

		dim=0;
		while(dim<noofdim){
			BigDecimal sum = BigDecimal.ZERO;
			while(rs.next()){
				Decimal128 l = readAttribute(rs,"dim_"+dim);
				BigDecimal bd = l.bigDecimalValue().subtract(mean[dim]);
				BigDecimal bd2 = bd.pow(2,MathContext.DECIMAL128);
				BigDecimal xx  = sum.add(bd2);
				sum = xx;
				//dim++;
				//}
			}
			BigDecimal bd3 = sum.divide(BigDecimal.valueOf(c),MathContext.DECIMAL128);
			std[dim] = bd3.sqrt(MathContext.DECIMAL128);
			dim++;
		}


		dim=0;

		d.append("_id","limits");
		while(dim<noofdim){
			Document d1 = new Document();
			d1.append("min",min[dim]);
			d1.append("max",max[dim]);
			d1.append("mean",mean[dim]);
			d1.append("std",std[dim]);
			d.append("dim_"+dim,d1);
			dim++;
		}
		collection.insertOne(d);


		//scale
		Document dd=new Document();
		rs.beforeFirst();
		while(rs.next()){
			dd.append("_id", "p_" + rs.getLong("id"));

			dim = 0;
			Document dinside = new Document();
			while(dim<noofdim){

				Decimal128 l = readAttribute(rs,"dim_"+dim);
				if(scaling.equals(Scaling.None)){
					dinside.append("dim_"+dim,l);
				}
				if(scaling.equals(Scaling.MinMax)){
					BigDecimal bgn = l.bigDecimalValue().subtract(min[dim].bigDecimalValue());
					BigDecimal bgd = max[dim].bigDecimalValue().subtract(min[dim].bigDecimalValue());
					BigDecimal mm = bgn.divide(bgd,MathContext.DECIMAL128);
					dinside.append("dim_"+dim,mm);
				}
				if(scaling.equals(Scaling.Mean)){
					BigDecimal bgn = l.bigDecimalValue().subtract(mean[dim]);
					BigDecimal bgd = max[dim].bigDecimalValue().subtract(min[dim].bigDecimalValue());
					BigDecimal men = bgn.divide(bgd,MathContext.DECIMAL128);
					dinside.append("dim_"+dim, men);
				}
				if(scaling.equals(Scaling.ZScore)){
					BigDecimal bgn = l.bigDecimalValue().subtract(mean[dim]);
					BigDecimal sd = bgn.divide(std[dim],MathContext.DECIMAL128);
					dinside.append("dim_"+dim, sd);
				}

				dim++;
			}
			dd.append("point",dinside);
			collection.insertOne(dd);
		}


		rs.beforeFirst();
		int j=0;
		while(j<k){
			Document dc = new Document();
			dc.append("_id","c_"+j);
			Document dcin = new Document();
			dc.append("centroid",dcin);
			collection.insertOne(dc);
			j++;
		}
		rs.close();
		st.close();
		//con.close();

		/*
		 * 
		 * The SQL query has a column named id with the id of each point (always long), and a number of columns dim_i that form the point with n dimensions. 
		 * 	You should store the value of each dimension as a Decimal128. In order to do so, use the readAttribute method provided.
		 * 
		 * All your computations must use BigDecimal/Decimal128. Note that x.add(y), where both x and y are BigDecimal, will not update x, so you need to 
		 * 	assign it to a BigDecimal, i.e., z = x.add(y). When dividing, use MathContext.DECIMAL128 to keep the desired precision. If you implement your 
		 * 	calculations using MongoDB, do not use {$divide : [x, y]}; instead, you must do: {$multiply : [x, {$pow: [y, -1]}]}.
		 * 
		 * Each point must be of the form: {_id: p_123, point: {dim_0:_, dim_1:_, ...}}; each centroid: {_id: c_7, centroid: {}}.
		 * 
		 * Compute stat values per dimension and store them in a document whose id is 'limits'. For each dimension i, dim_i:{min:_, max:_, mean:_, std:_}.
		 * 	Note that you can use Java or MongoDB to compute these. There is a stdDevPop in MongoDB to compute standard deviation; unfortunately, it does
		 * 	not return Decimal128, so you need to find an alternate way.
		 * 
		 * Using the limits, you must scale the value using MinMax, Mean or ZScore according to the input. This only applies to the points.
		 * 
		 */
		
			
		
		// TODO End of your code!
		
		client.close();
		con.close();
	}
	
	private static Decimal128 readAttribute(ResultSet rs, String label) throws SQLException {
		// From: https://stackoverflow.com/questions/9482889/set-specific-precision-of-a-bigdecimal
		BigDecimal x = rs.getBigDecimal(label);
		x = x.setScale(x.scale() + MathContext.DECIMAL128.getPrecision() - x.precision(), MathContext.DECIMAL128.getRoundingMode());
		return new Decimal128(x);
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
