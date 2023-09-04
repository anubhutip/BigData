package edu.rit.ibd.a4;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class IMDBSQLToMongo {

	public static void main(String[] args) throws Exception {
		final String dbURL = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String mongoDBURL = args[3];
		final String mongoDBName = args[4];
		
		System.out.println(new Date() + " -- Started");
		
		Connection con = DriverManager.getConnection(dbURL, user, pwd);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);


		MongoCollection<Document> col1 = db.getCollection("Movies");
		MongoCollection<Document> col2 = db.getCollection("MoviesDenorm");
		MongoCollection<Document> col3 = db.getCollection("People");
		MongoCollection<Document> col4 = db.getCollection("PeopleDenorm");

		//Movies and denormmovies table
		String query_for_movie = "select m.*,group_concat(name separator ', ') as genres from movie as m left join moviegenre as mg on m.id = mg.mid left join genre as g on mg.gid=g.id group by m.id";
		String query_for_actors = "select id,group_concat(pid separator ', ') as actors from movie as m left join actor as ac on m.id=ac.mid group by id";
		String query_for_directors = "select id,group_concat(pid separator ', ') as directors from movie as m left join director as d on m.id=d.mid group by id";
		String query_for_producers = "select id,group_concat(pid separator ', ') as producers from movie as m left join producer as p on m.id=p.mid group by id";
		String query_for_writers = "select id,group_concat(pid separator ', ') as writers from movie as m left join writer as w on m.id=w.mid group by id";
		PreparedStatement st = con.prepareStatement(query_for_movie);

		int batch=1000;
		st.setFetchSize(batch);//( Batch size : 1);
		ResultSet rs = st.executeQuery();
		int c= 0;
		List<Document> arrofdocs = new ArrayList<>();
		List<Document> arrofdocs2 = new ArrayList<>();
		while (rs.next()) {
			Document d = new Document();
			Document d2 = new Document();
			int id = rs.getInt("id");
			d.append("_id",id);
			d2.append("_id", id);
			d.append("ptitle",rs.getString("ptitle"));
			d.append("otitle",rs.getString("otitle"));
			//int adult = rs.getInt("adult");

			d.append("adult",rs.getBoolean("adult"));
			int yr = rs.getInt("year");
			if (!rs.wasNull()) {
				d.append("year",yr);
			}
			int rt = rs.getInt("runtime");
			if(!rs.wasNull()){
				d.append("runtime",rt);
			}
			BigDecimal rate = rs.getBigDecimal("rating");
			if(!rs.wasNull()){
				Decimal128 x = new Decimal128(rate);
				d.append("rating",x);
			}
			int tv = rs.getInt("totalvotes");
			if(!rs.wasNull()){
				d.append("totalvotes",tv);
			}
			String gen = rs.getString("genres");
			if(!rs.wasNull()){
				String[] arrofgenres = gen.split(", ");
				List<String> list = Arrays.asList(arrofgenres);
				d.append("genres",list);
			}
			arrofdocs.add(d);
			arrofdocs2.add(d2);
			c++;
			if( c%batch == 0 ){
				col1.insertMany(arrofdocs);
				col2.insertMany(arrofdocs2);
				arrofdocs.clear();
				arrofdocs2.clear();
			}
		}
		col1.insertMany(arrofdocs);
		col2.insertMany(arrofdocs2);
		rs.close();
		st.close();
		arrofdocs.clear();
		arrofdocs2.clear();


		PreparedStatement sta = con.prepareStatement(query_for_actors);
		batch=1000;
		sta.setFetchSize(batch);
		ResultSet rs1 = sta.executeQuery();
		while (rs1.next()) {
			String strofact = rs1.getString("actors");
			if(!rs1.wasNull()){
				String[] arrofactors = strofact.split(", ");
				List<String> listofact = Arrays.asList(arrofactors);
				List<Integer> intList = new ArrayList<>();
				for(String s : listofact) intList.add(Integer.valueOf(s));
				col2.updateOne(Document.parse("{ _id : "+rs1.getInt("id")+" }"), Document.parse("{$push:{\"actors\":{$each:"+intList+"}}}"));
			}

		}
		rs1.close();
		sta.close();


		PreparedStatement std = con.prepareStatement(query_for_directors);
		batch=1000;
		std.setFetchSize(batch);
		ResultSet rs2 = std.executeQuery();
		while (rs2.next()) {
			String strofdir = rs2.getString("directors");
			if(!rs2.wasNull()){
				String[] arrofdirectors = strofdir.split(", ");
				List<String> listofdir = Arrays.asList(arrofdirectors);
				List<Integer> intListd = new ArrayList<>();
				for(String s : listofdir) intListd.add(Integer.valueOf(s));
				col2.updateOne(Document.parse("{ _id : "+rs2.getInt("id")+" }"), Document.parse("{$push:{\"directors\":{$each:"+intListd+"}}}"));
			}

		}
		rs2.close();
		std.close();

		PreparedStatement stp = con.prepareStatement(query_for_producers);
		batch=1000;
		stp.setFetchSize(batch);
		ResultSet rs3 = stp.executeQuery();
		while (rs3.next()) {
			String strofpro = rs3.getString("producers");
			if(!rs3.wasNull()){
				String[] arrofproducers = strofpro.split(", ");
				List<String> listofpro = Arrays.asList(arrofproducers);
				List<Integer> intListp = new ArrayList<>();
				for(String s : listofpro) intListp.add(Integer.valueOf(s));
				col2.updateOne(Document.parse("{ _id : "+rs3.getInt("id")+" }"), Document.parse("{$push:{\"producers\":{$each:"+intListp+"}}}"));
			}

		}
		rs3.close();
		stp.close();

		PreparedStatement stw = con.prepareStatement(query_for_writers);
		batch=1000;
		stw.setFetchSize(batch);
		ResultSet rs4 = stw.executeQuery();
		while (rs4.next()) {
			String strofwriters = rs4.getString("writers");
			if(!rs4.wasNull()){
				String[] arrofwriters = strofwriters.split(", ");
				List<String> listofwr = Arrays.asList(arrofwriters);
				List<Integer> intListw = new ArrayList<>();
				for(String s : listofwr) intListw.add(Integer.valueOf(s));
				col2.updateOne(Document.parse("{ _id : "+rs4.getInt("id")+" }"), Document.parse("{$push:{\"writers\":{$each:"+intListw+"}}}"));
			}

		}
		rs4.close();
		stw.close();

		//people and peopledenorm table

		c=0;


		String query_for_people = "select * from person";
		PreparedStatement stpep = con.prepareStatement(query_for_people);
		batch=5000;
		stpep.setFetchSize(batch);//( Batch size  1);
		ResultSet rs_pep = stpep.executeQuery();
		while (rs_pep.next()) {
			Document d3 = new Document();
			Document d4 = new Document();
			//d.append("_id", 0);
			int id = rs_pep.getInt("id");
			d3.append("_id",id);
			d4.append("_id", id);
			d3.append("name",rs_pep.getString("name"));
			int byr = rs_pep.getInt("byear");
			if (!rs_pep.wasNull()) {
				d3.append("byear",byr);
			}
			int dyr = rs_pep.getInt("dyear");
			if(!rs_pep.wasNull()){
				d3.append("dyear",dyr);
			}
			arrofdocs.add(d3);
			arrofdocs2.add(d4);
			c++;
			if(c%batch==0){
				col3.insertMany(arrofdocs);
				col4.insertMany(arrofdocs2);
				arrofdocs.clear();
				arrofdocs2.clear();
			}

		}
		col3.insertMany(arrofdocs);
		col4.insertMany(arrofdocs2);
		arrofdocs.clear();
		arrofdocs2.clear();
		rs_pep.close();
		stpep.close();


		String query_for_acted = "select id,group_concat(mid separator ', ') as acted from person as p left join actor as ac on p.id=ac.pid group by id";
		String query_for_directed = "select id,group_concat(mid separator ', ') as directed  from person as p left join director as d on p.id=d.pid group by id";
		String query_for_knownfor = "select id,group_concat(mid separator ', ') as kf from person as p left join knownfor as k on p.id=k.pid group by id";
		String query_for_produced = "select id,group_concat(mid separator ', ') as produced from person as p left join producer as pr on p.id=pr.pid group by id";
		String query_for_written = "select id,group_concat(mid separator ', ') as written from person as p left join writer as w on p.id=w.pid group by id";
		PreparedStatement sta2 = con.prepareStatement(query_for_acted);
		batch=5000;
		sta2.setFetchSize(batch);
		ResultSet rs5 = sta2.executeQuery();
		while (rs5.next()) {
			String strofact = rs5.getString("acted");

			if(!rs5.wasNull()){
				String[] arrofacts = strofact.split(", ");
				List<String> listofa = Arrays.asList(arrofacts);
				List<Integer> intLista2 = new ArrayList<>();
				for(String s : listofa) intLista2.add(Integer.valueOf(s.replace(",","")));
				col4.updateOne(Document.parse("{ _id : "+rs5.getInt("id")+" }"), Document.parse("{$push:{\"acted\":{$each:"+intLista2+"}}}"));
			}

		}
		rs5.close();
		sta2.close();

		PreparedStatement std2 = con.prepareStatement(query_for_directed);
		batch=5000;
		std2.setFetchSize(batch);
		ResultSet rs6 = std2.executeQuery();
		while (rs6.next()) {
			String strofdir = rs6.getString("directed");
			if(!rs6.wasNull()){
				String[] arrofdirectors = strofdir.split(", ");
				List<String> listofdir = Arrays.asList(arrofdirectors);
				List<Integer> intListd = new ArrayList<>();
				for(String s : listofdir) intListd.add(Integer.valueOf(s.replace(",","")));
				col4.updateOne(Document.parse("{ _id : "+rs6.getInt("id")+" }"), Document.parse("{$push:{\"directed\":{$each:"+intListd+"}}}"));
			}

		}
		rs6.close();
		std2.close();

		PreparedStatement stf = con.prepareStatement(query_for_knownfor);
		batch=5000;
		stf.setFetchSize(batch);
		ResultSet rs7 = stf.executeQuery();
		while (rs7.next()) {
			String strofkf = rs7.getString("kf");
			if(!rs7.wasNull()){
				String[] arrofkf = strofkf.split(", ");
				List<String> listofkf = Arrays.asList(arrofkf);
				List<Integer> intListkf = new ArrayList<>();
				for(String s : listofkf) intListkf.add(Integer.valueOf(s.replace(",","")));
				col4.updateOne(Document.parse("{ _id : "+rs7.getInt("id")+" }"), Document.parse("{$push:{\"knownfor\":{$each:"+intListkf+"}}}"));
			}

		}
		rs7.close();
		stf.close();

		PreparedStatement stp2 = con.prepareStatement(query_for_produced);
		batch=5000;
		stp2.setFetchSize(batch);
		ResultSet rs8 = stp2.executeQuery();
		while (rs8.next()) {
			String strofpro = rs8.getString("produced");
			if(!rs8.wasNull()){
				String[] arrofproducers = strofpro.split(", ");
				List<String> listofpro = Arrays.asList(arrofproducers);
				List<Integer> intListp = new ArrayList<>();
				for(String s : listofpro) intListp.add(Integer.valueOf(s.replace(",","")));
				col4.updateOne(Document.parse("{ _id : "+rs8.getInt("id")+" }"), Document.parse("{$push:{\"produced\":{$each:"+intListp+"}}}"));
			}

		}
		rs8.close();
		stp2.close();

		PreparedStatement stw2 = con.prepareStatement(query_for_written);
		batch=5000;
		stw2.setFetchSize(batch);
		ResultSet rs9 = stw2.executeQuery();
		while (rs9.next()) {
			String strofwriters = rs9.getString("written");
			if(!rs9.wasNull()){
				String[] arrofwriters = strofwriters.split(", ");
				List<String> listofwr = Arrays.asList(arrofwriters);
				List<Integer> intListw = new ArrayList<>();
				for(String s : listofwr) intListw.add(Integer.valueOf(s.replace(",","")));
				col4.updateOne(Document.parse("{ _id : "+rs9.getInt("id")+" }"), Document.parse("{$push:{\"written\":{$each:"+intListw+"}}}"));
			}

		}
		rs9.close();
		stw2.close();


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
