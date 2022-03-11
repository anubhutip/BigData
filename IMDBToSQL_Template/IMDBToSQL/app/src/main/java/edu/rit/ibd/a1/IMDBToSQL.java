package edu.rit.ibd.a1;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class IMDBToSQL {
	private static final String NULL_STR = "\\N";

	public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String folderToIMDBGZipFiles = args[3];
		
		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);
		con.setAutoCommit(false);
		PreparedStatement st = con.prepareStatement("CREATE TABLE `Movie` (\n" +
				"  `id` INT(11) NOT NULL,\n" +
				"  `ptitle` VARCHAR(250) NOT NULL,\n" +
				"  `otitle` VARCHAR(250) NOT NULL,\n" +
				"  `adult` BOOLEAN NOT NULL,\n" +
				"  `year` INT(4) NULL,\n" +
				"  `runtime` INT(5) NULL,\n" +
				"  `rating` FLOAT(4,2) NULL,\n" +
				"  `totalvotes` INT(4) NULL,\n" +
				"  PRIMARY KEY (`id`))");
					st.execute();
					st= con.prepareStatement("CREATE TABLE `Genre` (\n" +
							"  `id` INT(4) NOT NULL AUTO_INCREMENT,\n" +
							"  `name` VARCHAR(11) NOT NULL,\n" +
							"  PRIMARY KEY (`id`))");
					st.execute();
		st= con.prepareStatement("CREATE TABLE `MovieGenre` (\n" +
				"  `mid` INT(11) NOT NULL,\n" +
				"  `gid` INT(11) NOT NULL,\n" +
				"  PRIMARY KEY (`mid`,`gid`))");
		st.execute();
		st= con.prepareStatement("CREATE TABLE `Person` (\n" +
				"  `id` INT(11) NOT NULL,\n" +
				"  `name` VARCHAR(110) NOT NULL,\n" +
				"  `byear` INT(4) NULL,\n" +
				"  `dyear` INT(4) NULL,\n" +
				"  PRIMARY KEY (`id`))");
		st.execute();
		st= con.prepareStatement("CREATE TABLE `Actor` (\n" +
				"  `mid` INT(11) NOT NULL,\n" +
				"  `pid` INT(11) NOT NULL,\n" +
				"  PRIMARY KEY (`mid`,`pid`))");
		st.execute();
		st= con.prepareStatement("CREATE TABLE `Director` (\n" +
				"  `mid` INT(11) NOT NULL,\n" +
				"  `pid` INT(11) NOT NULL,\n" +
				"  PRIMARY KEY (`mid`,`pid`))");
		st.execute();
		st= con.prepareStatement("CREATE TABLE `KnownFor` (\n" +
				"  `mid` INT(11) NOT NULL,\n" +
				"  `pid` INT(11) NOT NULL,\n" +
				"  PRIMARY KEY (`mid`,`pid`))");
		st.execute();
		st= con.prepareStatement("CREATE TABLE `Producer` (\n" +
				"  `mid` INT(11) NOT NULL,\n" +
				"  `pid` INT(11) NOT NULL,\n" +
				"  PRIMARY KEY (`mid`,`pid`))");
		st.execute();
		st= con.prepareStatement("CREATE TABLE `Writer` (\n" +
				"  `mid` INT(11) NOT NULL,\n" +
				"  `pid` INT(11) NOT NULL,\n" +
				"  PRIMARY KEY (`mid`,`pid`))");
		st.execute();
					con.commit();
					st.close();


		PreparedStatement stnew = null;
		PreparedStatement stgen = null;
		PreparedStatement stmg = null;

		int step = 1000;
		int cnt = 0;

		InputStream gzipStream_tr = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.ratings.tsv.gz"));
		Scanner sc_tr = new Scanner(gzipStream_tr, "UTF-8");

		sc_tr.nextLine();
		Map<String,String[]> ratings = new HashMap<>();
		String[] wordsArray;

		while(sc_tr.hasNextLine()) {

				wordsArray = sc_tr.nextLine().split("\t");
				ratings.put(wordsArray[0],wordsArray);

		}

		// Load movies. Make Movies table
		InputStream gzipStream_tb = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.basics.tsv.gz"));
		Scanner sc_tb = new Scanner(gzipStream_tb, "UTF-8");



		stnew = con.prepareStatement("INSERT INTO Movie(id,ptitle,otitle,adult,year,runtime,rating,totalvotes) VALUES(?,?,?,?,?,?,?,?)");
		stgen = con.prepareStatement("INSERT INTO Genre(name) VALUES(?)");
		stmg = con.prepareStatement("INSERT INTO MovieGenre(mid,gid) VALUES(?,?)");
		sc_tb.nextLine();
		Map<String, Integer> movie = new HashMap<>();
		int z=1;
		Map<String,Integer> mapgenre = new HashMap<>();
		int i=1;
		while (sc_tb.hasNextLine()) {

			String line = sc_tb.nextLine();

			String[] splitLine = null;
			splitLine = line.split("\t");

			if(splitLine[1].equals("movie")){
				cnt++;
				stnew.setInt(1,Integer.parseInt(splitLine[0].substring(2)));
				movie.put(splitLine[0],z++);
				stnew.setString(2,splitLine[2]);
				stnew.setString(3,splitLine[3]);
				stnew.setInt(4,Integer.parseInt(splitLine[4]));
				if (splitLine[5].equals(NULL_STR))
					stnew.setNull(5, Types.VARCHAR);
				else
					stnew.setInt(5,Integer.parseInt(splitLine[5]));

				if (splitLine[7].equals(NULL_STR))
					stnew.setNull(6, Types.VARCHAR);
				else
					stnew.setInt(6,Integer.parseInt(splitLine[7]));


				String[] arr = ratings.get(splitLine[0]);


				if (arr==null || arr[1].equals(NULL_STR))
					stnew.setNull(7, Types.VARCHAR);
				else
					stnew.setFloat(7,Float.parseFloat(arr[1]));
				if (arr==null || arr[2].equals(NULL_STR))
					stnew.setNull(8, Types.VARCHAR);
				else
					stnew.setInt(8,Integer.parseInt(arr[2]));


				if(!splitLine[8].equals(NULL_STR)){
					String[] arr_of_genre = splitLine[8].split(",");
					for (String gen : arr_of_genre){
						if(!mapgenre.containsKey(gen)){
							mapgenre.put(gen,i++);
							stgen.setString(1,gen);
							stgen.addBatch();
						}
						stmg.setInt(1,Integer.parseInt(splitLine[0].substring(2)));
						stmg.setInt(2,mapgenre.get(gen));
						stmg.addBatch();
					}

				}
				stnew.addBatch();

				if (cnt % step == 0) {
					stnew.executeBatch();
					stgen.executeBatch();
					stmg.executeBatch();
					// Commit the changes.
					con.commit();
				}
			}



		}
		ratings.clear();
		sc_tb.close();


		// Leftovers.
		stnew.executeBatch();
		stgen.executeBatch();
		stmg.executeBatch();
		con.commit();
		stnew.close();
		stgen.close();
		stmg.close();



		//Person table creation

		PreparedStatement stper = null;

		step = 10000;
		cnt = 0;

		InputStream gzipStream_tp = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"name.basics.tsv.gz"));
		Scanner sc_pr = new Scanner(gzipStream_tp, "UTF-8");
		stper = con.prepareStatement("INSERT INTO Person(id,name,byear,dyear) VALUES(?,?,?,?)");
		sc_pr.nextLine();

		while(sc_pr.hasNextLine()) {

			wordsArray = sc_pr.nextLine().split("\t");
			stper.setInt(1,Integer.parseInt(wordsArray[0].substring(2)));
			stper.setString(2,wordsArray[1]);
			if (wordsArray[2].equals(NULL_STR))
				stper.setNull(3, Types.VARCHAR);
			else
				stper.setInt(3,Integer.parseInt(wordsArray[2]));
			if (wordsArray[3].equals(NULL_STR))
				stper.setNull(4, Types.VARCHAR);
			else
				stper.setInt(4,Integer.parseInt(wordsArray[3]));
			stper.addBatch();
			cnt++;



			if (cnt % step == 0) {
				stper.executeBatch();
				con.commit();
			}

		}
		sc_pr.close();


		// Leftovers.
		stper.executeBatch();
		con.commit();
		stper.close();


		//Known for
		PreparedStatement st_knownfor = null;
		InputStream gzipStream_kf = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"name.basics.tsv.gz"));
		Scanner sc_kf = new Scanner(gzipStream_kf, "UTF-8");
		st_knownfor = con.prepareStatement("INSERT IGNORE INTO KnownFor(mid,pid) VALUES(?,?)");
		sc_kf.nextLine();
		String[] arr_of_movies = null;
		int cnt_of_kf = 0;
		step = 10000;

		while(sc_kf.hasNextLine()) {

			wordsArray = sc_kf.nextLine().split("\t");


			if (!wordsArray[5].equals(NULL_STR)){
				arr_of_movies = wordsArray[5].split(",");

				for (String mov : arr_of_movies){
					if(movie.containsKey(mov)){
						st_knownfor.setInt(1,Integer.parseInt(mov.substring(2)));
						st_knownfor.setInt(2,Integer.parseInt(wordsArray[0].substring(2)));
						st_knownfor.addBatch();
						cnt_of_kf++;
						if (cnt_of_kf % step == 0) {
							st_knownfor.executeBatch();
							con.commit();
						}
					}
				}

			}


			}

		sc_kf.close();


		// Leftovers.
		st_knownfor.executeBatch();
		con.commit();
		st_knownfor.close();
		movie.clear();

		//Actors table formation

		PreparedStatement st_actor = null;

		PreparedStatement st_direc = null;
		PreparedStatement st_prod = null;
		PreparedStatement st_writer = null;
		step = 10000;
		int cnt_of_actor = 0;
		int cnt_of_prod = 0;
		int cnt_of_director = 0;
		int cnt_of_writer = 0;



		InputStream gzipStream_p = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.principals.tsv.gz"));
		Scanner sc_p = new Scanner(gzipStream_p, "UTF-8");
		st_actor = con.prepareStatement("INSERT IGNORE INTO Actor(mid,pid) VALUES(?,?)");
		st_prod = con.prepareStatement("INSERT IGNORE INTO Producer(mid,pid) VALUES(?,?)");
		st_direc = con.prepareStatement("INSERT IGNORE INTO Director(mid,pid) VALUES(?,?)");
		st_writer = con.prepareStatement("INSERT IGNORE INTO Writer(mid,pid) VALUES(?,?)");
		sc_p.nextLine();
		while(sc_p.hasNextLine()) {

			wordsArray = sc_p.nextLine().split("\t");
			if(wordsArray[3].equals("actor") || wordsArray[3].equals("actress") || wordsArray[3].equals("self")){
								st_actor.setInt(1,Integer.parseInt(wordsArray[0].substring(2)));
								st_actor.setInt(2,Integer.parseInt(wordsArray[2].substring(2)));
								st_actor.addBatch();
								cnt_of_actor++;



					}
			if(wordsArray[3].equals("producer")){
				st_prod.setInt(1,Integer.parseInt(wordsArray[0].substring(2)));
				st_prod.setInt(2,Integer.parseInt(wordsArray[2].substring(2)));
				st_prod.addBatch();
				cnt_of_prod++;


			}
			if(wordsArray[3].equals("director")){
				st_direc.setInt(1,Integer.parseInt(wordsArray[0].substring(2)));
				st_direc.setInt(2,Integer.parseInt(wordsArray[2].substring(2)));
				st_direc.addBatch();
				cnt_of_director++;


			}
			if(wordsArray[3].equals("writer")){
				st_writer.setInt(1,Integer.parseInt(wordsArray[0].substring(2)));
				st_writer.setInt(2,Integer.parseInt(wordsArray[2].substring(2)));
				st_writer.addBatch();
				cnt_of_writer++;

			}
			if (cnt_of_actor % step == 0) {
				st_actor.executeBatch();
				con.commit();
			}
			if (cnt_of_prod % step == 0) {
				st_prod.executeBatch();
				con.commit();
			}
			if (cnt_of_director % step == 0) {
				st_direc.executeBatch();
				con.commit();
			}
			if (cnt_of_writer % step == 0) {
				st_writer.executeBatch();
				con.commit();
			}

		}
		sc_p.close();


		// Leftovers.
		st_actor.executeBatch();
		st_prod.executeBatch();
		st_direc.executeBatch();
		st_writer.executeBatch();
		con.commit();
		st_actor.close();
		st_prod.close();

		// crew >> Director and Writer
		InputStream gzipStream_c = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.crew.tsv.gz"));
		Scanner sc_c = new Scanner(gzipStream_c, "UTF-8");
		sc_c.nextLine();
		String[] arr_of_director = null;
		String[] arr_of_writer = null;
		int size =0;
		cnt_of_director = 0;
		cnt_of_writer = 0;
		step=10000;

		while(sc_c.hasNextLine()) {

			wordsArray = sc_c.nextLine().split("\t");
			if (!wordsArray[1].equals(NULL_STR)){
				arr_of_director = wordsArray[1].split(",");
				size = arr_of_director.length;
				for(int k=0;k<size;k++){
					st_direc.setInt(1,Integer.parseInt(wordsArray[0].substring(2)));
					st_direc.setInt(2,Integer.parseInt(arr_of_director[k].substring(2)));
					st_direc.addBatch();

				}
				cnt_of_director++;
				if (cnt_of_director % step == 0) {
					st_direc.executeBatch();
					con.commit();
				}

			}
			if (!wordsArray[2].equals(NULL_STR)){
				arr_of_writer = wordsArray[2].split(",");
				size = arr_of_writer.length;
				for(int j=0;j<size;j++){
					st_writer.setInt(1,Integer.parseInt(wordsArray[0].substring(2)));
					st_writer.setInt(2,Integer.parseInt(arr_of_writer[j].substring(2)));
					st_writer.addBatch();

				}
				cnt_of_writer++;
				if (cnt_of_writer % step == 0) {
					st_writer.executeBatch();
					con.commit();
				}
			}




		}
		sc_c.close();
		st_direc.executeBatch();
		st_writer.executeBatch();
		con.commit();
		st_direc.close();
		st_writer.close();


		//deletion

		String q11 = "DELETE b FROM Actor AS b  LEFT JOIN Movie AS m ON b.mid=m.id WHERE m.id IS NULL";
		String q22 = "DELETE b FROM Actor AS b LEFT JOIN Person AS m ON b.pid=m.id WHERE m.id IS NULL";
		String q33 = "DELETE b FROM Producer AS b  LEFT JOIN Movie AS m ON b.mid=m.id WHERE m.id IS NULL";
		String q44 = "DELETE b FROM Producer AS b LEFT JOIN Person AS m ON b.pid=m.id WHERE m.id IS NULL";
		String q55 = "DELETE b FROM Writer AS b LEFT JOIN Movie AS m ON b.mid=m.id WHERE m.id IS NULL";
		String q66 = "DELETE b FROM Writer AS b LEFT JOIN Person AS m ON b.pid=m.id WHERE m.id IS NULL";
		String q77 = "DELETE b FROM Director AS b LEFT JOIN Movie AS m ON b.mid=m.id WHERE m.id IS NULL";
		String q88 = "DELETE b FROM Director AS b LEFT JOIN Person AS m ON b.pid=m.id WHERE m.id IS NULL";


		Statement stt = con.createStatement();
		stt.executeUpdate(q11);
		stt.executeUpdate(q22);
		stt.executeUpdate(q33);
		stt.executeUpdate(q44);
		stt.executeUpdate(q55);
		stt.executeUpdate(q66);
		stt.executeUpdate(q77);
		stt.executeUpdate(q88);

		con.commit();

		con.close();
	}

}
