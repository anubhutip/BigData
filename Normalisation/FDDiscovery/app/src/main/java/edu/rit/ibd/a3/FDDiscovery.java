package edu.rit.ibd.a3;

import java.io.File;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;

import com.google.common.collect.Sets;

public class FDDiscovery {

	public static void main(String[] args) throws Exception {
		final String url = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String relationName = args[3];
		final String outputFile = args[4];
		
		Connection con = DriverManager.getConnection(url, user, pwd);
		
		// These are the attributes of the input relation.
		Set<String> attributes = new HashSet<>();
		// These are the functional dependencies discovered.
		Set<Object> fds = new HashSet<>();
		
		// TODO 0: Your code here!
		
		// Your program must be generic and work for any relation provided as input. You must read the names of the attributes from the input relation and store
		//	them in the attributes set.
		//
		// Read attributes. Use the metadata to get the column info.
		PreparedStatement st = con.prepareStatement("SELECT * FROM " + relationName);
		ResultSet rs = st.executeQuery();
		ResultSetMetaData rsmd = rs.getMetaData();
		int numOfCol = rsmd.getColumnCount();
		for (int i = 1; i <= numOfCol; i++)
			attributes.add(rsmd.getColumnName(i));
		rs.close();


		// Each FD has a left-hand side and a right-hand side. For LHS, start from size one and keep increasing.
		Map<Set<String>, String> map=new HashMap<>();
		for (int size = 1; size < numOfCol-1; size++) { //2
			// Get each combination of attributes in the left-hand side of the appropriate size.
			for (Set<String> leftHandSide : Sets.combinations(attributes, size)) {
				// Get the attributes in the right-hand side.

				for(String rightside : attributes){
					//check trivial
					if(!leftHandSide.contains(rightside)){
						//check minimal
						boolean first =true;
						String query = "SELECT * FROM " + relationName + " AS t1 JOIN "+ relationName + " AS t2 ON ";
						for(String leftatr : leftHandSide){
							if(first){
								query = query + "t1."+leftatr+ " = t2."+leftatr;
								first =false;
							}else{
								query = query + " AND t1."+leftatr+ " = t2."+leftatr;
							}

						}

						query = query + " WHERE t1."+rightside + " != t2."+rightside+ " LIMIT 1";
						st = con.prepareStatement(query);
						ResultSet rs1 = st.executeQuery();
						if (!rs1.isBeforeFirst() ) {
							boolean minimal=true;
							for(Object p: fds){
								String[] arrOfStr = ((String)p).split(" -> ");
								String left = arrOfStr[0];
								String right = arrOfStr[1];
								if(right.equals(rightside)){
									arrOfStr = left.split(", ");
									List<String> listParts = Arrays.asList(arrOfStr);
									if(leftHandSide.containsAll(listParts)) {
										minimal = false;
										break;
									}
								}
							}
							if(minimal){
								List<String> list = new ArrayList<String>(leftHandSide);
								Collections.sort(list);
								String fd = list.toString();
								fd = fd.substring(1,fd.length()-1)+ " -> " + rightside;
								fds.add(fd);
							}

						}


						rs1.close();
					}

				}

			}
		}
		st.close();
		
		// TODO 0: End of your code.
			
		// Write to file!
		PrintWriter writer = new PrintWriter(new File(outputFile));
		for (Object fd : fds)
			writer.println(fd);
		writer.close();
		
		con.close();
	}

}
