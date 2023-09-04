package edu.rit.ibd.a3;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

public class TNFDecomposition {

	public static void main(String[] args) throws Exception {
		final String relation = args[0];
		final String fdsStr = args[1];
		final String cksStr = args[2];
		final String outputFile = args[3];
		
		// This stores the attributes of the input relation.
		Set<String> attributes = new TreeSet<>();
		// This stores the functional dependencies provided as input.
		Set<Object> fds = new HashSet<>();
		// This stores the candidate keys provided as input.
		List<Set<String>> cks = new ArrayList<>();
		// This stores the final 3NF decomposition, i.e., the output.
		List<Set<String>> decomposition = new ArrayList<>();

		Map<Integer,Set<String>> leftattrOfFDs = new HashMap<>();
		Map<Integer,Set<String>> rightattrOfFDs = new HashMap<>();
		
		
		// TODO 0: Your code here!
		//
		// Parse the input relation that include its attributes. Recall that relation and attribute names can be formed by multiple letters.
		String all_attributes = relation.substring(relation.indexOf("(")+1,relation.indexOf(")"));
		String[] result = all_attributes.split(",\\s*");
		Collections.addAll(attributes, result);
		//
		// Parse the input functional dependencies. These are already a canonical cover. Recall that attributes can be formed by multiple letters.
		//
		String funcdeps = fdsStr.replace(" ","");
		String[] arroffds = funcdeps.split(";");
		int i=0;
		for(String fd: arroffds){
			String[] arrOfStr = fd.split("->");
			String left = arrOfStr[0];
			String right = arrOfStr[1];
			String[] attr_left = left.split(",");
			Set<String> leftlist = new HashSet<>(Arrays.asList(attr_left));
			leftattrOfFDs.put(i,leftlist);
			String[] attr_right = right.split(",");
			Set<String> rightlist = new HashSet<>(Arrays.asList(attr_right));
			rightattrOfFDs.put(i++,rightlist);

		}
		// Parse the input candidate keys. Recall that attributes can be formed by multiple letters.
		//
		String cks1 = cksStr.replace(" ","");
		String[] cks2 = cks1.split(";");
		for(String ck: cks2){
			String[] attrs = ck.split(",");
			Set<String> ckset = new TreeSet<>(Arrays.asList(attrs));
			cks.add(ckset);
		}

		// Test whether already in 3NF.
		// Analyze whether the relation is already in 3NF:

		for (Map.Entry<Integer, Set<String>> fd_leftattr : leftattrOfFDs.entrySet()) {
			boolean case2 = false;
			boolean case3 = false;
			if((fd_leftattr.getValue().containsAll(rightattrOfFDs.get(fd_leftattr.getKey())))){
				continue;
			}
			for(Set<String> ck : cks){
				if(fd_leftattr.getValue().containsAll(ck)){
					case2 = true;
					break;
				}
			}
			if(case2){
				continue;
			}
			Set<String> rightsttrs = new HashSet<>(rightattrOfFDs.get(fd_leftattr.getKey()));
			rightsttrs.removeAll(fd_leftattr.getValue());
			//beta-alpha
			for(Set<String> ck : cks){

				if(ck.containsAll(rightsttrs)){
					case3 = true;
					break;
				}
			}
			if(case3){
				continue;
			}

			for(Map.Entry<Integer, Set<String>> fd_leftattr2 : leftattrOfFDs.entrySet()){
				Set<String> new_relation = new TreeSet<>();
				new_relation.addAll(fd_leftattr2.getValue());
				new_relation.addAll(rightattrOfFDs.get(fd_leftattr2.getKey()));
				decomposition.add(new_relation);
			}
			break;

		}

		if(decomposition.size()>0 ){
			boolean ckWasPresent = false;
			for(Set<String> rel : decomposition){
				for(Set<String> ck : cks){

					if(rel.containsAll(ck)){
						ckWasPresent = true;
						break;
					}
				}
			}
			if(!ckWasPresent){
				decomposition.add(cks.get(0));
			}


			boolean redundant =true;
			//remove redundant relations
			while(redundant){
				redundant=false;
				Set<Set<String>> decomposition_set = new HashSet<>(decomposition);
				for(Set<String> reli: decomposition_set){
					for(Set<String> relj : decomposition_set){
						if(!reli.equals(relj) && relj.containsAll(reli)){
							decomposition.remove(reli);
							redundant=true;
							break;
						}
					}
				}
			}

		}else {
			decomposition.add(attributes);
		}

		
		PrintWriter writer = new PrintWriter(new File(outputFile));
		for (Set<String> r : decomposition)
			writer.println("r(" + r.stream().sorted().collect(java.util.stream.Collectors.toList()).
					toString().replace("[", "").replace("]", "") + ")");
		writer.close();
	}

}
