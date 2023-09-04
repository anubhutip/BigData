package edu.rit.ibd.a3;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

public class CanonicalCover {

	public static void main(String[] args) throws Exception {
		final String relation = args[0];
		final String fdsStr = args[1];
		final String outputFile = args[2];
		
		// This stores the attributes of the input relation.
		Set<String> attributes = new HashSet<>();
		// This stores the functional dependencies provided as input. This will be the output as well.
		Set<Object> fds = new HashSet<>();

		Map<Integer,List<String>> leftattrOfFDs = new HashMap<>();
		Map<Integer,List<String>> rightattrOfFDs = new HashMap<>();
		
		// TODO 0: Your code here!
		//
		// Parse the input relation that include its attributes. Recall that relation and attribute names can be formed by multiple letters.
		//
		String all_attributes = relation.substring(relation.indexOf("(")+1,relation.indexOf(")"));
		String[] result = all_attributes.split(",\\s*");
		Collections.addAll(attributes, result);

		// Parse the input functional dependencies. Recall that attributes can be formed by multiple letters.
		//
		String funcdeps = fdsStr.replace(" ","");
		String[] arroffds = funcdeps.split(";");
		int i=0;
		for(String fd: arroffds){
			String[] arrOfStr = fd.split("->");
			String left = arrOfStr[0];
			String right = arrOfStr[1];
			String[] attr_left = left.split(",");
			List<String> leftlist = Arrays.asList(attr_left);
			leftattrOfFDs.put(i,leftlist);
			String[] attr_right = right.split(",");
			List<String> rightlist = Arrays.asList(attr_right);
			rightattrOfFDs.put(i++,rightlist);

		}
		boolean thereWasAUnion = true;
		boolean thereWereExtraneous = true;
		while(thereWasAUnion!=false || thereWereExtraneous!=false) {
			thereWasAUnion = false;

			Map<Integer,List<String>> leftattrOfFDs_2 = new HashMap<>();
			for (Map.Entry<Integer, List<String>> entry_i : leftattrOfFDs.entrySet()) {
				leftattrOfFDs_2.put(entry_i.getKey(),entry_i.getValue());
			}

			for (Map.Entry<Integer, List<String>> entry_i : leftattrOfFDs_2.entrySet()) {
				for (Map.Entry<Integer, List<String>> entry_j : leftattrOfFDs_2.entrySet()) {
					if(!(entry_i.getValue().equals(entry_j.getValue()) && rightattrOfFDs.get(entry_i.getKey()).equals(rightattrOfFDs.get(entry_j.getKey())))){
						if (entry_i.getValue().equals(entry_j.getValue())) {
							thereWasAUnion = true;
							Set<String> temp = new HashSet<>();
							temp.addAll(rightattrOfFDs.get(entry_i.getKey()));
							temp.addAll(rightattrOfFDs.get(entry_j.getKey()));
							List<String> ll=new ArrayList<>(temp);
							rightattrOfFDs.put(entry_i.getKey(),ll);
							leftattrOfFDs.remove(entry_j.getKey());
							rightattrOfFDs.remove(entry_j.getKey());
							break;
						}
					}

				}
				if(thereWasAUnion){
					break;
				}
			}

			for (Map.Entry<Integer, List<String>> entry_i : leftattrOfFDs.entrySet()) {
				thereWereExtraneous = false;
				if(entry_i.getValue().size()>1) {
					for (String atrl : entry_i.getValue()) {
						Set<String> leftattributes = new HashSet<>(entry_i.getValue());
						leftattributes.remove(atrl);
						List<String> leftattributeslist = new ArrayList<>(leftattributes);
						Set<String> closure = computeClosure(leftattributes, leftattrOfFDs, rightattrOfFDs);
						if (closure.containsAll(rightattrOfFDs.get(entry_i.getKey()))) {
							entry_i.setValue(leftattributeslist);
							thereWereExtraneous=true;
							break;
						}

					}
				}


				if(thereWereExtraneous) {
					break;
				}


				if(rightattrOfFDs.get(entry_i.getKey()).size()>1) {
					for (String atrr : rightattrOfFDs.get(entry_i.getKey())) {

						Map<Integer,List<String>> leftattrOfFDs_temp = new HashMap<>();
						leftattrOfFDs_temp.putAll(leftattrOfFDs);
						Map<Integer,List<String>> rightattrOfFDs_temp = new HashMap<>();
						rightattrOfFDs_temp.putAll(rightattrOfFDs);

						Set<String> rightattributes = new HashSet<>(rightattrOfFDs.get(entry_i.getKey()));
						rightattributes.remove(atrr);
						List<String> rightattributeslist = new ArrayList<>(rightattributes);
						rightattrOfFDs_temp.put(entry_i.getKey(), rightattributeslist);
						//if atrr is extraneous
						Set<String> leftattributess = new HashSet<>(entry_i.getValue());
						Set<String> closure = computeClosure(leftattributess, leftattrOfFDs_temp, rightattrOfFDs_temp);
						if (closure.contains(atrr)) {
							rightattrOfFDs.clear();
							rightattrOfFDs.putAll(rightattrOfFDs_temp);
							thereWereExtraneous=true;
							break;
						}

					}
				}
				if(thereWereExtraneous) {
					break;
				}

			}

		}

		List<String> newlist;
		for (Map.Entry<Integer, List<String>> entry_j : leftattrOfFDs.entrySet()) {
			newlist = new ArrayList<>(entry_j.getValue());
			boolean first=true;
			String fd ="";
			Collections.sort(newlist);
			for(String str: newlist){
				if(first){
					fd = fd+ str;
					first=false;
				}else{
					fd = fd+ ", " +str;
				}
			}
			fd = fd + " -> ";
			first=true;
			newlist = new ArrayList<>(rightattrOfFDs.get(entry_j.getKey()));
			Collections.sort(newlist);
			for(String str2: newlist){
				if(first){
					fd = fd+ str2;
					first=false;
				}else{
					fd = fd+ ", " +str2;
				}
			}
			fds.add(fd);
		}
		PrintWriter writer = new PrintWriter(new File(outputFile));
		for (Object fd : fds)
			writer.println(fd);
		writer.close();
	}


	private static Set<String> computeClosure(Set<String> core, Map<Integer,List<String>> leftattrOfFDs,
											  Map<Integer,List<String>> rightattrOfFDs) {
		Set<String> closure = new HashSet<>();
		if(!core.isEmpty()){
			closure.addAll(core);
			boolean flag=true;
			while(flag){
				int count =0;
				for (Map.Entry<Integer,List<String>> entry : leftattrOfFDs.entrySet()){
					if(closure.containsAll(entry.getValue())){
						boolean flag2 = closure.addAll(rightattrOfFDs.get(entry.getKey()));
						if(!flag2){
							count++;
						}
					}else{
						count++;
					}
				}
				if(count==leftattrOfFDs.size()){
					flag=false;
				}
			}
		}
		return closure;
	}

}
