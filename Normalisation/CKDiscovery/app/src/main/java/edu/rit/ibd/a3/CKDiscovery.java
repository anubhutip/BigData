package edu.rit.ibd.a3;

import com.google.common.collect.Sets;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

public class CKDiscovery {
	
	public static void main(String[] args) throws Exception {
		final String relation = args[0];
		final String fdsStr = args[1];
		final String outputFile = args[2];
		
		// This stores the attributes of the input relation.
		Set<String> attributes = new HashSet<>();
		// This stores the functional dependencies provided as input.
		Set<Object> fds = new HashSet<>();
		// This stores the candidate keys discovered; each key is a set of attributes.
		List<Set<String>> keys = new ArrayList<>();

		Set<String> case_nowhere = new HashSet<>();
		Set<String> case_inright = new HashSet<>();
		Set<String> case_inleft = new HashSet<>();
		Set<String> case_both = new HashSet<>();
		Map<Integer,List<String>> leftattrOfFDs = new HashMap<>();
		Map<Integer,List<String>> rightattrOfFDs = new HashMap<>();

		
		// TODO 0: Your code here!
		
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
			fds.add(fd);
			String[] arrOfStr = fd.split("->");
			String left = arrOfStr[0];
			String right = arrOfStr[1];
			String[] attr_left = left.split(",");
			List<String> leftlist = Arrays.asList(attr_left);
			leftattrOfFDs.put(i,leftlist);
			String[] attr_right = right.split(",");
			List<String> rightlist = Arrays.asList(attr_right);
			rightattrOfFDs.put(i++,rightlist);
			for(String atr: attributes){
				if(leftlist.contains(atr)){
					case_inleft.add(atr);
				}
				if(rightlist.contains(atr)){
					case_inright.add(atr);
				}
			}

		}

		// For each attribute a, you must classify as case 1 (a is not in the functional dependencies), case 2 (a is only in the right-hand side),
		//	case 3 (a is only in the left-hand side), case 4 (a is in both left- and right-hand sides).
		//
		for(String atr: attributes){
			if(case_inleft.contains(atr) && case_inright.contains(atr)){
				case_both.add(atr);
				case_inleft.remove(atr);
				case_inright.remove(atr);
			}else if(!case_inleft.contains(atr) && !case_inright.contains(atr) && !case_both.contains(atr)){
				case_nowhere.add(atr);
			}
		}

		Set<String> core = new TreeSet<>();
		core.addAll(case_inleft);
		core.addAll(case_nowhere);


		//if core exists
		Set<String> closure = computeClosure(fds, core, leftattrOfFDs,rightattrOfFDs);
		if(closure.containsAll(attributes)){
			keys.add(core);
		}else{
			for (int size = 1; size <case_both.size(); size++) {
				for (Set<String> attrs : Sets.combinations(case_both, size)) {
					Set<String> coreattrs = new TreeSet<>();
					coreattrs.addAll(attrs);
					coreattrs.addAll(core);
					closure = computeClosure(fds, coreattrs, leftattrOfFDs,rightattrOfFDs);
					if(closure.containsAll(attributes)){
						boolean minimal =true;
						for(Set<String> key: keys){
							if(coreattrs.containsAll(key)){
								minimal=false;
								break;
							}
						}
						if(minimal){
							keys.add(coreattrs);
						}

					}
				}

			}

		}
		
		// TODO 0: End of your code.
		
		PrintWriter writer = new PrintWriter(new File(outputFile));
		for (Set<String> key : keys)
			writer.println(key.stream().sorted().collect(java.util.stream.Collectors.toList()).
					toString().replace("[", "").replace("]", ""));
		writer.close();

	}
	
	private static Set<String> computeClosure(Set<Object> fds, Set<String> core, Map<Integer,List<String>> leftattrOfFDs,
											  Map<Integer,List<String>> rightattrOfFDs) {
		Set<String> closure = new TreeSet<>();
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
				if(count==fds.size()){
					flag=false;
				}
			}
		}
		return closure;
	}

}
