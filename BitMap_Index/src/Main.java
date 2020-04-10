import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * 
 */

/**
 * @author ekjot
 *
 */
public class Main {

	static Integer TOTAL_TUPLES;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		System.out.println("File 1:");
		String fileAddress1 = "C:\\Users\\ekjot\\git\\BitMap_Index\\BitMap_Index\\Data\\Data2.txt";// getFileAddress();

		System.out.println("\nFile 2:");
		String fileAddress2 = "C:\\Users\\ekjot\\git\\BitMap_Index\\BitMap_Index\\Data\\Data3.txt";// getFileAddress();

		/*
		 * C:\Users\ekjot\git\BitMap_Index\BitMap_Index\Data\Data.txt
		 */

		byte numOfSublists = phaseOne(fileAddress1, fileAddress2);
		buildBitIndex(numOfSublists);

	}

	private static void buildBitIndex(byte numOfSublists) throws IOException {

		makeEmpIdBitIndex(numOfSublists);

	}

	private static void makeEmpIdBitIndex(byte numOfSublists) throws IOException {
		// list to store all reading streams
		ArrayList<DataReader> DR = new ArrayList<DataReader>();

		// linking streams to all sublists
		for (byte i = 1; i <= numOfSublists; i++) {
			DR.add(new DataReader("sublists\\" + i + "_emp.txt"));
		}

		// Map to store K,V as empid and a list of its indexes
		TreeMap<Integer, ArrayList<Integer>> empMap = new TreeMap<Integer, ArrayList<Integer>>();

		// stream for writing
		BufferedWriter bw = new BufferedWriter(new FileWriter("sublists\\emp.txt"));

		// MERGING ALGORITHM
		// read first line of every sublist into a list
		//// if two sublists have same empid, merge/append those in list
		// sort the list from min empid to max(used TreeMap, so no need of sort)
		// write bitmap index of min empid to a file
		// read next line from all sublists
		// repeat untill no sublist is left.

		// repeat untill all streams are done or in other words, DR is empty
		while (!DR.isEmpty()) {

			// used iterator because of concurrent modification to DR list
			Iterator<DataReader> itr = DR.iterator();

			// fill list with a line from each stream
			fillMap(itr, empMap);

			if (empMap.isEmpty()) {
				break;
			}

			// write bitmap index of min empid
			writeBIForMinEmpid(bw, empMap);

		}

		//if there are some left empid in list even after all streams are finished
		while (!empMap.isEmpty()) {
			writeBIForMinEmpid(bw, empMap);
		}

		bw.close();

	}

	private static void fillMap(Iterator<DataReader> itr, TreeMap<Integer, ArrayList<Integer>> empMap)
			throws IOException {
		// traverse through all streams
		while (itr.hasNext()) {
			DataReader dr = itr.next();
			String str = dr.readLine();

			// if there is no next line, meaning stream/sublist is complete
			if (str == null) {
				itr.remove(); // remove it from DR list
				continue; // continue to next stream
			}

			// adding string containing empid and indexes to list
			addStrToMap(str, empMap);

		}

	}

	private static void writeBIForMinEmpid(BufferedWriter bw, TreeMap<Integer, ArrayList<Integer>> empMap)
			throws IOException {
		Integer empid = empMap.firstKey();

		ArrayList<Integer> index_lastUpdate = empMap.get(empid);

		TreeMap<Integer, Integer> lastUpdate_index = new TreeMap<Integer, Integer>();
		Integer[] index = new Integer[index_lastUpdate.size() / 2];

		for (int i = 0; i < index_lastUpdate.size() / 2; i++) {
			lastUpdate_index.put(index_lastUpdate.get(i * 2 + 1), index_lastUpdate.get(i * 2));
			index[i] = index_lastUpdate.get(i * 2);
		}
		Arrays.sort(index);

		writeBitmapIndex(bw, empid, index);
		empMap.remove(empid);

	}

	private static void addStrToMap(String str, TreeMap<Integer, ArrayList<Integer>> empMap) {
		// splitting the line into empid and list of index and lastupdate
		String[] kv = (str).split(" ");
		Integer empid = Integer.parseInt(kv[0]);

		// if map do not have given empid, add it with list of index and lastupdate as
		// it is
		if (!empMap.containsKey(empid)) {
			ArrayList<Integer> list = new ArrayList<Integer>();
			for (int i = 1; i < kv.length; i++) {
				list.add(Integer.parseInt(kv[i]));
			}
			empMap.put(empid, list);
		} // if map have given empid, append list of index and lastupdate to its arraylist
		else {
			for (int i = 1; i < kv.length; i++) {
				empMap.get(empid).add(Integer.parseInt(kv[i]));
			}

		}

	}

	private static void writeBitmapIndex(BufferedWriter bw, Integer empid, Integer[] index) throws IOException {
		bw.write(empid + " ");
		int i = 1;
		for (Integer ind : index) {
			for (; i < ind; i++) {
				bw.write("0");
			}
			bw.write("1");
			i++;
		}
		while (TOTAL_TUPLES - (i++) + 1 > 0) {
			bw.write("0");
		}
		bw.write("\n");
	}

	private static byte phaseOne(String fileAddress1, String fileAddress2) throws IOException {
		byte numOfSublists = 0;
		int numOfTuples = 0;
		boolean file1Complete = false, file2Complete = false;

		DataReader dr = new DataReader(fileAddress1);
		byte firstByte = dr.readByte();

		TreeMap<Integer, ArrayList<Integer>> empHash = new TreeMap<Integer, ArrayList<Integer>>();
		TreeMap<Integer, ArrayList<Integer>> genderHash = new TreeMap<Integer, ArrayList<Integer>>();
		TreeMap<Integer, ArrayList<Integer>> deptHash = new TreeMap<Integer, ArrayList<Integer>>();

		// run until both files are complete
		while (!file1Complete || !file2Complete) {

			// code to select second file when first file ends
			if (firstByte == -1) { // any file ends

				if (!file1Complete) { // if first file ended
					file1Complete = true;

					dr = new DataReader(fileAddress2);
					firstByte = dr.readByte();

					// If the second file is empty
					if (firstByte == -1) {
						file2Complete = true;
						break;
					}
				} else if (!file2Complete) { // if second file ended
					file2Complete = true;
					break;
				}
			}

			// code to read a tuple and extract values
			Tuple tuple = dr.readTuple(firstByte);
			numOfTuples++;
			System.out.println(numOfTuples);
			Integer empid = tuple.getEmpIDAsNum();
			Integer gender = tuple.getGenderAsNum();
			Integer dept = tuple.getDeptAsNum();

			Integer lastUpdate = tuple.getLastUpdateAsNum();

			// if treemaps dont have given key, add it and assign new arraylist
			if (!empHash.containsKey(empid)) {
				ArrayList<Integer> list = new ArrayList<Integer>();
				list.add(numOfTuples);
				list.add(lastUpdate);
				empHash.put(empid, list);
			} // if treemap have given key, add index to its arraylist
			else {

				empHash.get(empid).add(numOfTuples);
				empHash.get(empid).add(lastUpdate);
			}

			if (!genderHash.containsKey(gender)) {
				ArrayList<Integer> list = new ArrayList<Integer>();
				list.add(numOfTuples);
				genderHash.put(gender, list);
			} else {
				genderHash.get(gender).add(numOfTuples);
			}

			if (!deptHash.containsKey(dept)) {
				ArrayList<Integer> list = new ArrayList<Integer>();
				list.add(numOfTuples);
				deptHash.put(dept, list);
			} else {
				deptHash.get(dept).add(numOfTuples);
			}

			// System.out.println(firstByte+" "+numOfTuples+" "+tuple.toString());

			// read a small batch only and write it to disk; refresh treemaps
			if (numOfTuples % 1000 == 0) {

				writeSublist(++numOfSublists, empHash, genderHash, deptHash);

				empHash = new TreeMap<Integer, ArrayList<Integer>>();
				genderHash = new TreeMap<Integer, ArrayList<Integer>>();
				deptHash = new TreeMap<Integer, ArrayList<Integer>>();
				// break;
			}
			firstByte = dr.readByte();
		}

		if (!empHash.isEmpty()) {
			writeSublist(++numOfSublists, empHash, genderHash, deptHash);
		}
		TOTAL_TUPLES = numOfTuples;
		return numOfSublists;
	}

	private static void writeSublist(byte numOfSublists, TreeMap<Integer, ArrayList<Integer>> empHash,
			TreeMap<Integer, ArrayList<Integer>> genderHash, TreeMap<Integer, ArrayList<Integer>> deptHash)
			throws IOException {

		BufferedWriter br = new BufferedWriter(new FileWriter("sublists\\" + numOfSublists + "_emp.txt"));

		for (Integer empid : empHash.keySet()) {
			br.write(String.format("%08d", empid));
			for (Integer index : empHash.get(empid)) {
				br.write(" " + index);
			}
			br.write("\r\n");
		}

		br.close();

		br = new BufferedWriter(new FileWriter("sublists\\" + numOfSublists + "_dept.txt"));

		for (Integer dept : deptHash.keySet()) {
			br.write(String.format("%03d", dept));
			for (Integer index : deptHash.get(dept)) {
				br.write(" " + index);
			}
			br.write("\r\n");
		}

		br.close();

		br = new BufferedWriter(new FileWriter("sublists\\" + numOfSublists + "_gender.txt"));

		for (Integer gender : genderHash.keySet()) {
			br.write(gender + "");
			for (Integer index : genderHash.get(gender)) {
				br.write(" " + index);
			}
			br.write("\r\n");
		}

		br.close();

	}

	private static String getFileAddress() throws IOException {
		// TODO Auto-generated method stub
		System.out.println("Enter File Address: ");
		String address = (new BufferedReader(new InputStreamReader(System.in))).readLine();
		if (!checkFileValidity(address)) {
			System.out.println("Entered File Address is invalid.\nTry again.");
			address = getFileAddress();
		}

		return address;
	}

	private static boolean checkFileValidity(String fileAddress) {
		File f = new File(fileAddress);
		return f.exists() && !f.isDirectory();
	}

}
