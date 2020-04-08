import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * 
 */

/**
 * @author ekjot
 *
 */
public class Main {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		System.out.println("File 1:");
		String fileAddress1 = "C:\\Users\\ekjot\\git\\TPMMS\\TPMMS\\Data\\Data1.txt";// getFileAddress();

		System.out.println("\nFile 2:");
		String fileAddress2 = "C:\\Users\\ekjot\\git\\TPMMS\\TPMMS\\Data\\Data2.txt";// getFileAddress();

		/*
		 * C:\Users\ekjot\git\TPMMS\TPMMS\Data\Data.txt
		 */

		byte numOfSublists = phaseOne(fileAddress1, fileAddress2);

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

		while (!file1Complete || !file2Complete) {
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

			Tuple tuple = dr.readTuple(firstByte);
			numOfTuples++;
System.out.println(numOfTuples);
			Integer empid = tuple.getEmpIDAsNum();
			Integer gender = tuple.getGenderAsNum();
			Integer dept = tuple.getDeptAsNum();

			if (!empHash.containsKey(empid)) {
				ArrayList<Integer> list = new ArrayList<Integer>();
				list.add(numOfTuples);
				empHash.put(empid, list);
			} else {
				
				empHash.get(empid).add(numOfTuples);
//				empHash.get(empid).trimToSize();
			}

			if (!genderHash.containsKey(gender)) {
				ArrayList<Integer> list = new ArrayList<Integer>();
				list.add(numOfTuples);
				genderHash.put(gender, list);
			} else {
				genderHash.get(gender).add(numOfTuples);
//				genderHash.get(gender).trimToSize();
			}

			if (!deptHash.containsKey(dept)) {
				ArrayList<Integer> list = new ArrayList<Integer>();
				list.add(numOfTuples);
				deptHash.put(dept, list);
			} else {
				deptHash.get(dept).add(numOfTuples);
//				deptHash.get(dept).trimToSize();
			}

			// System.out.println(firstByte+" "+numOfTuples+" "+tuple.toString());
			if (numOfTuples % 30000 == 0) {

				writeSublist(++numOfSublists, empHash, genderHash, deptHash);

				empHash = new TreeMap<Integer, ArrayList<Integer>>();
				genderHash = new TreeMap<Integer, ArrayList<Integer>>();
				deptHash = new TreeMap<Integer, ArrayList<Integer>>();
break;
			}
			firstByte = dr.readByte();
		}

		if (!empHash.isEmpty()) {
			writeSublist(++numOfSublists, empHash, genderHash, deptHash);
		}

		return numOfSublists;
	}

	private static void writeSublist(byte numOfSublists, TreeMap<Integer, ArrayList<Integer>> empHash,
			TreeMap<Integer, ArrayList<Integer>> genderHash, TreeMap<Integer, ArrayList<Integer>> deptHash)
			throws IOException {

		BufferedWriter br = new BufferedWriter(new FileWriter(numOfSublists + "_emp.txt"));

		for (Integer empid : empHash.keySet()) {
			br.write(String.format("%08d", empid));
			for (Integer index : empHash.get(empid)) {
				br.write(" " + index);
			}
			br.write("\r\n");
		}

		br.close();

		br = new BufferedWriter(new FileWriter(numOfSublists + "_dept.txt"));

		for (Integer dept : deptHash.keySet()) {
			br.write(String.format("%03d", dept));
			for (Integer index : deptHash.get(dept)) {
				br.write(" " + index);
			}
			br.write("\r\n");
		}

		br.close();

		br = new BufferedWriter(new FileWriter(numOfSublists + "_gender.txt"));

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
