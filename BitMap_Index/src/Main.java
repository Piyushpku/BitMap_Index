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

		TreeMap<String, ArrayList<String>> empHash = new TreeMap<String, ArrayList<String>>();
		TreeMap<String, ArrayList<String>> genderHash = new TreeMap<String, ArrayList<String>>();
		TreeMap<String, ArrayList<String>> deptHash = new TreeMap<String, ArrayList<String>>();

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

			String empid = tuple.getEmpIDAsString();
			String gender = tuple.getGenderAsString();
			String dept = tuple.getDeptAsString();

			if (!empHash.containsKey(empid)) {
				ArrayList<String> list = new ArrayList<String>();
				list.add(numOfTuples + "");
				empHash.put(empid, list);
			} else {
				empHash.get(empid).add(numOfTuples + "");
			}

			if (!genderHash.containsKey(gender)) {
				ArrayList<String> list = new ArrayList<String>();
				list.add(numOfTuples + "");
				genderHash.put(gender, list);
			} else {
				genderHash.get(gender).add(numOfTuples + "");
			}

			if (!deptHash.containsKey(dept)) {
				ArrayList<String> list = new ArrayList<String>();
				list.add(numOfTuples + "");
				deptHash.put(dept, list);
			} else {
				deptHash.get(dept).add(numOfTuples + "");
			}

			// System.out.println(firstByte+" "+numOfTuples+" "+tuple.toString());
			if (numOfTuples % 250000 == 0) {

				writeSublist(++numOfSublists, empHash, genderHash, deptHash);

				empHash = new TreeMap<String, ArrayList<String>>();
				genderHash = new TreeMap<String, ArrayList<String>>();
				deptHash = new TreeMap<String, ArrayList<String>>();

			}
			firstByte = dr.readByte();
		}

		if (!empHash.isEmpty()) {
			writeSublist(++numOfSublists, empHash, genderHash, deptHash);
		}

		return numOfSublists;
	}

	private static void writeSublist(byte numOfSublists, TreeMap<String, ArrayList<String>> empHash,
			TreeMap<String, ArrayList<String>> genderHash, TreeMap<String, ArrayList<String>> deptHash)
			throws IOException {

		BufferedWriter br = new BufferedWriter(new FileWriter(numOfSublists + "_emp.txt"));

		for (String empid : empHash.keySet()) {
			br.write(empid);
			for (String index : empHash.get(empid)) {
				br.write(" " + index);
			}
			br.write("\r\n");
		}

		br.close();

		br = new BufferedWriter(new FileWriter(numOfSublists + "_dept.txt"));

		for (String dept : deptHash.keySet()) {
			br.write(dept + "");
			for (String index : deptHash.get(dept)) {
				br.write(" " + index);
			}
			br.write("\r\n");
		}

		br.close();

		br = new BufferedWriter(new FileWriter(numOfSublists + "_gender.txt"));

		for (String gender : genderHash.keySet()) {
			br.write(gender + "");
			for (String index : genderHash.get(gender)) {
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
