import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

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

		buildIndex(fileAddress1, fileAddress2);

	}

	private static void buildIndex(String fileAddress1, String fileAddress2) throws IOException {

		HashMap<Integer, ArrayList<Boolean>> empHash = new HashMap<Integer, ArrayList<Boolean>>();
		HashMap<Boolean, ArrayList<Boolean>> genderHash = new HashMap<Boolean, ArrayList<Boolean>>();
		HashMap<Integer, ArrayList<Boolean>> deptHash = new HashMap<Integer, ArrayList<Boolean>>();

		int numOfTuples = 0;
		boolean file1Complete = false, file2Complete = false;

		DataReader dr = new DataReader(fileAddress1);
		byte firstByte = dr.readByte();

		while (!file1Complete || !file2Complete) {

			if (firstByte == -1) { // file ends

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

			Integer empid = tuple.getEmpIDAsNum();
			Boolean gender = tuple.getGenderAsBoolean(); // true is male
			Integer dept = tuple.getDeptAsNum();

			
			
			
			if (!empHash.containsKey(empid)) {
				ArrayList<Boolean> list = new ArrayList<Boolean>();
				if (numOfTuples > 1) {
					list = new ArrayList<Boolean>(Arrays.asList(new Boolean[numOfTuples - 1]));

					Collections.fill(list, Boolean.FALSE);
				}
				list.add(true);
				empHash.put(empid, list);
			}
			if (!genderHash.containsKey(gender)) {
				ArrayList<Boolean> list = new ArrayList<Boolean>();
				if (numOfTuples > 1) {
					list = new ArrayList<Boolean>(Arrays.asList(new Boolean[numOfTuples - 1]));

					Collections.fill(list, Boolean.FALSE);
				}
				list.add(true);
				genderHash.put(gender, list);
			}
			if (!deptHash.containsKey(dept)) {
				ArrayList<Boolean> list = new ArrayList<Boolean>();
				if (numOfTuples > 1) {
					list = new ArrayList<Boolean>(Arrays.asList(new Boolean[numOfTuples - 1]));

					Collections.fill(list, Boolean.FALSE);
				}
				list.add(true);
				deptHash.put(dept, list);
			}

		}

		// List<Boolean> list=new ArrayList<Boolean>(Arrays.asList(new Boolean[10]));
		// Collections.fill(list, Boolean.TRUE);

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
