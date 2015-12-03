package files_indexing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;


public class MyDatabase {
	
	final static byte double_blind_mask      = 8;    // binary 0000 1000
	final static byte controlled_study_mask  = 4;    // binary 0000 0100
	final static byte govt_funded_mask       = 2;    // binary 0000 0010
	final static byte fda_approved_mask      = 1;    // binary 0000 0001	
	final static byte delete_mask            = (byte) 64;
	
	static PrintWriter fileId = null;		
	static PrintWriter fileCompany = null;
	static PrintWriter fileDid = null;
	static PrintWriter fileTrials = null;
	static PrintWriter filePatients = null;
	static PrintWriter fileDosage = null;
	static PrintWriter fileReading = null;
	static PrintWriter fileBlind = null;
	static PrintWriter fileStudy = null;
	static PrintWriter fileGovt = null;
	static PrintWriter fileFda = null;
	
	static Map<Integer, Long> MapId;
	static HashMap<String, ArrayList<Long>> MapComp;
	static HashMap<String, ArrayList<Long>> MapDid;
	static HashMap<Short, ArrayList<Long>> MapTrials;
	static HashMap<Short, ArrayList<Long>> MapPatients;
	static HashMap<Short, ArrayList<Long>> MapDosage;
	static HashMap<Float, ArrayList<Long>> MapReading;
	static HashMap<String, ArrayList<Long>> MapBlind;
	static HashMap<String, ArrayList<Long>> MapStudy;
	static HashMap<String, ArrayList<Long>> MapGovt;
	static HashMap<String, ArrayList<Long>> MapFda;
	
	static Map<Integer, Long> treeMapId;
	static Map<String, ArrayList<Long>> treeMapComp;
	static Map<String, ArrayList<Long>> treeMapDid;
	static Map<Short, ArrayList<Long>> treeMapTrials;
	static Map<Short, ArrayList<Long>> treeMapPatients;
	static Map<Short, ArrayList<Long>> treeMapDosage;
	static Map<Float, ArrayList<Long>> treeMapReading;
	
	static Map<String, ArrayList<Long>> treeMapBlind;
	static Map<String, ArrayList<Long>> treeMapStudy;
	static Map<String, ArrayList<Long>> treeMapGovt;
	static Map<String, ArrayList<Long>> treeMapFda;
	
	
	private static int Mask(String a, String b, String c, String d)
	{
		byte commonByte = 0 | delete_mask;
		
		if(a.equals("true"))
			commonByte = (byte) (commonByte | double_blind_mask);
		
		if(b.equals("true"))
			commonByte = (byte) (commonByte | controlled_study_mask);
		
		if(c.equals("true"))
			commonByte = (byte) (commonByte | govt_funded_mask);
		
		if(d.equals("true"))
			commonByte = (byte) (commonByte | fda_approved_mask);
		
		return commonByte;		
	}
	
	private static int DeleteMask(byte code)
	{
		if((code & delete_mask) == delete_mask)
		{
			byte mask = (byte) (code ^ delete_mask);
			return mask;
		}
		else
			return -1;
	}
	
	private static String UnMaskStr(byte code)
	{
		String output = "";		
		
		if((code & double_blind_mask) == double_blind_mask)
			output = "true,";
		else
			output = "false,";
		
		if((code & controlled_study_mask) == controlled_study_mask)
			output += "true,";
		else
			output += "false,";
		
		if((code & govt_funded_mask) == govt_funded_mask)
			output += "true,";
		else
			output += "false,";
		
		if((code & fda_approved_mask) == fda_approved_mask)
			output += "true";
		else
			output += "false";
		return output;
	}
	
	private static String[] Tokenize(String line)
	{		
		String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		return tokens;
	}	
	
	private static void initMaps()
	{
		//Create hash map for index entries
		MapId = new HashMap<Integer, Long>();
		MapComp = new HashMap<String, ArrayList<Long>>();
		MapDid = new HashMap<String, ArrayList<Long>>();
		MapTrials = new HashMap<Short, ArrayList<Long>>();
		MapPatients = new HashMap<Short, ArrayList<Long>>();
		MapDosage = new HashMap<Short, ArrayList<Long>>();
		MapReading = new HashMap<Float, ArrayList<Long>>();
		MapBlind = new HashMap<String, ArrayList<Long>>();
		MapGovt = new HashMap<String, ArrayList<Long>>();
		MapStudy = new HashMap<String, ArrayList<Long>>();
		MapFda = new HashMap<String, ArrayList<Long>>();	
	}
	
	private static int getIndex(String field)
	{
		if(field.toLowerCase().equals("id"))
			return 1;
		else if(field.toLowerCase().equals("company"))
			return 2;
		else if(field.toLowerCase().equals("drug_id"))
			return 3;
		else if(field.toLowerCase().equals("trials"))
			return 4;
		else if(field.toLowerCase().equals("patients"))
			return 5;
		else if(field.toLowerCase().equals("dosage_mg"))
			return 6;
		else if(field.toLowerCase().equals("reading"))
			return 7;
		else if(field.toLowerCase().equals("double_blind"))
			return 8;
		else if(field.toLowerCase().equals("controlled_study"))
			return 9;
		else if(field.toLowerCase().equals("govt_funded"))
			return 10;
		else if(field.toLowerCase().equals("fda_approved"))
			return 11;
		return -1;
	}	
	
	private static void IndexFileReader(String tableName,int index) throws IOException
	{
		BufferedReader reader = null;
		String line;
	
		switch(index)
		{
		case 1:
			reader = new BufferedReader(new FileReader(tableName + ".id.ndx"));
			MapId = new HashMap<Integer, Long>();
			line = "";
			while((line = reader.readLine()) != null)
			{
				String row[] = line.split("::");
				//if(!(row[0].isEmpty()) && !(row[1].isEmpty()))
				MapId.put(Integer.parseInt(row[0]), Long.parseLong(row[1]));				
			}
			break;
			
		case 2:
			reader = new BufferedReader(new FileReader(tableName + ".company.ndx"));
			MapComp = new HashMap<String, ArrayList<Long>>();
			line="";
			while((line = reader.readLine()) != null)
			{
				String row[] = line.split("::");
				//if(!(row[0].isEmpty()) && !(row[1].isEmpty()))
				String str = row[1];
				String str1 = str.replace("[", "");
				String str2 = str1.replace("]", "");
				
				String[] fields = str2.split(",");
				ArrayList<Long> temp = new ArrayList<Long>();
				for(int i=0; i< fields.length;++i)
					temp.add(Long.parseLong(fields[i].trim()));
				
				MapComp.put(row[0], temp);
			}
			break;
			
		case 3:
			reader = new BufferedReader(new FileReader(tableName + ".drug_id.ndx"));
			MapDid = new HashMap<String, ArrayList<Long>>();
			line="";
			while((line = reader.readLine()) != null)
			{
				String row[] = line.split("::");
				
				String str = row[1];
				String str1 = str.replace("[", "");
				String str2 = str1.replace("]", "");
				
				String[] fields = str2.split(",");
				ArrayList<Long> temp = new ArrayList<Long>();
				for(int i=0; i< fields.length;++i)
					temp.add(Long.parseLong(fields[i].trim()));
				
				MapDid.put(row[0], temp);
			}
			break;
			
		case 4:
			reader = new BufferedReader(new FileReader(tableName + ".trials.ndx"));
			MapTrials = new HashMap<Short, ArrayList<Long>>();
			line="";
			while((line = reader.readLine()) != null)
			{
				String row[] = line.split("::");
				
				String str = row[1];
				String str1 = str.replace("[", "");
				String str2 = str1.replace("]", "");
				
				String[] fields = str2.split(",");
				ArrayList<Long> temp = new ArrayList<Long>();
				for(int i=0; i< fields.length;++i)
					temp.add(Long.parseLong(fields[i].trim()));
				
				MapTrials.put(Short.parseShort(row[0]), temp);
			}
			break;
			
		case 5:
			reader = new BufferedReader(new FileReader(tableName + ".patients.ndx"));
			MapPatients = new HashMap<Short, ArrayList<Long>>();
			line="";
			while((line = reader.readLine()) != null)
			{
				String row[] = line.split("::");
				
				String str = row[1];
				String str1 = str.replace("[", "");
				String str2 = str1.replace("]", "");
				
				String[] fields = str2.split(",");
				ArrayList<Long> temp = new ArrayList<Long>();
				for(int i=0; i< fields.length;++i)
					temp.add(Long.parseLong(fields[i].trim()));
				
				MapPatients.put(Short.parseShort(row[0]), temp);
			}
			break;	
			
		case 6:
			reader = new BufferedReader(new FileReader(tableName + ".dosage_mg.ndx"));
			MapDosage = new HashMap<Short, ArrayList<Long>>();
			line="";
			while((line = reader.readLine()) != null)
			{
				String row[] = line.split("::");
				
				String str = row[1];
				String str1 = str.replace("[", "");
				String str2 = str1.replace("]", "");
				
				String[] fields = str2.split(",");
				ArrayList<Long> temp = new ArrayList<Long>();
				for(int i=0; i< fields.length;++i)
					temp.add(Long.parseLong(fields[i].trim()));
				
				MapDosage.put(Short.parseShort(row[0]), temp);
			}
			break;
			
		case 7:
			reader = new BufferedReader(new FileReader(tableName + ".reading.ndx"));
			MapReading = new HashMap<Float, ArrayList<Long>>();
			line="";
			while((line = reader.readLine()) != null)
			{
				String row[] = line.split("::");
				
				String str = row[1];
				String str1 = str.replace("[", "");
				String str2 = str1.replace("]", "");
				
				String[] fields = str2.split(",");
				ArrayList<Long> temp = new ArrayList<Long>();
				for(int i=0; i< fields.length;++i)
					temp.add(Long.parseLong(fields[i].trim()));
				
				MapReading.put(Float.parseFloat(row[0]), temp);
			}
			break;
			
		case 8:
			reader = new BufferedReader(new FileReader(tableName + ".double_blind.ndx"));
			MapBlind = new HashMap<String, ArrayList<Long>>();
			line="";
			while((line = reader.readLine()) != null)
			{
				String row[] = line.split("::");
				
				String str = row[1];
				String str1 = str.replace("[", "");
				String str2 = str1.replace("]", "");
				
				String[] fields = str2.split(",");
				ArrayList<Long> temp = new ArrayList<Long>();
				for(int i=0; i< fields.length;++i)
					temp.add(Long.parseLong(fields[i].trim()));
				
				MapBlind.put(row[0], temp);
			}
			break;
			
		case 9:
			reader = new BufferedReader(new FileReader(tableName + ".controlled_study.ndx"));
			MapStudy = new HashMap<String, ArrayList<Long>>();
			line="";
			while((line = reader.readLine()) != null)
			{
				String row[] = line.split("::");
				
				String str = row[1];
				String str1 = str.replace("[", "");
				String str2 = str1.replace("]", "");
				
				String[] fields = str2.split(",");
				ArrayList<Long> temp = new ArrayList<Long>();
				for(int i=0; i< fields.length;++i)
					temp.add(Long.parseLong(fields[i].trim()));
				
				MapStudy.put(row[0], temp);
			}
			break;
			
		case 10:
			reader = new BufferedReader(new FileReader(tableName + ".govt_funded.ndx"));
			MapGovt = new HashMap<String, ArrayList<Long>>();
			line="";
			while((line = reader.readLine()) != null)
			{
				String row[] = line.split("::");
				
				String str = row[1];
				String str1 = str.replace("[", "");
				String str2 = str1.replace("]", "");
				
				String[] fields = str2.split(",");
				ArrayList<Long> temp = new ArrayList<Long>();
				for(int i=0; i< fields.length;++i)
					temp.add(Long.parseLong(fields[i].trim()));
				
				MapGovt.put(row[0], temp);
			}
			break;
			
		case 11:
			reader = new BufferedReader(new FileReader(tableName + ".fda_approved.ndx"));
			MapFda = new HashMap<String, ArrayList<Long>>();
			line="";
			while((line = reader.readLine()) != null)
			{
				String row[] = line.split("::");
				
				String str = row[1];
				String str1 = str.replace("[", "");
				String str2 = str1.replace("]", "");
				
				String[] fields = str2.split(",");
				ArrayList<Long> temp = new ArrayList<Long>();
				for(int i=0; i< fields.length;++i)
					temp.add(Long.parseLong(fields[i].trim()));
				
				MapFda.put(row[0], temp);
			}
			break;
		}
		reader.close();
	}

	public static void main(String[] args) throws FileNotFoundException, IOException
	{
		//readFromBinaryFile();		
		while(true)
		{			
			System.out.println("==========================================");
			System.out.println("The following operations are supported");
			System.out.println("Import");
			System.out.println("Query");
			System.out.println("Insert");
			System.out.println("Delete");
			System.out.println("==========================================");
		
			BufferedReader bufferReader = new BufferedReader(new InputStreamReader(System.in));
			String inputLine = bufferReader.readLine();
			int endIndex = inputLine.lastIndexOf(';');
			if(endIndex != -1)
				inputLine = inputLine.substring(0, endIndex).toString();
		
			if(inputLine.isEmpty())
			{
				System.out.println("Bye!!!!!");
				return;
			}
		
			String param[] = inputLine.split(" ");
			if(param[0].toLowerCase().startsWith("import"))
			{
				URL obj = MyDatabase.class.getResource(param[1].trim().toString());
				if(obj == null)
				{
					System.out.println("File " + param[1] + " not found");
					continue;
				}				
				String tableName = param[1].replace(".csv", "").toString();
						
				initMaps();
				indexFileSetUp(tableName);
				int ret = writeToBinaryFile(param[1]);
				if(ret == -1)
					continue;
				createIndexFiles();
				System.out.println("Import Operation successful");
				continue;
			}
		
			if(param[0].toLowerCase().startsWith("insert"))
			{
				int start = inputLine.lastIndexOf('(');
				int end = inputLine.lastIndexOf(')');
				
				File fp = new File(param[2].trim().toString() + ".db");
				if(!(fp.exists()))
				{
					System.out.println("Invalid table");
					continue;
				}
				RandomAccessFile file = new RandomAccessFile(param[2].trim().toString() + ".db", "rw");
				
				String values = "";
				if(start != -1 && end != -1)
					values = inputLine.substring(start+1, end).toString();
				else
				{
					System.out.println("Invalid command!!!!Values should be in valid braces");
					continue;
				}			

				//Update Map data structures from index file
				IndexFileReader(param[2].trim().toString(), 1);
				
				String temp[] = values.split(",");
				if(MapId.containsKey(Integer.parseInt(temp[0])))
				{
					System.out.println("Insert Failed!!!!Record with id " + temp[0] + " already exists");
					continue;
				}
				
				IndexFileReader(param[2].trim().toString(), 2);
				IndexFileReader(param[2].trim().toString(), 3);
				IndexFileReader(param[2].trim().toString(), 4);
				IndexFileReader(param[2].trim().toString(), 5);
				IndexFileReader(param[2].trim().toString(), 6);
				IndexFileReader(param[2].trim().toString(), 7);
				IndexFileReader(param[2].trim().toString(), 8);
				IndexFileReader(param[2].trim().toString(), 9);
				IndexFileReader(param[2].trim().toString(), 10);
				IndexFileReader(param[2].trim().toString(), 11);				
				
				file.seek(file.length());
				writeLineAsBinary(values, file, true);				
				indexFileSetUp(param[2]);
				createIndexFiles();
				file.close();
				System.out.println("Insert Operation successful");
				continue;
			}
			
			if(param[0].toLowerCase().startsWith("select"))
			{
				int len = param.length;
				boolean notFlag = false;
				String op = "";
				String value = "";
				String value2 = "";				
				
				File fp = new File(param[3].trim().toString() + ".db");
				if(!(fp.exists()))
				{
					System.out.println("Invalid table");
					continue;
				}
				
				if(param[6].equals("NOT"))
					notFlag = true;				
				
				if(notFlag)
				{					
					value = param[8];
					if(param[7].contentEquals("="))
						op = "!=";
					if(param[7].contentEquals(">"))
						op = "<=";
					if(param[7].contentEquals("<"))
						op = ">=";
					if(param[7].contentEquals(">="))
						op = "<";
					if(param[7].contentEquals("<="))
						op = ">";
					
					for(int i = 8 ; i<len; ++i)
					{
						value2 += param[i] ;
						if(i < len-1)
							value2 += " ";
					}
				}
				else
				{
					op = param[6];
					value = param[7];
					
					for(int i = 7 ; i<len; ++i)
					{
						value2 += param[i] ;
						if(i < len-1)
							value2 += " ";
					}
				}
				String field = param[5].trim();
				int index = getIndex(field);
				if(index == -1)
				{
					System.out.println("Operation Failed!!!! Attribute " + field + " not found");
					continue;
				}
				IndexFileReader(param[3].trim().toString(),index);
				
				if(index == 1)
				{
					int count = queryId(param, op, value, param[3].trim().toString());									
					System.out.println(count + " rows found");
					continue;
				}
				else if(index == 2)
				{
					int count = queryComp(param, op, value2, param[3].trim().toString());
					System.out.println(count + " rows found");
					continue;
				}
				else if(index == 3)
				{
					int count = queryDrug(param, op, value, param[3].trim().toString());
					System.out.println(count + " rows found");
					continue;
				}
				else if(index == 4)
				{					
					int count = queryTrials(param, op, value, param[3].trim().toString());
					System.out.println(count + " rows found");
					continue;
				}
				else if(index == 5)
				{
					int count = queryPatients(param, op, value, param[3].trim().toString());
					System.out.println(count + " rows found");
					continue;
				}
				else if(index == 6)
				{
					int count = queryDosage(param, op, value, param[3].trim().toString());
					System.out.println(count + " rows found");
					continue;
				}
				else if(index == 7)
				{
					int count = queryReading(param, op, value, param[3].trim().toString());
					System.out.println(count + " rows found");
					continue;
				}
				else if(index == 8)
				{
					int count = queryBlind(param, op, value, param[3].trim().toString());
					if(count != -1)
						System.out.println(count + " rows found");
					continue;
				}
				else if(index == 9)
				{
					int count = queryStudy(param, op, value, param[3].trim().toString());
					if(count != -1)
						System.out.println(count + " rows found");
					continue;
				}
				else if(index == 10)
				{
					int count = queryGovt(param, op, value, param[3].trim().toString());
					if(count != -1)
						System.out.println(count + " rows found");
					continue;
				}
				else if(index == 11)
				{
					int count = queryFda(param, op, value, param[3].trim().toString());
					if(count != -1)
						System.out.println(count + " rows found");
					continue;
				}
			}
			
			if(param[0].toLowerCase().startsWith("delete"))
			{
				int len = param.length;
				boolean notFlag = false;
				String op = "";
				String value = "";
				String value2 = "";				
				
				File fp = new File(param[2].trim().toString() + ".db");
				if(!(fp.exists()))
				{
					System.out.println("Invalid table");
					continue;
				}
				
				if(param[5].equals("NOT"))
					notFlag = true;				
				
				if(notFlag)
				{					
					value = param[7];
					if(param[6].contentEquals("="))
						op = "!=";
					if(param[6].contentEquals(">"))
						op = "<=";
					if(param[6].contentEquals("<"))
						op = ">=";
					if(param[6].contentEquals(">="))
						op = "<";
					if(param[6].contentEquals("<="))
						op = ">";
					
					for(int i = 7 ; i<len; ++i)
					{
						value2 += param[i] ;
						if(i < len-1)
							value2 += " ";
					}
				}
				else
				{
					op = param[5];
					value = param[6];
					
					for(int i = 6 ; i<len; ++i)
					{
						value2 += param[i] ;
						if(i < len-1)
							value2 += " ";
					}
				}
				String field = param[4].trim();
				int index = getIndex(field);
				if(index == -1)
				{
					System.out.println("Operation Failed!!!! Attribute " + field + " not found");
					continue;
				}
				IndexFileReader(param[2].trim().toString(),index);
				
				if(index == 1)
				{
					int count = deleteId(param, op, value, param[2].trim().toString());									
					System.out.println(count + " records deleted");
					continue;
				}
				
				if(index == 2)
				{
					int count = deleteComp(param, op, value2, param[2].trim().toString());									
					System.out.println(count + " records deleted");
					continue;
				}
				
				if(index == 3)
				{
					int count = deleteDrug(param, op, value, param[2].trim().toString());									
					System.out.println(count + " records deleted");
					continue;
				}
				
				if(index == 4)
				{
					int count = deleteTrials(param, op, value, param[2].trim().toString());									
					System.out.println(count + " records deleted");
					continue;
				}
				
				if(index == 5)
				{
					int count = deletePatients(param, op, value, param[2].trim().toString());									
					System.out.println(count + " records deleted");
					continue;
				}
				
				if(index == 6)
				{
					int count = deleteDosage(param, op, value, param[2].trim().toString());									
					System.out.println(count + " records deleted");
					continue;
				}
				
				if(index == 7)
				{
					int count = deleteReading(param, op, value, param[2].trim().toString());									
					System.out.println(count + " records deleted");
					continue;
				}
				
				if(index == 8)
				{
					int count = deleteBlind(param, op, value, param[2].trim().toString());									
					System.out.println(count + " records deleted");
					continue;
				}
				
				if(index == 9)
				{
					int count = deleteStudy(param, op, value, param[2].trim().toString());									
					System.out.println(count + " records deleted");
					continue;
				}
				
				if(index == 10)
				{
					int count = deleteGovt(param, op, value, param[2].trim().toString());									
					System.out.println(count + " records deleted");
					continue;
				}
				
				if(index == 11)
				{
					int count = deleteFda(param, op, value, param[2].trim().toString());									
					System.out.println(count + " records deleted");
					continue;
				}
			}
		}		
	}	

	private static int processDelete(String op, String value, String tableName, int count, String key,
			ArrayList<Long> recaddr) throws IOException {
		count = processDeleteStr(op, value, tableName, count, key, recaddr);
		return count;
	}
	
	private static int deleteFda(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(String key : MapFda.keySet())
		{
			ArrayList<Long> recaddr = MapFda.get(key);
			count = processDelete(op, value, tableName, count, key, recaddr);
		}
		return count;
	}
	
	private static int deleteGovt(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(String key : MapGovt.keySet())
		{
			ArrayList<Long> recaddr = MapGovt.get(key);
			count = processDelete(op, value, tableName, count, key, recaddr);
		}
		return count;
	}
	
	private static int deleteStudy(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(String key : MapStudy.keySet())
		{
			ArrayList<Long> recaddr = MapStudy.get(key);
			count = processDelete(op, value, tableName, count, key, recaddr);
		}
		return count;
	}
	
	private static int deleteBlind(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(String key : MapBlind.keySet())
		{
			ArrayList<Long> recaddr = MapBlind.get(key);
			count = processDelete(op, value, tableName, count, key, recaddr);
		}
		return count;
	}
	
	private static int deleteReading(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(Float key : MapReading.keySet())
		{						
			if(op.equals("="))
			{
				if(key == Float.parseFloat(value))
				{
					ArrayList<Long> recaddr = MapReading.get(key);
					
					for(Long addr : recaddr)
					{
						int present = SetDeleteBit(addr, tableName);
						if(present != -1)
						{
							count++;
						}
					}										
				}
			}
			
			if(op.equals("!="))
			{
				if(key != Float.parseFloat(value))
				{
					ArrayList<Long> recaddr = MapReading.get(key);
					for(Long addr : recaddr)
					{
						int present = SetDeleteBit(addr, tableName);					
						if(present != -1)
						{
							count++;
						}
					}
				}
			}				
			
			if(op.contentEquals(">"))
			{
				if(key > Float.parseFloat(value))
				{
					ArrayList<Long> recaddr = MapReading.get(key);
					for(Long addr : recaddr)
					{
						int present = SetDeleteBit(addr, tableName);					
						if(present != -1)
						{
							count++;
						}
					}
				}
			}
			
			if(op.contentEquals("<"))
			{
				if(key < Float.parseFloat(value))
				{
					ArrayList<Long> recaddr = MapReading.get(key);
					for(Long addr : recaddr)
					{
						int present = SetDeleteBit(addr, tableName);					
						if(present != -1)
						{
							count++;
						}
					}
				}
			}
			
			if(op.contentEquals("<="))
			{
				if(key <= Float.parseFloat(value))
				{
					ArrayList<Long> recaddr = MapReading.get(key);
					for(Long addr : recaddr)
					{
						int present = SetDeleteBit(addr, tableName);					
						if(present != -1)
						{
							count++;
						}
					}
				}
			}
			
			if(op.contentEquals(">="))
			{
				if(key >= Float.parseFloat(value))
				{
					ArrayList<Long> recaddr = MapReading.get(key);
					for(Long addr : recaddr)
					{
						int present = SetDeleteBit(addr, tableName);					
						if(present != -1)
						{
							count++;
						}
					}							
				}
			}
		}
		return count;
	}	

	private static int processDeleteShort(String op, String value, String tableName, int count, Short key,
			ArrayList<Long> recaddr) throws IOException {
		if(op.equals("="))
		{
			if(key == Short.parseShort(value))
			{					
				for(Long addr : recaddr)
				{
					int present = SetDeleteBit(addr, tableName);
					if(present != -1)
					{
						count++;
					}
				}										
			}
		}
		
		if(op.equals("!="))
		{
			if(key != Short.parseShort(value))
			{					
				for(Long addr : recaddr)
				{
					int present = SetDeleteBit(addr, tableName);					
					if(present != -1)
					{
						count++;
					}
				}
			}
		}				
		
		if(op.contentEquals(">"))
		{
			if(key > Short.parseShort(value))
			{				
				for(Long addr : recaddr)
				{
					int present = SetDeleteBit(addr, tableName);					
					if(present != -1)
					{
						count++;
					}
				}
			}
		}
		
		if(op.contentEquals("<"))
		{
			if(key < Short.parseShort(value))
			{					
				for(Long addr : recaddr)
				{
					int present = SetDeleteBit(addr, tableName);					
					if(present != -1)
					{
						count++;
					}
				}
			}
		}
		
		if(op.contentEquals("<="))
		{
			if(key <= Short.parseShort(value))
			{					
				for(Long addr : recaddr)
				{
					int present = SetDeleteBit(addr, tableName);					
					if(present != -1)
					{
						count++;
					}
				}
			}
		}
		
		if(op.contentEquals(">="))
		{
			if(key >= Short.parseShort(value))
			{					
				for(Long addr : recaddr)
				{
					int present = SetDeleteBit(addr, tableName);					
					if(present != -1)
					{
						count++;
					}
				}							
			}
		}
		return count;
	}
	
	private static int deleteDosage(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(Short key : MapDosage.keySet())
		{
			ArrayList<Long> recaddr = MapDosage.get(key);
			count = processDeleteShort(op, value, tableName, count, key, recaddr);
		}
		return count;
	}
	
	private static int deletePatients(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(Short key : MapPatients.keySet())
		{
			ArrayList<Long> recaddr = MapPatients.get(key);
			count = processDeleteShort(op, value, tableName, count, key, recaddr);
		}
		return count;
	}
	
	private static int deleteTrials(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(Short key : MapTrials.keySet())
		{
			ArrayList<Long> recaddr = MapTrials.get(key);
			count = processDeleteShort(op, value, tableName, count, key, recaddr);
		}
		return count;
	}

	private static int processDeleteStr(String op, String value, String tableName, int count, String key,
			ArrayList<Long> recaddr) throws IOException {
		if(op.equals("="))
		{
			if(value.equals(key))
			{					
				for(Long addr : recaddr)
				{
					int present = SetDeleteBit(addr, tableName);
					if(present != -1)
					{
						count++;
					}
				}										
			}
		}
		
		if(op.equals("!="))
		{
			if(!(value.equals(key)))
			{					
				for(Long addr : recaddr)
				{
					int present = SetDeleteBit(addr, tableName);					
					if(present != -1)
					{
						count++;
					}
				}
			}
		}
			
		int result = key.compareTo(value);
		if(op.contentEquals(">"))
		{
			if(result > 0)
			{					
				for(Long addr : recaddr)
				{
					int present = SetDeleteBit(addr, tableName);					
					if(present != -1)
					{
						count++;
					}
				}
			}
		}
		
		if(op.contentEquals("<"))
		{
			if(result < 0)
			{					
				for(Long addr : recaddr)
				{
					int present = SetDeleteBit(addr, tableName);					
					if(present != -1)
					{
						count++;
					}
				}
			}
		}
		
		if(op.contentEquals("<="))
		{
			if(result < 0 || result == 0)
			{					
				for(Long addr : recaddr)
				{
					int present = SetDeleteBit(addr, tableName);					
					if(present != -1)
					{
						count++;
					}
				}
			}
		}
		
		if(op.contentEquals(">="))
		{
			if(result > 0 || result == 0)
			{					
				for(Long addr : recaddr)
				{
					int present = SetDeleteBit(addr, tableName);					
					if(present != -1)
					{
						count++;
					}
				}							
			}
		}
		return count;
	}
	
	private static int deleteComp(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(String key : MapComp.keySet())
		{
			ArrayList<Long> recaddr = MapComp.get(key);
			count = processDeleteStr(op, value, tableName, count, key, recaddr);
		}
		return count;
	}
	
	private static int deleteDrug(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(String key : MapDid.keySet())
		{
			ArrayList<Long> recaddr = MapDid.get(key);
			count = processDeleteStr(op, value, tableName, count, key, recaddr);
		}
		return count;
	}	
	
	private static int deleteId(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(int key : MapId.keySet())
		{						
			if(op.equals("="))
			{
				if(key == Integer.parseInt(value))
				{
					Long recaddr = MapId.get(key);
					int present = SetDeleteBit(recaddr, tableName);
					if(present != -1)
					{
						count++;
					}
				}
			}
			
			if(op.equals("!="))
			{
				if(key != Integer.parseInt(value))
				{
					Long recaddr = MapId.get(key);
					int present = SetDeleteBit(recaddr, tableName);
					if(present != -1)
					{
						count++;
					}
				}
			}
			
			if(op.contentEquals(">"))
			{
				if(key > Integer.parseInt(value))
				{
					Long recaddr = MapId.get(key);
					int present = SetDeleteBit(recaddr, tableName);
					if(present != -1)
					{
						count++;
					}
				}
			}
			
			if(op.contentEquals("<"))
			{
				if(key < Integer.parseInt(value))
				{
					Long recaddr = MapId.get(key);
					int present = SetDeleteBit(recaddr, tableName);
					if(present != -1)
					{
						count++;
					}
				}
			}
			
			if(op.contentEquals("<="))
			{
				if(key <= Integer.parseInt(value))
				{
					Long recaddr = MapId.get(key);
					int present = SetDeleteBit(recaddr, tableName);
					if(present != -1)
					{
						count++;
					}
				}
			}
			
			if(op.contentEquals(">="))
			{
				if(key >= Integer.parseInt(value))
				{
					Long recaddr = MapId.get(key);
					int present = SetDeleteBit(recaddr, tableName);
					if(present != -1)
					{
						count++;
					}
				}							
			}
		}
		return count;
	}	

	private static int processQuery(String[] param, String op, String value, String tableName, int count, String key,
			ArrayList<Long> recaddr) throws IOException {
		if(op.equals("="))
		{
			if(value.equals(key))
			{				
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
		}
		
		if(op.equals("!="))
		{
			if(!(value.equals(key)))
			{					
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
		}
		return count;
	}
	
	private static boolean IsOperInValid(String op)
	{
		if(op.contentEquals(">"))
		{
			System.out.println("Invalid Operator!!!Greater/Lesser than operator is not valid for boolean attribute");
			return true;
		}
		
		if(op.contentEquals("<"))
		{
			System.out.println("Invalid Operator!!!Greater/Lesser than operator is not valid for boolean attribute");
			return true;
		}
		
		if(op.contentEquals("<="))
		{
			System.out.println("Invalid Operator!!!Greater/Lesser than operator is not valid for boolean attribute");
			return true;
		}
		
		if(op.contentEquals(">="))
		{
			System.out.println("Invalid Operator!!!Greater/Lesser than operator is not valid for boolean attribute");
			return true;
		}
		return false;
	}
	
	private static int queryFda(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		if(IsOperInValid(op))
			return -1;
		for(String key : MapFda.keySet())
		{
			ArrayList<Long> recaddr = MapFda.get(key);
			count = processQuery(param, op, value, tableName, count, key, recaddr);			
		}
		return count;
	}
	
	private static int queryGovt(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		if(IsOperInValid(op))
			return -1;
		for(String key : MapGovt.keySet())
		{
			ArrayList<Long> recaddr = MapGovt.get(key);
			count = processQuery(param, op, value, tableName, count, key, recaddr);			
		}
		return count;
	}
	
	private static int queryStudy(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		if(IsOperInValid(op))
			return -1;
		for(String key : MapStudy.keySet())
		{
			ArrayList<Long> recaddr = MapStudy.get(key);
			count = processQuery(param, op, value, tableName, count, key, recaddr);			
		}
		return count;
	}
	
	private static int queryBlind(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		if(IsOperInValid(op))
			return -1;
		for(String key : MapBlind.keySet())
		{
			ArrayList<Long> recaddr = MapBlind.get(key);
			count = processQuery(param, op, value, tableName, count, key, recaddr);			
		}
		return count;
	}	
	
	private static int queryReading(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(Float key : MapReading.keySet())
		{			
			if(op.equals("="))
			{
				if(key == Float.parseFloat(value))
				{
					ArrayList<Long> recaddr = MapReading.get(key);
					if(param[1].contentEquals("*"))
					{
						for(Long addr : recaddr)
						{
							if(IsPresent(addr, tableName))
							{
								getRecord(addr, tableName);
								count++;
							}
						}
					}
					else
					{
						String[] attrs = param[1].split(",");
						for(Long addr : recaddr)
						{
							if(IsPresent(addr, tableName))
							{
								int cnt = 0;
								for(String attr : attrs)
								{
									cnt++;
									PrintAttr(attr, addr, tableName);
									if(cnt < attrs.length)
										System.out.print(",");
								}
								System.out.println();
								count++;
							}
						}
					}
				}
			}
			
			if(op.equals("!="))
			{
				if(key != Float.parseFloat(value))
				{
					ArrayList<Long> recaddr = MapReading.get(key);
					if(param[1].contentEquals("*"))
					{
						for(Long addr : recaddr)
						{
							if(IsPresent(addr, tableName))
							{
								getRecord(addr, tableName);
								count++;
							}
						}
					}
					else
					{
						String[] attrs = param[1].split(",");
						for(Long addr : recaddr)
						{
							if(IsPresent(addr, tableName))
							{
								int cnt = 0;
								for(String attr : attrs)
								{
									cnt++;
									PrintAttr(attr, addr, tableName);
									if(cnt < attrs.length)
										System.out.print(",");
								}
								System.out.println();
								count++;
							}
						}
					}
				}
			}				
			
			if(op.contentEquals(">"))
			{
				if(key > Float.parseFloat(value))
				{
					ArrayList<Long> recaddr = MapReading.get(key);
					if(param[1].contentEquals("*"))
					{
						for(Long addr : recaddr)
						{
							if(IsPresent(addr, tableName))
							{
								getRecord(addr, tableName);
								count++;
							}
						}
					}
					else
					{
						String[] attrs = param[1].split(",");
						for(Long addr : recaddr)
						{
							if(IsPresent(addr, tableName))
							{
								int cnt = 0;
								for(String attr : attrs)
								{
									cnt++;
									PrintAttr(attr, addr, tableName);
									if(cnt < attrs.length)
										System.out.print(",");
								}
								System.out.println();
								count++;
							}
						}
					}
				}
			}
			
			if(op.contentEquals("<"))
			{
				if(key < Float.parseFloat(value))
				{
					ArrayList<Long> recaddr = MapReading.get(key);
					if(param[1].contentEquals("*"))
					{
						for(Long addr : recaddr)
						{
							if(IsPresent(addr, tableName))
							{
								getRecord(addr, tableName);
								count++;
							}
						}
					}
					else
					{
						String[] attrs = param[1].split(",");
						for(Long addr : recaddr)
						{
							if(IsPresent(addr, tableName))
							{
								int cnt = 0;
								for(String attr : attrs)
								{
									cnt++;
									PrintAttr(attr, addr, tableName);
									if(cnt < attrs.length)
										System.out.print(",");
								}
								System.out.println();
								count++;
							}
						}
					}
				}
			}
			
			if(op.contentEquals("<="))
			{
				if(key <= Float.parseFloat(value))
				{
					ArrayList<Long> recaddr = MapReading.get(key);
					if(param[1].contentEquals("*"))
					{
						for(Long addr : recaddr)
						{
							if(IsPresent(addr, tableName))
							{
								getRecord(addr, tableName);
								count++;
							}
						}
					}
					else
					{
						String[] attrs = param[1].split(",");
						for(Long addr : recaddr)
						{
							if(IsPresent(addr, tableName))
							{
								int cnt = 0;
								for(String attr : attrs)
								{
									cnt++;
									PrintAttr(attr, addr, tableName);
									if(cnt < attrs.length)
										System.out.print(",");
								}
								System.out.println();
								count++;
							}
						}
					}
				}
			}
			
			if(op.contentEquals(">="))
			{
				if(key >= Float.parseFloat(value))
				{
					ArrayList<Long> recaddr = MapReading.get(key);
					if(param[1].contentEquals("*"))
					{
						for(Long addr : recaddr)
						{
							if(IsPresent(addr, tableName))
							{
								getRecord(addr, tableName);
								count++;
							}
						}
					}
					else
					{
						String[] attrs = param[1].split(",");
						for(Long addr : recaddr)
						{
							if(IsPresent(addr, tableName))
							{
								int cnt = 0;
								for(String attr : attrs)
								{
									cnt++;
									PrintAttr(attr, addr, tableName);
									if(cnt < attrs.length)
										System.out.print(",");
								}
								System.out.println();
								count++;
							}
						}
					}							
				}
			}
		}
		return count;
	}	

	private static int processQueryShort(String[] param, String op, String value, String tableName, int count,
			Short key, ArrayList<Long> recaddr) throws IOException {
		if(op.equals("="))
		{
			if(key == Short.parseShort(value))
			{					
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
		}
		
		if(op.equals("!="))
		{
			if(key != Short.parseShort(value))
			{					
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
		}				
		
		if(op.contentEquals(">"))
		{
			if(key > Short.parseShort(value))
			{					
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
		}
		
		if(op.contentEquals("<"))
		{
			if(key < Short.parseShort(value))
			{					
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
		}
		
		if(op.contentEquals("<="))
		{
			if(key <= Short.parseShort(value))
			{					
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
		}
		
		if(op.contentEquals(">="))
		{
			if(key >= Short.parseShort(value))
			{					
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}							
			}
		}
		return count;
	}
	
	private static int queryDosage(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(Short key : MapDosage.keySet())
		{
			ArrayList<Long> recaddr = MapDosage.get(key);
			count = processQueryShort(param, op, value, tableName, count, key, recaddr);
		}
		return count;
	}
	
	private static int queryTrials(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(Short key : MapTrials.keySet())
		{
			ArrayList<Long> recaddr = MapTrials.get(key);
			count = processQueryShort(param, op, value, tableName, count, key, recaddr);
		}
		return count;
	}
	
	private static int queryPatients(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(Short key : MapPatients.keySet())
		{
			ArrayList<Long> recaddr = MapPatients.get(key);
			count = processQueryShort(param, op, value, tableName, count, key, recaddr);
		}
		return count;
	}

	private static int processQueryStr(String[] param, String op, String value, String tableName, int count, String key,
			ArrayList<Long> recaddr) throws IOException {
		if(op.equals("="))
		{
			if(value.equals(key))
			{					
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
		}
		
		if(op.equals("!="))
		{
			if(!(value.equals(key)))
			{					
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
		}
			
		int result = key.compareTo(value);
		if(op.contentEquals(">"))
		{
			if(result > 0)
			{					
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
		}
		
		if(op.contentEquals("<"))
		{
			if(result < 0)
			{					
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
		}
		
		if(op.contentEquals("<="))
		{
			if(result < 0 || result == 0)
			{					
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
		}
		
		if(op.contentEquals(">="))
		{
			if(result > 0 || result == 0)
			{					
				if(param[1].contentEquals("*"))
				{
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							getRecord(addr, tableName);
							count++;
						}
					}
				}
				else
				{
					String[] attrs = param[1].split(",");
					for(Long addr : recaddr)
					{
						if(IsPresent(addr, tableName))
						{
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, addr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}							
			}
		}
		return count;
	}
	
	private static int queryDrug(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(String key : MapDid.keySet())
		{
			ArrayList<Long> recaddr = MapDid.get(key);
			count = processQueryStr(param, op, value, tableName, count, key, recaddr);
		}
		return count;
	}
	
	private static int queryComp(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(String key : MapComp.keySet())
		{
			ArrayList<Long> recaddr = MapComp.get(key);
			count = processQueryStr(param, op, value, tableName, count, key, recaddr);
		}
		return count;
	}	

	private static int queryId(String[] param, String op, String value, String tableName) throws IOException {
		int count = 0;
		for(int key : MapId.keySet())
		{						
			if(op.equals("="))
			{
				if(key == Integer.parseInt(value))
				{
					Long recaddr = MapId.get(key);
					if(IsPresent(recaddr, tableName))
					{
						if(param[1].contentEquals("*"))
						{						
							getRecord(recaddr, tableName);
							count++;
							return count;
						}
					
						else
						{
							String[] attrs = param[1].split(",");
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, recaddr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
							return count;
						}
					}
				}
			}
			
			if(op.equals("!="))
			{
				if(key != Integer.parseInt(value))
				{
					Long recaddr = MapId.get(key);
					if(IsPresent(recaddr, tableName))
					{
						if(param[1].contentEquals("*"))
						{
							getRecord(recaddr, tableName);
							count++;
						}
						else
						{
							String[] attrs = param[1].split(",");
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, recaddr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
			
			if(op.contentEquals(">"))
			{
				if(key > Integer.parseInt(value))
				{
					Long recaddr = MapId.get(key);
					if(IsPresent(recaddr, tableName))
					{
						if(param[1].contentEquals("*"))
						{
							getRecord(recaddr, tableName);
							count++;
						}
						else
						{
							String[] attrs = param[1].split(",");
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, recaddr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
			
			if(op.contentEquals("<"))
			{
				if(key < Integer.parseInt(value))
				{
					Long recaddr = MapId.get(key);
					if(IsPresent(recaddr, tableName))
					{
						if(param[1].contentEquals("*"))
						{
							getRecord(recaddr, tableName);
							count++;
						}
						else
						{
							String[] attrs = param[1].split(",");
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, recaddr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
			
			if(op.contentEquals("<="))
			{
				if(key <= Integer.parseInt(value))
				{
					Long recaddr = MapId.get(key);
					if(IsPresent(recaddr, tableName))
					{
						if(param[1].contentEquals("*"))
						{
							getRecord(recaddr, tableName);
							count++;
						}
						else
						{
							String[] attrs = param[1].split(",");
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, recaddr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}
			}
			
			if(op.contentEquals(">="))
			{
				if(key >= Integer.parseInt(value))
				{
					Long recaddr = MapId.get(key);
					if(IsPresent(recaddr, tableName))
					{
						if(param[1].contentEquals("*"))
						{
							getRecord(recaddr, tableName);
							count++;
						}
						else
						{
							String[] attrs = param[1].split(",");
							int cnt = 0;
							for(String attr : attrs)
							{
								cnt++;
								PrintAttr(attr, recaddr, tableName);
								if(cnt < attrs.length)
									System.out.print(",");
							}
							System.out.println();
							count++;
						}
					}
				}							
			}
		}
		return count;
	}

	private static void createIndexFiles() {
		
		//Sorting of index entries
		treeMapId = new TreeMap<Integer, Long>(MapId);
		treeMapComp = new TreeMap<String, ArrayList<Long>>(MapComp);
		treeMapDid = new TreeMap<String, ArrayList<Long>>(MapDid);
		treeMapTrials = new TreeMap<Short, ArrayList<Long>>(MapTrials);
		treeMapPatients = new TreeMap<Short, ArrayList<Long>>(MapPatients);
		treeMapDosage = new TreeMap<Short, ArrayList<Long>>(MapDosage);
		treeMapReading = new TreeMap<Float, ArrayList<Long>>(MapReading);
		treeMapBlind = new TreeMap<String, ArrayList<Long>>(MapBlind);
		treeMapStudy = new TreeMap<String, ArrayList<Long>>(MapStudy);
		treeMapGovt = new TreeMap<String, ArrayList<Long>>(MapGovt);
		treeMapFda = new TreeMap<String, ArrayList<Long>>(MapFda);		
				
		//write the index file after sorting
		writeIntIndexFile(treeMapId, fileId);
		writeStringIndexFile(treeMapComp, fileCompany);
		writeStringIndexFile(treeMapDid, fileDid);
		writeShortIndexFile(treeMapTrials, fileTrials);
		writeShortIndexFile(treeMapPatients, filePatients);
		writeShortIndexFile(treeMapDosage, fileDosage);
		writeFloatIndexFile(treeMapReading, fileReading);
		
		writeStringIndexFile(treeMapBlind, fileBlind);
		writeStringIndexFile(treeMapStudy, fileStudy);
		writeStringIndexFile(treeMapGovt, fileGovt);
		writeStringIndexFile(treeMapFda, fileFda);
	}

	private static void indexFileSetUp(String tableName) throws IOException {	
		
		//Create files for all the index entries only for import command		
		
		fileId = new PrintWriter(tableName + ".id.ndx", "UTF-8");		
		fileCompany = new PrintWriter(tableName + ".company.ndx", "UTF-8");
		fileDid = new PrintWriter(tableName + ".drug_id.ndx", "UTF-8");
		fileTrials = new PrintWriter(tableName + ".trials.ndx", "UTF-8");
		filePatients = new PrintWriter(tableName + ".patients.ndx", "UTF-8");
		fileDosage = new PrintWriter(tableName + ".dosage_mg.ndx", "UTF-8");
		fileReading = new PrintWriter(tableName + ".reading.ndx", "UTF-8");		
		fileBlind = new PrintWriter(tableName + ".double_blind.ndx", "UTF-8");
		fileStudy = new PrintWriter(tableName + ".controlled_study.ndx", "UTF-8");
		fileGovt = new PrintWriter(tableName + ".govt_funded.ndx", "UTF-8");
		fileFda = new PrintWriter(tableName + ".fda_approved.ndx", "UTF-8");				
	}
	
	private static void PrintAttr(String attr, Long address, String tableName) throws IOException
	{
		RandomAccessFile file = new RandomAccessFile(tableName + ".db", "r");
		
		switch(attr)
		{
			case "id":
				getId(address,file);
				break;
			case "company":
				getCompany(address,file);
				break;
			case "drug_id":
				getDrug(address,file);
				break;
			case "trials":
				getTrials(address,file);
				break;
			case "patients":
				getPatients(address,file);
				break;
			case "dosage_mg":
				getDosage(address,file);
				break;
			case "reading":
				getReading(address,file);
				break;
			case "double_blind":
				getBlind(address,file);
				break;
			case "controlled_study":
				getStudy(address,file);
				break;
			case "govt_funded":
				getGovt(address,file);
				break;
			case "fda_approved":
				getFda(address,file);
				break;
			default:
				break;
		}
		file.close();
	}
	
	private static void getId(Long address, RandomAccessFile file) throws IOException
	{
		file.seek(address);
		System.out.print(file.readInt());
	}
	
	private static void getCompany(Long address, RandomAccessFile file) throws IOException
	{
		file.seek(address);
		file.readInt();
		int len = file.read();				
		byte[] bytes = new byte[len];
		file.read(bytes);
		System.out.print(new String(bytes));
	}
	
	private static void getDrug(Long address, RandomAccessFile file) throws IOException
	{
		file.seek(address);
		file.readInt();
		int len = file.read();				
		byte[] bytes = new byte[len];
		file.read(bytes);
		bytes = new byte[6];
		file.read(bytes);
		System.out.print(new String(bytes));
	}
	
	private static void getTrials(Long address, RandomAccessFile file) throws IOException
	{
		file.seek(address);
		file.readInt();
		int len = file.read();				
		byte[] bytes = new byte[len];
		file.read(bytes);
		bytes = new byte[6];
		file.read(bytes);
		System.out.print(file.readShort());
	}
	
	private static void getPatients(Long address, RandomAccessFile file) throws IOException
	{
		file.seek(address);
		file.readInt();
		int len = file.read();				
		byte[] bytes = new byte[len];
		file.read(bytes);
		bytes = new byte[6];
		file.read(bytes);
		file.readShort();
		System.out.print(file.readShort());
	}
	
	private static void getDosage(Long address, RandomAccessFile file) throws IOException
	{
		file.seek(address);
		file.readInt();
		int len = file.read();				
		byte[] bytes = new byte[len];
		file.read(bytes);
		bytes = new byte[6];
		file.read(bytes);
		file.readShort();
		file.readShort();
		System.out.print(file.readShort());
	}
	
	private static void getReading(Long address, RandomAccessFile file) throws IOException
	{
		file.seek(address);
		file.readInt();
		int len = file.read();				
		byte[] bytes = new byte[len];
		file.read(bytes);
		bytes = new byte[6];
		file.read(bytes);
		file.readShort();
		file.readShort();
		file.readShort();
		System.out.print(file.readFloat());
	}
	
	private static void getBlind(Long address, RandomAccessFile file) throws IOException
	{
		String outp = getMask(address, file);
		String[] temp = outp.split(",");
		System.out.print(temp[0]);
	}
	
	private static void getStudy(Long address, RandomAccessFile file) throws IOException
	{
		String outp = getMask(address, file);
		String[] temp = outp.split(",");
		System.out.print(temp[1]);
	}
	
	private static void getGovt(Long address, RandomAccessFile file) throws IOException
	{
		String outp = getMask(address, file);
		String[] temp = outp.split(",");
		System.out.print(temp[2]);
	}
	
	private static void getFda(Long address, RandomAccessFile file) throws IOException
	{
		String outp = getMask(address, file);
		String[] temp = outp.split(",");
		System.out.print(temp[3]);
	}	
	
	private static boolean IsPresent(Long address, String tableName) throws IOException
	{
		RandomAccessFile file = new RandomAccessFile(tableName + ".db", "r");
		
		file.seek(address);
		file.readInt();
		int len = file.read();				
		byte[] bytes = new byte[len];
		file.read(bytes);
		bytes = new byte[6];
		file.read(bytes);
		file.readShort();
		file.readShort();
		file.readShort();
		file.readFloat();
		byte code = file.readByte();
		file.close();
		if((code & delete_mask) ==  delete_mask)		
			return true;		
		else
			return false;
	}
	
	private static int SetDeleteBit(Long address, String tableName) throws IOException
	{
		RandomAccessFile file = new RandomAccessFile(tableName + ".db", "rw");
		
		file.seek(address);
		file.readInt();
		int len = file.read();				
		byte[] bytes = new byte[len];
		file.read(bytes);		
		bytes = new byte[6];
		file.read(bytes);
		
		file.readShort();
		file.readShort();
		file.readShort();
		file.readFloat();
		byte code = file.readByte();
		int msk = DeleteMask(code);		
		
		if(msk == -1)
		{
			file.close();		
			return -1;
		}
	    
		else
		{
			file.seek(address);
			file.readInt();
			len = file.read();				
			bytes = new byte[len];
			file.read(bytes);
			bytes = new byte[6];
			file.read(bytes);
			file.readShort();
			file.readShort();
			file.readShort();
			file.readFloat();
			file.writeByte(msk);
			file.close();
			return 0;
		}		
	}
	
	private static String getMask(Long address, RandomAccessFile file) throws IOException
	{
		file.seek(address);
		file.readInt();
		int len = file.read();				
		byte[] bytes = new byte[len];
		file.read(bytes);
		bytes = new byte[6];
		file.read(bytes);
		file.readShort();
		file.readShort();
		file.readShort();
		file.readFloat();
		return UnMaskStr(file.readByte());
	}
	
	private static void getRecord(Long address, String tableName) throws IOException
	{
		RandomAccessFile file = new RandomAccessFile(tableName + ".db", "r");
		
		getId(address, file);
		System.out.print(",");
		getCompany(address, file);
		System.out.print(",");
		getDrug(address, file);
		System.out.print(",");
		getTrials(address, file);
		System.out.print(",");
		getPatients(address, file);
		System.out.print(",");
		getDosage(address, file);
		System.out.print(",");
		getReading(address, file);
		System.out.print(",");
		System.out.println(getMask(address, file));
		file.close();
	}
	
	/*private static void readFromBinaryFile() throws FileNotFoundException, UnsupportedEncodingException, IOException
	{
		RandomAccessFile file = new RandomAccessFile("PHARMA_TRIALS_1000B.db", "r");		
		
		file.seek(0);		
		while(file.getFilePointer() < file.length())
		{			
			for(int i=0;i<8; ++i)
			{
				switch(i)
				{
					case 0://int
						System.out.print( file.readInt() + " ");				
						break;
					case 1://var char				
						int len = file.read();				
						byte[] bytes = new byte[len];
						file.read(bytes);
						System.out.print(new String(bytes) + " ");
						
						break;
					case 2://char[]												
						bytes = new byte[6];
						file.read(bytes);
						System.out.print(new String(bytes) + " ");
						break;
					case 3://short int											
						System.out.print(file.readShort() + " ");	
						break;
					case 4://short int											
						System.out.print(file.readShort() + " ");
						break;
					case 5://short int					
						System.out.print(file.readShort() + " ");
						break;
					case 6://float											
						System.out.print(file.readFloat() + " ");
						break;
					case 7:					
						byte msk = file.readByte();
						String[] output = UnMask(msk);						
						break;					
				}
			}		
		}
		file.close();
	}*/
	
	private static void writeIntIndexFile(Map<Integer, Long> map, PrintWriter file)
	{
		
		for (Map.Entry<Integer, Long> entry : map.entrySet())		
			file.println(entry.getKey() + "::" + entry.getValue());		
		
		file.close();
	}
	
	private static void writeStringIndexFile(Map<String, ArrayList<Long>> map, PrintWriter file)
	{		
		Set<Entry<String, ArrayList<Long>>> setMap = map.entrySet();
		Iterator<Entry<String,  ArrayList<Long>>> iteratorMap = setMap.iterator();
		
		while(iteratorMap.hasNext()) 
		{
			Map.Entry<String, ArrayList<Long>> entry = (Map.Entry<String, ArrayList<Long>>) iteratorMap.next();
			String key = entry.getKey();
			List<Long> values = entry.getValue();
			file.println(key + "::" + values);			
		}
		file.close();
	}	
	
	
	private static void writeShortIndexFile(Map<Short, ArrayList<Long>> map, PrintWriter file)
	{		
		Set<Entry<Short, ArrayList<Long>>> setMap = map.entrySet();
		Iterator<Entry<Short,  ArrayList<Long>>> iteratorMap = setMap.iterator();		
		
		while(iteratorMap.hasNext()) 
		{
			Map.Entry<Short, ArrayList<Long>> entry = (Map.Entry<Short, ArrayList<Long>>) iteratorMap.next();
			Short key = entry.getKey();
			List<Long> values = entry.getValue();
			file.println(key + "::" + values);			
		}
		file.close();
	}	
	
	private static void writeFloatIndexFile(Map<Float, ArrayList<Long>> map, PrintWriter file)
	{		
		Set<Entry<Float, ArrayList<Long>>> setMap = map.entrySet();
		Iterator<Entry<Float,  ArrayList<Long>>> iteratorMap = setMap.iterator();		
				
		while(iteratorMap.hasNext()) 
		{
			Map.Entry<Float, ArrayList<Long>> entry = (Map.Entry<Float, ArrayList<Long>>) iteratorMap.next();
			Float key = entry.getKey();
			List<Long> values = entry.getValue();
			file.println(key + "::" + values);			
		}
		
		file.close();
	}

	private static int writeToBinaryFile(String filename) throws FileNotFoundException, UnsupportedEncodingException, IOException {
		BufferedReader br = null;
		String line = "";		
		
		//Read CSV file
		String filePath = MyDatabase.class.getResource(filename).getPath();		
		
		br = new BufferedReader(new FileReader(filePath));
		line = br.readLine(); //ignore first line as it contains header data
		
		String tableName = filename.replace(".csv", ".db").toString();
		
		RandomAccessFile file = new RandomAccessFile(tableName, "rw");		
		//file.seek(0);
		while ((line = br.readLine()) != null)
		{
			//Write each line of CSV file to binary file
			writeLineAsBinary(line, file, false);	       
		}		
		br.close();	
		file.close();
		return 0;
	}

	private static void writeLineAsBinary(String line, RandomAccessFile file, boolean append) throws IOException {
		String[] Fields = Tokenize(line);			
		
		Long recAddress = 0L;		
		
		for(int i=0;i<8; ++i)
		{
			switch(i)
			{
			case 0://int
				if(append)
					recAddress = file.length();
				else
					recAddress = file.getFilePointer();
				MapId.put(Integer.parseInt(Fields[i]), recAddress);
				
				file.writeInt(Integer.parseInt(Fields[i]));					
				break;
			case 1://var char
				int len = Fields[i].length();					
				if(Fields[1].startsWith("\""))
				{
					if(MapComp.containsKey(Fields[i].substring(1, (len-1)).toString()))
					{
						ArrayList<Long> listTemp = new ArrayList<Long>();
						listTemp = MapComp.get(Fields[i].substring(1, (len-1)).toString());
						listTemp.add(recAddress);
						MapComp.put(Fields[i].substring(1, (len-1)).toString(), listTemp);
					}
					else
					{
						ArrayList<Long> listTemp = new ArrayList<Long>();
						listTemp.add(recAddress);
						MapComp.put(Fields[i].substring(1, (len-1)).toString(), listTemp);
					}
					file.write(len-2);
					file.writeBytes(Fields[i].substring(1, (len-1)).toString());						
				}
				else
				{
					if(MapComp.containsKey(Fields[i]))
					{
						ArrayList<Long> listTemp = new ArrayList<Long>();
						listTemp = MapComp.get(Fields[i]);
						listTemp.add(recAddress);
						MapComp.put(Fields[i], listTemp);
					}
					else
					{
						ArrayList<Long> listTemp = new ArrayList<Long>();
						listTemp.add(recAddress);
						MapComp.put(Fields[i], listTemp);
					}
					file.write(len);
					file.writeBytes(Fields[i]);
				}							
				break;
			case 2://char[]					
				if(MapDid.containsKey(Fields[i]))
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp = MapDid.get(Fields[i]);
					listTemp.add(recAddress);
					MapDid.put(Fields[i], listTemp);
				}
				else
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp.add(recAddress);
					MapDid.put(Fields[i], listTemp);
				}
				file.writeBytes(Fields[i]);
				break;
			case 3://short int						
				if(MapTrials.containsKey(Short.parseShort(Fields[i])))
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp = MapTrials.get(Short.parseShort(Fields[i]));
					listTemp.add(recAddress);
					MapTrials.put(Short.parseShort(Fields[i]), listTemp);
				}
				else
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp.add(recAddress);
					MapTrials.put(Short.parseShort(Fields[i]), listTemp);
				}
				file.writeShort(Integer.parseInt(Fields[i]));
				break;
			case 4://short int					
				if(MapPatients.containsKey(Short.parseShort(Fields[i])))
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp = MapPatients.get(Short.parseShort(Fields[i]));
					listTemp.add(recAddress);
					MapPatients.put(Short.parseShort(Fields[i]), listTemp);
				}
				else
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp.add(recAddress);
					MapPatients.put(Short.parseShort(Fields[i]), listTemp);
				}
				file.writeShort(Integer.parseInt(Fields[i]));
				break;
			case 5://short int					
				if(MapDosage.containsKey(Short.parseShort(Fields[i])))
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp = MapDosage.get(Short.parseShort(Fields[i]));
					listTemp.add(recAddress);
					MapDosage.put(Short.parseShort(Fields[i]), listTemp);
				}
				else
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp.add(recAddress);
					MapDosage.put(Short.parseShort(Fields[i]), listTemp);
				}
				file.writeShort(Integer.parseInt(Fields[i]));
				break;
			case 6://float					
				if(MapReading.containsKey(Float.parseFloat(Fields[i])))
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp = MapReading.get(Float.parseFloat(Fields[i]));
					listTemp.add(recAddress);
					MapReading.put(Float.parseFloat(Fields[i]), listTemp);
				}
				else
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp.add(recAddress);
					MapReading.put(Float.parseFloat(Fields[i]), listTemp);
				}
				file.writeFloat(Float.parseFloat(Fields[i]));
				break;
			case 7:	
				if(MapBlind.containsKey(Fields[7]))
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp = MapBlind.get(Fields[7]);
					listTemp.add(recAddress);
					MapBlind.put(Fields[7], listTemp);					
				}
				else
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp.add(recAddress);
					MapBlind.put(Fields[7], listTemp);
				}
				
				if(MapStudy.containsKey(Fields[8]))
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp = MapStudy.get(Fields[8]);
					listTemp.add(recAddress);
					MapStudy.put(Fields[8], listTemp);
				}
				else
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp.add(recAddress);
					MapStudy.put(Fields[8], listTemp);
				}
				
				if(MapGovt.containsKey(Fields[9]))
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp = MapGovt.get(Fields[9]);
					listTemp.add(recAddress);
					MapGovt.put(Fields[9], listTemp);
				}
				else
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp.add(recAddress);
					MapGovt.put(Fields[9], listTemp);
				}
				
				if(MapFda.containsKey(Fields[10]))
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp = MapFda.get(Fields[10]);
					listTemp.add(recAddress);
					MapFda.put(Fields[10], listTemp);					
				}
				else
				{
					ArrayList<Long> listTemp = new ArrayList<Long>();
					listTemp.add(recAddress);
					MapFda.put(Fields[10], listTemp);					
				}
				file.writeByte(Mask(Fields[7], Fields[8], Fields[9], Fields[10]));								
				break;					
			}
		}
	}
}
