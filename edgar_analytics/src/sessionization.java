import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;
import com.opencsv.CSVReader;

public class sessionization {
	
	public static void main(String[] args) {
		sessionization test = new sessionization();
		test.edgarAnalytics();
	}
	
	final  String   period_file_path = "./input/inactivity_period.txt";
	final  String   edgar_file_path  = "./input/log.csv";
	final  String   output_file_path = "./output/sessionization.txt";
	
	// required field
	final  String[] FIELD_NAMES = {"ip", "date", "time"};
	final  int      FIELD_COUNT = FIELD_NAMES.length;
	final  int      IDX_IP   = 0;
	final  int      IDX_DATE = 1;
	final  int      IDX_TIME = 2;

	// For printing debugging message.
	final int PRINT_SRC = 1;
	int       verbose   = 0;

	SimpleDateFormat time_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	// Put this IP's: start time, latest time, rank and web counter into IpNode.
	// Use map to related IP address and IP node
	HashMap<String, IpNode> ip_info_map = new HashMap<>();
	

	//------------------------------------
	// IP address information node.
	private class IpNode{
		Date start_time;	// first access time
		Date latest_time;	// latest access time
		int  rank; 		    // identify which is in front, when start_time same.
		int  web_counter;	// web page request counter
		public IpNode(Date start_time, Date latest_time, int rank) {
			this.start_time = start_time;
			this.latest_time = latest_time;
			this.rank = rank;
			this.web_counter = 0;
		}
	}

	//---------------------------------------
	// Analyze given EDGAR file, 
	// output a summary into output file.
	public void edgarAnalytics(){
		final String func_name = new Object(){}.getClass().getEnclosingMethod().getName();
		
		// get inactive period from given file
		int inact_period = getInactPeriod(period_file_path);
		if(inact_period == -1) {
			return;
		}
		
		// open log file
		int[] field_indices = new int[FIELD_COUNT];;
		CSVReader csv_reader  = openLogFile(edgar_file_path, field_indices);
		if(csv_reader == null) {
			System.out.println(csv_reader);
			System.out.println(field_indices);
			
			my_print_error(func_name, "ERROR type: abnormal return from func openLogFile!");
			return;
		}
		
		// open output file
		BufferedWriter writer = openOutFile(output_file_path);
		if(writer == null) {
			return;
		}		
		
		// process log file
		analyzeLogFile(csv_reader, writer, field_indices, inact_period);
	}
	
	
	//------------------------------------
	// Get inactive period from given file.
	public int getInactPeriod(String file_path) {		
		final String func_name = new Object(){}.getClass().getEnclosingMethod().getName();
		try{
			BufferedReader reader = Files.newBufferedReader(Paths.get(file_path));
			String inact_period_str = reader.readLine();
			reader.close();
			
			int inact_period = 0;
			if(inact_period_str != null) {
				inact_period = Integer.valueOf(inact_period_str);
				if((verbose & PRINT_SRC) == 1) {
					System.out.println("Inactivity Period = " + inact_period + "s");
				}
				if(inact_period < 1 ) {
					my_print_warning(func_name, 
							"WARN type: Unsurported inactivity_period : " + inact_period + "! Use 1 instead!");
					return 1;
				} else if(inact_period > 86400){
					my_print_warning(func_name, "WARN type: Unsurported inactivity_period : "
				                                + inact_period + "! Will use this value anyway! ");
					return inact_period;					
				} else {
					return inact_period;
				}
			} else {
				my_print_error(func_name, "Error type     : inactivity_period.txt is EMPTY! ");
				return -1;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			my_print_error(func_name, "ERROR type     : "+file_path+" open ERROR.");
			e.printStackTrace();
			return -1;
		}
	}
	

	//---------------------------------------------
	// open Log file and check the required fields are there.
	// if missed, report error and quit.
	// if had duplicated field, take the last one.	
	public CSVReader openLogFile(String file_path, int[] field_indices){
		final String func_name = new Object(){}.getClass().getEnclosingMethod().getName();
		
		// open log file
		Reader reader = null;
		try {
			reader = Files.newBufferedReader(Paths.get(file_path));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			my_print_error(func_name, "ERROR type : " + file_path + " open error! ");
			e.printStackTrace();
			return null;
		}
		
		// parse in CSV format
		CSVReader csv_reader = new CSVReader(reader);
		String[] cvs_line = null;
		try {
			cvs_line = csv_reader.readNext();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			my_print_error(func_name, "ERROR type: CSV readNext ERROR ");
			e.printStackTrace();
			return null;
		}
		
		// check the required fields are there, and get the corresponding index.
		// if found duplicate required fields, report warning, take the last one.
		// [0]: ip, [1]: date, [2]: time
		Arrays.fill(field_indices, -1);
		
		int   how_many_fields_found = 0;
		if(cvs_line != null) {
			for(int field_index = 0; field_index < cvs_line.length; field_index++){
				for(int i = 0; i < FIELD_COUNT; i++) {
					if(cvs_line[field_index].equals(FIELD_NAMES[i])) {
						if(field_indices[i] != -1) {
							my_print_warning(func_name, 
									"WARN type     : Duplicated Field Name:" + FIELD_NAMES[i]);
						}
						field_indices[i] = field_index;
						how_many_fields_found ++;
					}
				}
			}
		}
		
		// check: no missing required fields.
		// if missing, report error and quit
		if(how_many_fields_found != FIELD_COUNT) {
			System.out.println("ERROR from func: - " + func_name);
			System.out.println("Error type     : Some Fields are MISSING");
			for(int i = 0; i < FIELD_COUNT; i++) {
				if(field_indices[i] == -1) {
					my_print_error(func_name, "Error missing field name : " + FIELD_NAMES[i]);
				}
			}
			return null;
		}
		return csv_reader;
	}

	//-----------------------------
	// Open output file
	public BufferedWriter openOutFile(String file_path){
		final String func_name = new Object(){}.getClass().getEnclosingMethod().getName();
		BufferedWriter writer = null;
		try {
			writer = Files.newBufferedWriter(Paths.get(file_path));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			my_print_error(func_name, "ERROR type     : " + file_path + " open ERROR.");
			e.printStackTrace();
		}
		return writer;
	}


	//------------------------------------
	// Analyze log file line by line
	//		1. put new record input Queue and Map
	//		2. Queue keep the active records, when
	//         not active for a period, move to file.
	//		3. if this ip is in Map, update latest acc time.
	//		4. When EOF, reorder the leftover according
	//         to the rule and move all leftover to file
	long line_counter = 0;
	public void analyzeLogFile(CSVReader csv_reader, 
			BufferedWriter writer, int[] field_indices, int inact_period){
		
		final String func_name = new Object(){}.getClass().getEnclosingMethod().getName();

		String[] csv_line = null;
		try {
			// Use queue to put the record of a time slot "inact_period"
			// From: cur_time - inact_period
			// To  : cur_time (the time of the latest record)
			// each record keep: IP, DATE+TIME
			Queue<String[]> records = new LinkedList<>();
			Date cur_time = time_format.parse("2000-01-01 00:00:00");
			Date pre_time = time_format.parse("2000-01-01 00:00:00");
			int  same_time_record_counter  = 0; // count the records with same time
			while((csv_line = csv_reader.readNext()) != null) {
				// Print content of current line. For debug
				if((verbose & PRINT_SRC) == 1) {
					line_counter ++;
					System.out.print(line_counter + ": ");
					for(String content : csv_line) {
						System.out.print(content + ",");
					}
					System.out.println("");
				}
				
				// Keep ip and data+time information.
				String[] cur_record = new String[2]; // 0: IP, 1: DATE_TIME
				cur_record[0] = new String(csv_line[field_indices[IDX_IP]]);
				cur_record[1] = new String(csv_line[field_indices[IDX_DATE]]
						+" " +csv_line[field_indices[IDX_TIME]]);
				records.add(cur_record);
				
				// counting the number of records with same time
				cur_time = time_format.parse(cur_record[1]);
				if(cur_time.compareTo(pre_time) == 0) {
					same_time_record_counter ++; // +1 for each new record with same time
				} else {
					// time changed, restart counter.
					same_time_record_counter = 0;
				}
				pre_time = cur_time;
				
				// process records becoming inactive.
				boolean stop = false;
				while(!stop && !records.isEmpty()){
					String[] first_record = records.peek();
					Date oldest_time = time_format.parse(first_record[1]);
					long time_interval_s = (cur_time.getTime() - oldest_time.getTime());
				
					if (time_interval_s > (inact_period * 1000)) {
						// inactive records move to file
						first_record = records.remove();
						if(ip_info_map.containsKey(first_record[0])) {
							Date latest_time = ip_info_map.get(first_record[0]).latest_time;
							if((cur_time.getTime()-latest_time.getTime()) > (inact_period * 1000)) {
								// this ip has not active over 'inact_period'. put into file
								String ip_address = first_record[0];
								putToFile(writer, ip_address, ip_info_map.get(ip_address));
								ip_info_map.remove(ip_address);
							}
						}
					} else {
						// all inactive records has been processed.
						stop = true;
					}
				}
				
				// Add cur_record into map.
				// new ip: add to map, set start time, latest time, and rank
				// old ip: update latest time, web page counter + 1
				if(ip_info_map.containsKey(cur_record[0])) {
					ip_info_map.get(cur_record[0]).latest_time = cur_time;							
				} else {
					ip_info_map.put(cur_record[0], 
							new IpNode(cur_time, cur_time, same_time_record_counter));
				}
				ip_info_map.get(cur_record[0]).web_counter++;
			}
			
			// EOF, reorder leftover, then move out to file
			if(!records.isEmpty()) {
				String [] reordered_leftover = reorderLeftOver(records);
				
				for(int i = 0; i < reordered_leftover.length; i++){
					String ip_address = reordered_leftover[i];
					if(ip_info_map.containsKey(ip_address)) {
						putToFile(writer, ip_address, ip_info_map.get(ip_address));
						ip_info_map.remove(ip_address);
					}
				}
			}
			writer.close();
			csv_reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			my_print_error(func_name, "ERROR type     : Log file read ERROR.");
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block		
			my_print_error(func_name, "ERROR type     : Could NOT parse the date. ");
			e.printStackTrace();
		}
	}
	//------------------------------------
	// end of file, dump left records:
	// Reorder leftover in sequence:
	//		1. put early-start record in front.
	//		2. same start time, first in first out.
	public String[] reorderLeftOver(Queue<String[]> records) {
		final int REMAIN_COUNT = records.size();
		String[] reordered = new String[REMAIN_COUNT];
		
		// early time first, same time, smaller rank first
		PriorityQueue<String> sorted_ip_addr = new PriorityQueue<>((o1, o2)->{
			if (ip_info_map.get(o1).start_time.compareTo(ip_info_map.get(o2).start_time) == 0) {
				return ip_info_map.get(o1).rank - ip_info_map.get(o2).rank;
			} else {
				return ip_info_map.get(o1).start_time.compareTo(ip_info_map.get(o2).start_time);
			}
		});
		
		while(!records.isEmpty()) {
			String[] record = records.remove();
			sorted_ip_addr.add(record[0]); // ip_address
		}
		
		for(int i = 0; i < REMAIN_COUNT; i++) {
			reordered[i] = sorted_ip_addr.remove();
		}
		return reordered;
	}
	
	
	//------------------------------------
	// Write output message to file.
	public void putToFile(BufferedWriter writer, String ip_addr, IpNode ip_node){
		// 1st field: ip
		StringBuilder out_line_sb = new StringBuilder(ip_addr + ",");
		
		// 2nd field: start time
		Date start_time = ip_node.start_time;
		out_line_sb.append(time_format.format(start_time) +",");
		
		// 3rd field: end time
		Date latest_time = ip_node.latest_time;
		out_line_sb.append(time_format.format(latest_time)+",");
		
		// 4th field: time elapse
		long time_elapse = (latest_time.getTime() - start_time.getTime())/1000 + 1;
		out_line_sb.append(time_elapse+",");
		
		// 5th field: number of webpages
		out_line_sb.append(ip_node.web_counter);
		
		try {
			writer.write(out_line_sb.toString()+"\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			final String func_name = new Object(){}.getClass().getEnclosingMethod().getName();
			my_print_error(func_name, "Error type: could NOT write to output file. ");
			e.printStackTrace();
		}
	}
	
	
	//------------------------------------
	// Print error message, indicate the error scenario.
	public void my_print_error(String func_name, String message){
		System.out.println("ERROR from func : - " + func_name);
		System.out.println(message);
	}
	
	//------------------------------------
	// Print warning message, indicate the error scenario.
	public void my_print_warning(String func_name, String message){
		System.out.println("WARN from func : - " + func_name);
		System.out.println(message);
	}
	
}
