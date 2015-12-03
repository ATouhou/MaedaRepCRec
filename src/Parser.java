import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {
	private String instructionfile = "";
	private int currentTimestamp = 0; 
	private TransactionManager tm;
	
	
	public Parser(String instructionfile, TransactionManager tm){
		this.instructionfile = instructionfile;
		this.tm = tm;
	}
	
	/*
	 * parse() parses each line from the input file and sends each command to TransactionManager.processOperation()
	 * Every line it reads will increment the current timestamp. 
	 */
	public void parse(){
		//Read next line in content; timestamp++
		try (BufferedReader br = new BufferedReader(new FileReader(instructionfile))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	//divide the line by semicolons
		    	String[] lineParts = line.split(";");
		    	for(String aPart: lineParts){
			    	System.out.println(aPart);

			    	// process the partial line.
			    	String[] commands = parseNextInstruction(aPart);
			    	if(commands!=null){
						
			    		//For each instruction in line: TransactionManager.processOperation()
			    		tm.processOperation(commands, currentTimestamp);
			    	}
		    	}
		    	//Every new line, time stamp increases
	    		currentTimestamp++;
		    }
		} catch (FileNotFoundException e) {
 			e.printStackTrace();
		} catch (IOException e) {
 			e.printStackTrace();
		}
	}
	/*
	 * @commandStr, e.g. W(T1,x1,101), a partial line from the input i.e. split by ;
	 * @returns an array of command details
	 * i.e. [write, write params ...] = ["W", "1", "1", "101"]
	 */
	public String[] parseNextInstruction(String commandStr){
		if(commandStr.startsWith("begin")){
			
			//Format: begin(T1)
			String pattern = "begin\\s*\\(\\s*T(\\d+)\\s*\\)";
			
			// Create a Pattern object
			Pattern r = Pattern.compile(pattern);

			// Now create matcher object.
 			Matcher m = r.matcher(commandStr);
			m.find();

			//Get the transaction
			return new String[]{"begin", m.group(1)};
			
		}else if(commandStr.startsWith("beginRO")){
			
			//Format: beginRO(T3)
			String pattern = "beginRO\\s*\\(\\s*T(\\d+)\\s*\\)";
			
			// Create a Pattern object
			Pattern r = Pattern.compile(pattern);
			
			// Now create matcher object.
			Matcher m = r.matcher(commandStr);
			m.find();

			//Get the transaction
			return new String[]{"beginRO", m.group(1)};
			
		}else if(commandStr.startsWith("R")){
			
			//Format: R(T1, x4)
			String pattern = "R\\s*\\(\\s*T(\\d+)\\s*,\\s*x(\\d+)\\s*\\)";
			
			// Create a Pattern object
			Pattern r = Pattern.compile(pattern);
			
			// Now create matcher object.
			Matcher m = r.matcher(commandStr);
			m.find();

			//Get the read information: instruction, transaction number, and index variable
			return new String[]{"R", m.group(1), m.group(2)};
			
		}else if(commandStr.startsWith("W")){
			
			//Format: W(T1,x6,v); v is some number
			String pattern = "W\\s*\\(\\s*T(\\d+)\\s*,\\s*x(\\d+)\\s*,\\s*(\\d+)\\s*\\)";
			
			// Create a Pattern object
			Pattern r = Pattern.compile(pattern);
			
			// Now create matcher object.
			Matcher m = r.matcher(commandStr);
			m.find();

			//Get the write information: instruction, transaction number, index variable, value to write
			return new String[]{"W", m.group(1), m.group(2), m.group(3)};
			
		}else if(commandStr.equals("dump()")){
			
			return new String[]{"dump()"};
			
		}else if(commandStr.startsWith("dump")){
			
			//Format: dump(i) gives the committed values of all copies of all variables at site i.
			//dump(xj) gives the committed values of all copies of variable xj at all sites.
			String pattern1 = "dump\\s*\\(\\s*(\\d+)\\s*\\)";
			String pattern2 = "dump\\s*\\(\\s*x(\\d+)\\s*\\)";

			// Create a Pattern object
			Pattern r1 = Pattern.compile(pattern1);
			Pattern r2 = Pattern.compile(pattern2);

			// Now create matcher object.
			Matcher m1 = r1.matcher(commandStr);
			Matcher m2 = r2.matcher(commandStr);
			m1.find();
			m2.find();

			if(m1.matches())
				return new String[]{"dumpSite", m1.group(1)};
			else
				return new String[]{"dumpVariable", m2.group(1)};

		}else if(commandStr.startsWith("end")){
			
			//Format:end(T1)
			String pattern = "end\\s*\\(\\s*T(\\d+)\\s*\\)";
			
			// Create a Pattern object
			Pattern r = Pattern.compile(pattern);
			
			// Now create matcher object.
			Matcher m = r.matcher(commandStr);
			m.find();

			//Get the end information: transaction number
			return new String[]{"end", m.group(1)};
			
		}else if(commandStr.startsWith("fail")){
			
			//Format: fail(6) 
			String pattern = "fail\\s*\\(\\s*(\\d+)\\s*\\)";
			
			// Create a Pattern object
			Pattern r = Pattern.compile(pattern);
			
			// Now create matcher object.
			Matcher m = r.matcher(commandStr);
			m.find();

			//Get the fail information: siteIndex
			return new String[]{"fail", m.group(1)};
			
		}else if(commandStr.startsWith("recover")){
			
			//Format: recover(7)
			String pattern = "recover\\s*\\(\\s*(\\d+)\\s*\\)";
			
			// Create a Pattern object
			Pattern r = Pattern.compile(pattern);
			
			// Now create matcher object.
			Matcher m = r.matcher(commandStr);
			m.find();

			//Get the recover information: siteIndex
			return new String[]{"recover", m.group(1)};
			
		}
		return null;
		
	}
	
}
