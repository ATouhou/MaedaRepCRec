
public class Test {
	//TODO:General description (see syllabus)
	//TODO:Each function, input, output, side affects
	//TODO: reprozip
	//The submitted code similarly should include as a header: the author (if you are working in a team of two), the date, 
	//the general description of the function, the inputs, the outputs, and the side effects.
	//TODO: In addition, you should provide a few testing scripts in plain text to the grader in the format above and say what you believe will happen in each test.
	public static void main(String[] args){
		String inputfile = System.getProperty("user.dir")+"/TestCases/test12.txt";
		if (args.length > 0) {
			inputfile = args[0];
	
		}
		//Load the database
		TransactionManager tm = new TransactionManager();
		
		Parser parser = new Parser(inputfile, tm);

		//Parser will invoke the transaction manager for the next command
		parser.parse();
		
	}
}
