
public class Test {
	//TODO:General description (see syllabus)
	//TODO:Each function, input, output, side affects
	//TODO:Bitbucket and add viswanath
	public static void main(String[] args){
		String inputfile = System.getProperty("user.dir")+"/projectsampletests.txt";
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
