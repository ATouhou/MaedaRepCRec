import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ROTransaction implements Transaction{
	private boolean isReadOnly = true;
	private DataManager dm = null; 
	
	private int transactionNumber = -1;

	//This is timestamp at which the transaction was created
	private int beginningTimestamp = -1;
	
	//A list of all the sites accessed
	private List<Integer> sitesIndexesAccessed = new ArrayList<Integer>();
		
	//A list of commands, where command has the same format as the output of Parser.parseNextInstruction()
	private List<String[]> queuedOperations = new ArrayList<String[]>();

	public ROTransaction(DataManager dm, int transactionNumber, int beginningTimestamp){
		this.dm = dm;
		this.transactionNumber = transactionNumber;
		this.beginningTimestamp = beginningTimestamp;
	}
	/*
	 * processOperation() = read(Variable x) - return the version from before this transaction started.
	 */
	@Override
	public void processOperation(String operation, String[] inputs, int currentTimestamp) {

		//command = [ "R", transaction number, index variable]

		//Get the possible site indexes the transaction to read from
		int variableIndex = Integer.parseInt(inputs[2]);
		List<Integer> siteIndexesToReadFrom  = this.dm.getAvailableSitesVariablesWhere(variableIndex);

		//Read from any one site
		int readValue = this.dm.readCommitted(siteIndexesToReadFrom.get(0), variableIndex, this.beginningTimestamp);
		//Site siteToRead = this.dm.getSite(siteIndexesToReadFrom.get(0));
		//int readValue =siteToRead.getVariable(variableIndex).readCommitted(this.beginningTimestamp);
		System.out.println("ROTran: "+readValue);

		//Add the site to list of accessed sites
		sitesIndexesAccessed.add(siteIndexesToReadFrom.get(0));
	}

	/*
	 * A read only transaction only uses multiversion protocol, so there aren't any locks
	 */
	@Override
	public void releaseLocks(int currentTimestamp) {
		//Do nothing here
	}

	/*
	 * Since this is a read-only transaction, there is no chance of abort since according to the available 
	 * copies algorithm a read-only transaction need not to check for validation at commit (see slides).
	 */
	@Override
	public void commit(int currentTimestamp) {
		//At commit nothing is done
	}
	
	@Override
	public void abort(int currentTimestamp) {
		//Abort is never called in read-only transactions
		System.out.println("ROTran: T"+this.transactionNumber+" aborted.");

	}

	@Override
	public List<Integer> getSiteIndexesAccessed() {
		return this.sitesIndexesAccessed;
	}
	 

	@Override
	public int getBeginningTimestamp() {
		return this.beginningTimestamp;
	}

	@Override
	public void setReadOnly(boolean input) {
		this.isReadOnly = input;
	}

	@Override
	public boolean getReadOnly() {
		return this.isReadOnly;
	}
	@Override
	public int getTransactionNumber() {
		return this.transactionNumber;
	}
	/******************************************************************************************************
	 * Related to queuing. Since this transaction is read only, there is nothing stopping this transaction.
	 ******************************************************************************************************/
	
	@Override
	public List<String[]> getQueuedOperations() {
		return this.queuedOperations;
	}
	@Override
	public void addQueuedOperation(String[] queuedOperations) {
		this.queuedOperations.add(queuedOperations);
	}
	@Override
	public void removeQueuedOperation(String[] queuedOperations) {
		this.queuedOperations.remove(queuedOperations);
	}
	@Override
	public boolean isWaiting() {
		return false;
	}
	@Override
	public int getTransactionWaitFor() {
		return -1;
	}
	@Override
	public void setToWaitFor(int transactionNumWait) {
	}
	@Override
	public void setTransactionToActive() {
		
	}
	
	
	 
	
	

}
