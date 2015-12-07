import java.util.ArrayList;
import java.util.List;

public class RWTransaction implements Transaction{
	private boolean isReadOnly = false;
	private DataManager dm = null; 
	private boolean isWait = false;
	
	//List of locks the transaction owns; this list reflects the lock tables on the sites
	List<Lock> locks = new ArrayList<Lock>();
	
	private int transactionNumber = -1;
	
	//This is timestamp at which the transaction was created
	private int beginningTimestamp = -1;
	
	//A list of all the sites accessed
	private List<Integer> sitesIndexesAccessed = new ArrayList<Integer>();
	
	//Pairs of site indexes and variable indexes that the transaction has accessed
	//i.e. [ {site index 1, variable index 3}, {...}, ... ]
	private List<Integer[]> sitesIndexesVariableIndexesAccessed = new ArrayList<Integer[]>();
		
	//A list of commands, where command has the same format as the output of Parser.parseNextInstruction()
	//The latest queued operation is at the end of the list
	private List<String[]> queuedOperations = new ArrayList<String[]>();
		
	/*
	 * Instantiates the read-write transaction
	 */
	public RWTransaction(DataManager dm, int transactionNumber, int beginningTimestamp){
		this.dm = dm;
		this.transactionNumber = transactionNumber;
		this.beginningTimestamp = beginningTimestamp;
	}
	/*
	 *  Before calling this method, the TransactionManager has already checked for needed locks.
	 *  If the needed locks were not acquired, then this method is never called anyways. 
	 *  @operation is either "W" or "R"
	 * 	@command is the array of details about the incoming command; it is the output of Parser.parse
	 */
	@Override
	public void processOperation(String operation, String[] command, int currentTimestamp) {
		if(operation.equals("W")){
			
			//command = ["W", transaction number, index variable, value to write]
			int transactionNumber = Integer.parseInt(command[1]);
			int variableIndex = Integer.parseInt(command[2]);
			int valueToWrite = Integer.parseInt(command[3]);

			List<Integer> siteIndexesToWrite  = this.dm.getAvailableSitesVariablesWhere(variableIndex);
			
			for(Integer siteIndex: siteIndexesToWrite){
				//dm.getSite(siteIndex).getVariable(variableIndex).write(valueToWrite, currentTimestamp, transactionNumber);
				dm.write(siteIndex, variableIndex, valueToWrite, currentTimestamp, transactionNumber);
				
				System.out.println("T"+this.transactionNumber+" write "+valueToWrite+" to x"+variableIndex+"."+siteIndex);

				//Record the accessed sites
				if(!this.sitesIndexesAccessed.contains(siteIndex)){
					this.sitesIndexesAccessed.add(siteIndex);
				}
				
				if(!this.sitesIndexesVariableIndexesAccessed.contains(new Integer[]{siteIndex, variableIndex})){
					sitesIndexesVariableIndexesAccessed.add(new Integer[]{siteIndex, variableIndex});
				}
			}
			
		}else if(operation.equals("R")){
			
			//In this case, the command array contains the site index to read from:
			//command = [ "R", transaction number, index variable, site index to read from]
			int siteIndexToReadFrom = Integer.parseInt(command[3]);
			int variableIndex = Integer.parseInt(command[2]);
			
			//Read the latest version, which may not necessarily be committed
			//dm.getSite(siteIndexToReadFrom).getVariable(variableIndex).readLatest();
			int readValue = this.dm.readLatest(siteIndexToReadFrom,variableIndex );
			System.out.println("T"+this.transactionNumber+" read "+readValue);
			
			//Record the accessed sites
			if(!this.sitesIndexesAccessed.contains(siteIndexToReadFrom)){
				this.sitesIndexesAccessed.add(siteIndexToReadFrom);
			}
			
			if(!this.sitesIndexesVariableIndexesAccessed.contains(new Integer[]{siteIndexToReadFrom, variableIndex})){
				sitesIndexesVariableIndexesAccessed.add(new Integer[]{siteIndexToReadFrom, variableIndex});
			}
		}
		
	}
	
	/*
	 * @return whether this transaction has the write lock on  @variableIndex at @siteIndex
	 */
	public boolean isHasWriteLock(int siteIndex, int variableIndex){
		for(Lock locks: this.locks){
			//Find a match for a lock based on @siteIndex and @variableIndex
			if(locks.getTransactionNumber()==this.transactionNumber 
					&& locks.getSiteIndex()==siteIndex
					&& locks.getLockedVariableIndex()==variableIndex
					&& !locks.isReadOnly()){
				return true;
			}
		}
		return false;
	}
	
	/*
	 * @return whether this transaction has the read lock on  @variableIndex at @siteIndex
	 */
	public boolean isHasReadLock(int siteIndex, int variableIndex){
		for(Lock locks: this.locks){
			//Find a match for a lock based on @siteIndex and @variableIndex
			//Don't have to check for the type of the lock, because a write lock is good as well
			if(locks.getTransactionNumber()==this.transactionNumber 
					&& locks.getSiteIndex()==siteIndex
					&& locks.getLockedVariableIndex()==variableIndex
					){
				return true;
			}
		}
		return false;
	}

	@Override
	public void commit(int currentTimestamp) {
		//At commit, the transaction informs all the variables to commit
		for(Integer[] siteXVariableAccessed: this.sitesIndexesVariableIndexesAccessed){
			int siteIndex = siteXVariableAccessed[0];
			int variableIndex = siteXVariableAccessed[1];
			
			this.dm.commit(siteIndex, variableIndex, transactionNumber, currentTimestamp, transactionNumber);
		}
	}
	
	/*
	 * On abort, nothing should be committed, i.e. any changes done in the database musn't be reflected in the actual database
	 */
	@Override
	public void abort() {
		// TODO: should we delete the version written by this transaction?
		System.out.println("T"+this.transactionNumber+" aborted.");
	
	}
	
	@Override
	public void releaseLocks(int currentTimestamp) {
		//When releasing the locks, the transaction informs the sites about the releasing the locks
		//The transaction sets its own set of locks to null
		for(Integer siteAccessedIndex: this.sitesIndexesAccessed){
			this.dm.getSite(siteAccessedIndex).releaseLocks(transactionNumber, currentTimestamp);
		}
		
		this.locks = new ArrayList<Lock>();
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
	public List<String[]> getQueuedOperations() {
		return queuedOperations;
	}
	@Override
	public void addQueuedOperation(String[] queuedOperations) {
		this.queuedOperations.add(queuedOperations);
	}
	@Override
	public void removeQueuedOperation(String[] queuedOperations) {
		this.queuedOperations.remove(queuedOperations);
	}
	
	

}
