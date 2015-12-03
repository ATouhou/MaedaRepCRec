import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	private List<Integer[]> sitesIndexesVariableIndexesAccessed = new ArrayList<Integer[]>();
		
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
				dm.getSite(siteIndex).getVariable(variableIndex).write(valueToWrite, currentTimestamp, transactionNumber);
				
				//Record the accessed sites
				if(!this.sitesIndexesAccessed.contains(siteIndex)){
					this.sitesIndexesAccessed.add(siteIndex);
				}
			}
			
		}else if(operation.equals("R")){
			
			//In this case, the command array contains the site index to read from:
			//command = [ "R", transaction number, index variable, site index to read from]
			int siteIndexToReadFrom = Integer.parseInt(command[3]);
			int variableIndex = Integer.parseInt(command[2]);
			
			//Read the latest version, which may not necessarily be committed
			dm.getSite(siteIndexToReadFrom).getVariable(variableIndex).readLatest();

			//Record the accessed sites
			if(!this.sitesIndexesAccessed.contains(siteIndexToReadFrom)){
				this.sitesIndexesAccessed.add(siteIndexToReadFrom);
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
	public void commit() {
		//At commit, the transaction informs all the variables to commit
	}
	
	@Override
	public void abort() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void releaseLocks() {
		//When releasing the locks, the transaction informs the sites about the releasing the locks
		//The transaction sets its own set of locks to null
		for(Integer siteAccessedIndex: this.sitesIndexesAccessed){
			this.dm.getSite(siteAccessedIndex).
		}
		
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
	

}
