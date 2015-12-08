import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RWTransaction implements Transaction{
	private boolean debug = false;
	
	private boolean isReadOnly = false;
	private DataManager dm = null; 
	
	//List of locks the transaction owns; this list reflects the lock tables on the sites
	private List<Lock> locks = new ArrayList<Lock>();
	
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

	//Since the waits for graph is acyclic due to wait-die, there can at most be one transaction that this transaction is waiting for
	//If this value is -1, then this transaction is active.
	private int waitForTransaction = -1;

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
				Lock  requiredLock = getActiveLock( siteIndex,  variableIndex, false);
				dm.write(siteIndex, variableIndex, valueToWrite, currentTimestamp, transactionNumber, requiredLock);
				
				System.out.println("RWTran: T"+this.transactionNumber+" write "+valueToWrite+" to x"+variableIndex+"."+siteIndex);

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
			Lock  requiredLock = getActiveLock( siteIndexToReadFrom,  variableIndex, true);
			
			//Read the latest version, which may not necessarily be committed
			int readValue = this.dm.readLatest(siteIndexToReadFrom,variableIndex, requiredLock );
			System.out.println("RWTran: T"+this.transactionNumber+" read "+readValue);
			
			//Record the accessed sites
			if(!this.sitesIndexesAccessed.contains(siteIndexToReadFrom)){
				this.sitesIndexesAccessed.add(siteIndexToReadFrom);
			}
			
			if(!this.sitesIndexesVariableIndexesAccessed.contains(new Integer[]{siteIndexToReadFrom, variableIndex})){
				sitesIndexesVariableIndexesAccessed.add(new Integer[]{siteIndexToReadFrom, variableIndex});
			}
		}
		
	}
	/******************************************************************************************************
	 * Lock related operations
	 ******************************************************************************************************/
	/*
	 * @return the active with the given paramters
	 */
	public Lock getActiveLock(int siteIndex, int variableIndex, boolean isReadOnlyLockType){
		for(Lock lock: this.locks){
			if(lock.isEqual(new Lock(this.transactionNumber, siteIndex, variableIndex,  isReadOnlyLockType))){
				return lock;
			}
		}
		return null;
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
	 * @return whether this transaction has the read/write lock on  @variableIndex at @siteIndex
	 */
	public boolean isHasReadOrWriteLock(int siteIndex, int variableIndex){
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

	/*
	 * @return whether this transaction has a read only lock on @variableIndex at @siteIndex
	 */
	public boolean isHasReadOnlyLock(int siteIndex, int variableIndex){
		for(Lock locks: this.locks){
			//Find a match for a lock based on @siteIndex and @variableIndex
			//Don't have to check for the type of the lock, because a write lock is good as well
			if(locks.getTransactionNumber()==this.transactionNumber 
					&& locks.getSiteIndex()==siteIndex
					&& locks.getLockedVariableIndex()==variableIndex
					&& locks.isReadOnly()
					){
				return true;
			}
		}
		return false;
	}
	/*
	 * Upgrade a read lock to a write lock. This assumes that the transaction manager has gained the permission to do so by the site.
	 * @transactionNumber, @siteIndex, @variableIndex are values of the lock
	 */
	public void upgradeLock(int transactionNumber, int siteIndex, int variableIndex){
		//Find the lock with the given parameters
		for(int i=0; i<this.locks.size(); i++){
			Lock lockToUpgrade = this.locks.get(i);
			if(lockToUpgrade.getTransactionNumber()==transactionNumber
					&& lockToUpgrade.getSiteIndex()==siteIndex
					&& lockToUpgrade.getLockedVariableIndex()==variableIndex){
				//Change the locks
				this.locks.get(i).setIsReadOnly(false);
				return;
				
			}
		}
	}
	/*
	 * @lock is the lock given by the Transaction Manager. Whenever a lock is acquired, the transaction manager mustnotify the transaction.
	 */
	@Override
	public void addLock(Lock lock) {
		this.locks.add(lock);
	}
	
	/******************************************************************************************************
	 * Transaction commit, abort, release locks, called by the Transaction Manager
	 ******************************************************************************************************/
	
	@Override
	public void commit(int currentTimestamp) {
		//At commit, the transaction informs all the variables to commit
		for(Integer[] siteXVariableAccessed: this.sitesIndexesVariableIndexesAccessed){
			int siteIndex = siteXVariableAccessed[0];
			int variableIndex = siteXVariableAccessed[1];
			
			this.dm.commit(siteIndex, variableIndex, transactionNumber, currentTimestamp, transactionNumber);
		}
		//The transaction releases its locks
		releaseLocks( currentTimestamp);

	}
	
	/*
	 * On abort, nothing should be committed, i.e. any changes done in the database musn't be reflected in the actual database
	 * Locks must be released
	 */
	@Override
	public void abort(int currentTimestamp) {
		// TODO: should we delete the version written by this transaction?
		System.out.println("RWTran: T"+this.transactionNumber+" aborted.");
		releaseLocks( currentTimestamp);

	}
	
	@Override
	public void releaseLocks(int currentTimestamp) {
		//When releasing the locks, the transaction informs the sites about the releasing the locks
		//The transaction sets its own set of locks to null
		if(debug) System.out.println("RWTran: T"+this.transactionNumber+" accessed "+this.sitesIndexesAccessed);
		for(Integer siteAccessedIndex: this.sitesIndexesAccessed){
			this.dm.getSite(siteAccessedIndex).releaseLocks(transactionNumber, currentTimestamp);
		}
		
		this.locks = new ArrayList<Lock>();
	}
	/******************************************************************************************************
	 * Getters and setters
	 ******************************************************************************************************/

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
	 * Related to queuing. 
	 ******************************************************************************************************/
	public List<String[]> getQueuedOperations() {
		return queuedOperations;
	}
	public String getQueuedOperationsToString() {
		String str = "";
		for(String[] op: this.queuedOperations){
			str = str +" "+Arrays.toString(op);
		}
		return str;
	}
	@Override
	public void addQueuedOperation(String[] queuedOperations) {
		System.out.println("RWTran: Queue T"+this.transactionNumber+" operations "+Arrays.toString(queuedOperations));
		this.queuedOperations.add(queuedOperations);
	}
	@Override
	public void removeQueuedOperation(String[] queuedOperations) {
		this.queuedOperations.remove(queuedOperations);
	}
	@Override
	public boolean isWaiting() {
		return waitForTransaction!=-1?  true :  false;
	}
	@Override
	public int getTransactionWaitFor() {
		return this.waitForTransaction;
	}
	@Override
	public void setToWaitFor(int transactionNumWait) {
		this.waitForTransaction = transactionNumWait;
	}
	@Override
	public void setTransactionToActive() {
		this.waitForTransaction = -1;
	}
	
	
	

}
