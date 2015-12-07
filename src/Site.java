import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class Site {
	//Integer refers to the variable index
	private Map<Integer, Variable> siteVariables = new HashMap<Integer, Variable>();
	
	private boolean isSiteDown = true;
	
	//Nothing shall pass the site without the log knowing of it
	//TODO: add locks, recover, and fail to log
	private Log siteLog = new Log();
	
	//Site index
	private int siteIndex = -1;
	
	//Each site has lockmanager
	private LockManager lockmanager = new LockManager();
	
	private List<Lock> activeLocks = new ArrayList<Lock>();

	/*
	 * Instantiates the site
	 */
	public Site(int siteIndex){
		this.siteIndex = siteIndex;
		setSiteDown(false);
	}
	/******************************************************************************************************
	 * The following methods must be called by DataManager. 
	 * This way the variables are not changed without locks or read without protocols.
	 * Updates to the site log are done by these methods too.
	 ******************************************************************************************************/
	/*
	 * Read the version of @variableIndex, where the version's timestamp
	 * is less than @timestampBefore i.e. read the COMMITTED version before @timestampBefore.
	 * This method is used by the multiversion protocol.
	 * @variableIndex is the variable index to read, also given from DataManager.
	 * @return is the value read
	 */
	public int readCommitted(int variableIndex, int timestampBefore){
		return this.getVariable(variableIndex).readCommitted(timestampBefore);
	}
	/*
	 * @variableIndex is the variable index to read, also given from DataManager.
	 * @return the latest version, which may not necessarily be committed.
	 */
	public int readLatest(int variableIndex){
		return this.getVariable(variableIndex).readLatest();
	}
	/*
	 * When a currently active transaction writes to a variable, it create a new version.
	 * @variableIndex is the variable index to write to,  given from DataManager.
	 * @value is the new value
	 * @currentTimestamp is the current timestamp
	 * @transactionNumber is the current transaction doing this operation
	 */
	public void write(int variableIndex, int value, int currentTimestamp, int transactionNumber){
		this.getVariable(variableIndex).write(value, currentTimestamp, transactionNumber);
	}
	/*
	 * For recovery purposes, if Variable.isAllowRead = false, then set it to true, 
	 * now that there is a current new update to it Set lastCommittedVersion to the currentVersion
	 * Add a before image and timestamp to the list of versions
	 * @variableIndex is the variable index, also given from Transaction.
	 * @committingTransaction indicates which transaction is committing
	 * @currentTimestamp is the current timestamp
	 * @transactionNumber is the current transaction doing this operation
	 */
	public void commit(int variableIndex, int committingTransaction, int currentTimestamp, int transactionNumber){

		int valueCommitted = this.getVariable(variableIndex).commit(committingTransaction);
			 
		String[] details = new String[]{"commit",""+transactionNumber, ""+variableIndex, ""+valueCommitted};
		siteLog.addEvent(currentTimestamp, details);
		

	}
	/******************************************************************************************************
	 *  Fail and recovery
	 ******************************************************************************************************/
	/*
	 * Set all variable's Site.isAllowRead = false
	 * All sites must release locks from living transactions that were also live at that site.
	 */
	public void fail(){
		setSiteDown(true);
		//Set all variable's Site.isAllowRead = false
		for(Integer key: siteVariables.keySet()){
			Variable variable = siteVariables.get(key);
			variable.setAllowRead(false);
		}
		lockmanager.releaseSiteLock();
	}
	/******************************************************************************************************
	 * Setter, getter, and dump method
	 ******************************************************************************************************/
	/*
	 * @return String currentState for all Variables
	 */
	public String dump(){
		String currentState = "";
		for(Integer key: siteVariables.keySet()){
			Variable variable = siteVariables.get(key);
			currentState = currentState + variable.toString();
		}
		return currentState;
	}

	/*
	 * @return all the variables at this site
	 */
	public Map<Integer, Variable> getVariableSite() {
		return this.siteVariables;
	}

	/*
	 * @variableIndex is the new variable index to add; it is unique
	 * @variable is the actual variable object
	 */
	public void addVariableAtSite(int variableIndex, Variable variable) {
		this.siteVariables.put(variableIndex, variable);
	}
	
	/*
	 * @variableIndex is the index of the variable to find in this site
	 * @return boolean indicating whether @variableIndex is in this sites
	 */
	public boolean isSiteContainVariableIndex(int variableIndex){
		return this.siteVariables.containsKey(variableIndex);
	}
	
	/*
	 * @return a boolean indicating whether this site is CURRENTLY down
	 */
	public boolean isSiteDown() {
		return isSiteDown;
	}
	
	/*
	 * Check whether the site went down after a particular timestamp i.e. @timestamp. 
	 * This function is used for validating a transaction
	 */
	public boolean isSiteDownAfter(int timestamp) {
		int lastFailEvent =  this.siteLog.findLastFailTimestamp();
		
		//If the lastFailEvent happened after the timestamp, then return true
		if(lastFailEvent>timestamp && lastFailEvent!=-1){
			return true;
		}
		return false;
	}
	
	/*
	 * @isSiteDown the new state of the site; this is simply a setter function
	 */
	public void setSiteDown(boolean isSiteDown) {
		this.isSiteDown = isSiteDown;
	}

	/*
	 * This is a getter function for siteIndex
	 */
	public int getSiteIndex() {
		return siteIndex;
	}
	
	/*
	 * Get a variable given @variableIndex
	 */
	public Variable getVariable(int variableIndex){
		return this.siteVariables.get(variableIndex);
	}


	/*
	 * @return a list of latest committed values of all variables in string form, for the sake of print
	 */
	public String getVariableToString(){
		String str = ""+ "\t";
		SortedSet<Integer> keys = new TreeSet<Integer>(this.siteVariables.keySet());
		for(Integer variableIndex: keys){
			Variable variable = this.siteVariables.get(variableIndex);
			str = str + "x" + variableIndex + "." + this.siteIndex + "=";
			str = str + variable.toStringLatestCommitted()+ " ";
		}
		return str;
	}
	/*
	 * @return the latest committed value of at variable index in string form, for the sake of print
	 */
	public String getVariableToString(int variableIndex){
		String str = "";
		Variable variable = this.siteVariables.get(variableIndex);
		str = str + "\t" + "x" + variableIndex + "." + this.siteIndex + " => ";
		str = str + variable.toStringLatestCommitted()+ "\n";
		return str;
	}
	
	
	/******************************************************************************************************
	 * The following methods are lock related.
	 ******************************************************************************************************/
	/*
	 * Process a request of a lock for the variable with @variableIndex
	 * @transactionNumber is the transaction number of the trasaction requesting the lock
	 * @isLockRequestReadOnly refers to the type of lock to request
	 * @return the lock or null if not successful
	 * 
  	 */
	public Lock requestLock(int currentTimestamp, int transactionNumber, int variableIndex, boolean isLockRequestReadOnly){
		Lock candidateLock = new Lock(transactionNumber, this.siteIndex, variableIndex, isLockRequestReadOnly);
		for(Lock lock: this.activeLocks){
			//Loop through all the active locks. 
			//If there is a write lock already on @variableIndex, no other transaction may access it, so return null
			//If a different transaction has a read lock and the requesting lock is a write lock, do not give the lock i.e. return null
			if((lock.getTransactionNumber()!=transactionNumber
					&& lock.getLockedVariableIndex()==variableIndex
					&& !lock.isReadOnly())
				|| 
				(lock.getTransactionNumber()!=transactionNumber
					&& lock.getLockedVariableIndex() == variableIndex
					&& lock.isReadOnly()
					&& !isLockRequestReadOnly)){
				
				System.out.println((isLockRequestReadOnly?"Read":"Exclusive") +" Lock denied for T"+transactionNumber+" on x"+variableIndex+"."+siteIndex);
				return null;
				
			}
		}
		//At this point, the check is successful and add the lock to the active locks
		this.activeLocks.add(candidateLock);
		
		System.out.println((isLockRequestReadOnly?"Read":"Exclusive") +" Lock give to T"+transactionNumber+" on x"+variableIndex+"."+siteIndex);

		//Give lock and record in the site
		String[] details = new String[]{"give lock",""+transactionNumber, ""+variableIndex, ""+isLockRequestReadOnly};
		siteLog.addEvent(currentTimestamp, details);
		
		//Return the lock to the requesting transaction
		return candidateLock;
	}
	
	/*
	 * Release all locks for a particular transaction from @activeLocks
	 * @transactionNumber refers to the transaction in which his locks should be released
	 */
	public void releaseLocks(int transactionNumber, int currentTimestamp){
		for(Lock lock: this.activeLocks){
			if(lock.getTransactionNumber() == transactionNumber){
				
				System.out.println("Release lock from T"+transactionNumber+" on x"+lock.getLockedVariableIndex()+"."+siteIndex);
				
				//Give lock and record in the site
				String[] details = new String[]{"release lock",""+transactionNumber, ""+lock.getLockedVariableIndex(), ""+lock.isReadOnly()};
				siteLog.addEvent(currentTimestamp, details);
				
				this.activeLocks.remove(lock);
			}
		}
	}
	
	/*
	 * Return the lock that is conflicting with a hypothetical lock of the following properties:
	 * @transactionNumber is the transaction number of the transaction requesting the lock
	 * @variableIndex is the variable index the hypothetical lock would hold on
	 * @isLockRequestReadOnly refers to the type of the hypothetical lock
	 * @return the lock or null if not successful
	 */
	public Lock getConflictingLock(int transactionNumber, int variableIndex, boolean isLockRequestReadOnly){
		for(Lock lock: this.activeLocks){
			//Loop through all the active locks. 
			
			//If there is a conflicting lock  on @variableIndex, return that lock
			if((lock.getTransactionNumber()!=transactionNumber
					&& lock.getLockedVariableIndex()==variableIndex
					&& !lock.isReadOnly())
				|| 
				(lock.getTransactionNumber()!=transactionNumber
					&& lock.getLockedVariableIndex() == variableIndex
					&& lock.isReadOnly()
					&& !isLockRequestReadOnly)){
				
				return lock;
				
			}
		}
		return null;
	}
	
}
