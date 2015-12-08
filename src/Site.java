import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class Site {
	boolean debug = false;
	
	//Integer refers to the variable index
	private Map<Integer, Variable> siteVariables = new HashMap<Integer, Variable>();
	
	private boolean isSiteDown = true;
	
	//Nothing shall pass the site without the log knowing of it
	//TODO: add locks, recover, and fail to log
	private Log siteLog = new Log();
	
	//Site index
	private int siteIndex = -1;
	
	//Each site has lockmanager
	//private LockManager lockmanager = new LockManager();
	
	private List<Lock> activeLocks = new ArrayList<Lock>();
	
	//Indexes of variables that aren't replicated, which is important information for recovery purposes
	private List<Integer> unreplicatedVariables = new ArrayList<Integer>();

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
	 * Reading the latest requires a shared lock/write lock.
	 * @variableIndex is the variable index to read, also given from DataManager.
	 * @requiredLock is the lock that gives access to the variable. 
	 *  	It must exist in the lock manager, otherwise the operation returns -1, meaning that the read couldn't be done.
	 * @return the latest version, which may not necessarily be committed.
	 */
	public int readLatest(int variableIndex, Lock requiredLock){
		if(this.isActiveLockExist(requiredLock)){
			return this.getVariable(variableIndex).readLatest();
		}
		
		System.out.println("Site: Required lock not valid for operation on x"+variableIndex+"."+this.siteIndex);
		return -1;
	}
	/*
	 * When a currently active transaction writes to a variable, it create a new version.
	 * @variableIndex is the variable index to write to,  given from DataManager.
	 * @value is the new value
	 * @currentTimestamp is the current timestamp
	 * @transactionNumber is the current transaction doing this operation
 	 * @requiredLock is the lock that gives access to the variable. 
 	 */
	public void write(int variableIndex, int value, int currentTimestamp, int transactionNumber, Lock requiredLock){
		if(this.isActiveLockExist(requiredLock)){
			this.getVariable(variableIndex).write(value, currentTimestamp, transactionNumber);
		}else{
			System.out.println("Site: Required lock not valid for operation from T"+transactionNumber+"on x"+variableIndex+"."+this.siteIndex);
		}
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
		
		System.out.println("Site: Committing T"+committingTransaction+" x"+variableIndex+"."+this.siteIndex);
		
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
	public void fail(int currentTimestamp){
		setSiteDown(true);
		//Set all variable's Site.isAllowRead = false
		for(Integer key: siteVariables.keySet()){
			Variable variable = siteVariables.get(key);
			variable.setAllowRead(false);
		}
		
		this.activeLocks = new ArrayList<Lock>();
		System.out.println("Site: Site "+this.siteIndex+" erased locks.");
		
		String[] details = new String[]{"fail"};
		siteLog.addEvent(currentTimestamp, details);
		
		System.out.println("Site: fail site "+this.getSiteIndex());

	}
	/*
	 * Recover this site.
	 * For all Variables at x, if the variable is not replicated at any other sites, then Variable.isAllowRead = true.
	 * This allows reads to a variable not replicated anywhere else. 
	 * If the variable is replicated: 
	 *  - Variable.lastCommittedVersion = -1, which sets the data to unusable, until a write happens. 
	 *  - Variable.currentVersion = -1, which sets the data to unusable until a write happens.
	 *   
	 *  Site.isSiteDown = false, which allows writes to the site, but not reads to replicated ones.
	 *  Record the recovery in the site's log.
	 */
	public void recover(int currentTimestamp){
		System.out.println("Site: recover site "+this.getSiteIndex());
		
		//For all Variables at x, if the variable is not replicated at any other sites, then Variable.isAllowRead = true.
		//This allows reads to a variable not replicated anywhere else. 
		for(Variable variable: this.siteVariables.values()){
			if(!isVariableReplicated(variable.getIndexVariable())){
				variable.setAllowRead(true);
			}else{
				//Version.lastCommittedVersion = -1, which sets the data to unreadable, until a write happens 
				//Version.currentVersion = -1, which sets the data to unreadable until a write happens 
				variable.resetVersionIndexes();
			}
		}
		
		
		//Site.isSiteDown = false, which allows writes to the site, but not reads to replicated ones
		this.isSiteDown = false;
		
		String[] details = new String[]{"recover"};
		siteLog.addEvent(currentTimestamp, details);
		
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
	 * @isReplicatedAnywhereElse is a boolean referring to whether is variable is replicated at other sites, which is important information used by recovery
	 */
	public void addVariableAtSite(int variableIndex, Variable variable, boolean isReplicatedAnywhereElse) {
		this.siteVariables.put(variableIndex, variable);
		if(!isReplicatedAnywhereElse){
			unreplicatedVariables.add(variableIndex);
		}
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
		if(variable!=null){
			str = str + "\t" + "x" + variableIndex + "." + this.siteIndex + " => ";
			str = str + variable.toStringLatestCommitted()+ "\n";
		}else{
			str = str + "\t" + "x" + variableIndex + "." + this.siteIndex + " => Not exists\n";
		}
		return str;
	}
	/*
	 * @variableIndex is the index of the variable that we want to check.
	 * @return true if replicated at other site
	 */
	public boolean isVariableReplicated(int variableIndex){
		for(int nonreplicated: this.unreplicatedVariables){
			if(nonreplicated == variableIndex){
				return false;
			}
		}
		return true;
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
				System.out.println("Site: T"+transactionNumber+" found conflicting lock held by T"+lock.getTransactionNumber());
				System.out.println("Site: "+(isLockRequestReadOnly?"Read":"Exclusive") +" Lock denied for T"+transactionNumber+" on x"+variableIndex+"."+siteIndex);
				return null;
				
			}
		}
		//At this point, the check is successful and add the lock to the active locks
		this.activeLocks.add(candidateLock);
		
		System.out.println("Site: "+(isLockRequestReadOnly?"Read":"Exclusive") +" Lock give to T"+transactionNumber+" on x"+variableIndex+"."+siteIndex);

		//Give lock and record in the site
		String[] details = new String[]{"give lock",""+transactionNumber, ""+variableIndex, ""+isLockRequestReadOnly};
		siteLog.addEvent(currentTimestamp, details);
		
		//Return the lock to the requesting transaction
		return candidateLock;
	}
	/*
	 * Process an upgrade request of a lock from read to exclusive for the variable with @variableIndex
	 * @transactionNumber is the transaction number of the transaction requesting the lock
	 * @return the lock or null if not successful
	 * 
	 */
	public Lock upgradeRequest(int currentTimestamp, int transactionNumber, int variableIndex){
		Lock candidateLock = new Lock(transactionNumber, this.siteIndex, variableIndex, false);
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
					&& lock.isReadOnly())){
				System.out.println("Site: T"+transactionNumber+" found conflicting lock held by T"+lock.getTransactionNumber());
				System.out.println("Site: Upgrade to exclusive lock denied for T"+transactionNumber+" on x"+variableIndex+"."+siteIndex);
				return null;
				
			}
		}
		
		//If successful, remove the lock from the activeLocks and add the candidate to the activeLocks
		int index =0;
		while(index<this.activeLocks.size()){
			if(this.activeLocks.get(index).getLockedVariableIndex()==variableIndex &&
					this.activeLocks.get(index).getTransactionNumber()==transactionNumber){
				this.activeLocks.remove(index);
				break;
			}
			index++;
		}
		this.activeLocks.add(candidateLock);
		
		System.out.println("Site: Upgrade to exclusive lock give to T"+transactionNumber+" on x"+variableIndex+"."+siteIndex);

		//Give lock and record in the site
		String[] details = new String[]{"give lock",""+transactionNumber, ""+variableIndex, ""+false};
		siteLog.addEvent(currentTimestamp, details);
		
		return candidateLock;

	}
	/*
	 * Release all locks for a particular transaction from @activeLocks
	 * @transactionNumber refers to the transaction in which his locks should be released
	 */
	public void releaseLocks(int transactionNumber, int currentTimestamp){
		int i=0;
		while(i<this.activeLocks.size()){
			Lock lock =this.activeLocks.get(i);
			if(lock.getTransactionNumber() == transactionNumber){
				
				System.out.println("Site: Release lock from T"+transactionNumber+" on x"+lock.getLockedVariableIndex()+"."+siteIndex);
				
				//Give lock and record in the site
				String[] details = new String[]{"release lock",""+transactionNumber, ""+lock.getLockedVariableIndex(), ""+lock.isReadOnly()};
				siteLog.addEvent(currentTimestamp, details);
				
				this.activeLocks.remove(lock);
			}else{
				if(debug) System.out.println("Site: Lock not released on x"+lock.getLockedVariableIndex()+"."+siteIndex+" held by T"+lock.getTransactionNumber());

				i++;
				
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
	
	/*
	 * @return true if this site has given such a lock, @testLock 
	 */
	public boolean isActiveLockExist(Lock testLock){
		for(Lock lock:this.activeLocks){
			if(lock.isEqual(testLock)){
				return true;
			}
		}
		return false;
	}
	
}
