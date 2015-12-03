import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Site {
	//Integer refers to the variable index
	private Map<Integer, Variable> siteVariables = new HashMap<Integer, Variable>();
	private boolean isSiteDown = true;
	//Nothing shal pass the site without the log knowing of it
	private Log siteLog;
	
	//Site index
	private int siteIndex = -1;
	
	//Each site has lockmanager
	private LockManager lockmanager = new LockManager();
	
	List<Lock> activeLocks = new ArrayList<Lock>();

	/*
	 * Instantiates the site
	 */
	public Site(int siteIndex){
		this.siteIndex = siteIndex;
		setSiteDown(false);
	}
	
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
	 * @variables is the new set of variables to replace the current one
	 */
	/*public void setVariablesAtSite(Map<Integer, Variable> variables){
		this.siteVariables = variables;
	}
	
	/*
	 * @variableIndex is the index of the variable to find in this site
	 * @return boolean indicating whether @variableIndex is in this sites
	 */
	public boolean isSiteContainVariableIndex(int variableIndex){
		return this.siteVariables.containsKey(variableIndex);
	}
	
	/*public Variable getCommittedVariable(int indexVariable){
		for(int i=0; i<this.siteVariables.size(); i++){
			if(this.siteVariables.get(i).getIndexVariable() == indexVariable ){
				return this.siteVariables.get(i);
			}
		}
		return null;
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
	 * Process a request of a lock for the variable with @variableIndex
	 * @transactionNumber is the transaction number of the trasaction requesting the lock
	 * @isLockRequestReadOnly refers to the type of lock to request
	 * @return the lock or null if not successful
	 * 
  	 */
	public Lock requestLock(int transactionNumber, int variableIndex, boolean isLockRequestReadOnly){
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
				return null;
			}
		}
		//At this point, the check is successful and add the lock to the active locks
		this.activeLocks.add(candidateLock);
		//Return the lock to the requesting transaction
		return candidateLock;
	}
}
