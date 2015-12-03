import java.util.ArrayList;
import java.util.List;

public class Variable {
	//An index, a value from 1 to 20, referring to one of the 20 distinct variables
	private int indexVariable = -1;
	
	private Version currentVersion = null;
	private Version lastCommittedVersion = null;
	//Don't need versions when everything is in the site's log
	//private List<Version> versions = new ArrayList<Version>();
	
	// isAllowRead true, by default, until fail()
	private boolean isAllowRead = true;
	
	// indicates whether there is a replication of itself at other sites
	private boolean isVariableReplicated; 
	
	//	isLock has access control by a semaphore i.e. synchronized, ensuring that only one write thread has access
	// TODO: locks must be kept seperate from variable because there can be upgrades from read to write
	private boolean isLock = false;
	
	//List of all version of this variable
	private List<Version> allVersions = new ArrayList<Version>();
	
	public Variable(int indexVariable, int value, int timestamp){
		this.setIndexVariable(indexVariable);
		this.currentVersion = new Version(value, timestamp, -1);

	}
	/*
	 * When a currently active transaction writes to a variable, it create a new version
	 * @value is the new value
	 * @timestamp is the current timestamp
	 * @transactionNumber is the current transaction doing this operation
	 */
	public void write(int value, int timestamp, int transactionNumber){
		this.allVersions.add(new Version(value, timestamp, transactionNumber));
	}
	/*
	 * Update current version to value
	 * TODO: What is the timestamp of the final written value? The current
	 * TODO: timestamp when it was written or the transaction's timestamp?
	 */
	/*private void writeToCurrentVersion(int value, int timestamp){
		this.currentVersion = new Version(value, timestamp, -1);
	}
	/*
	 * Set the currentVersion to the before image of t
	 */
	public void restoreBeforeImage(Transaction t){
		//Set the currentVersion to the before image of t
		//TODO: IOW, set currentVersion to the last committed value that happenned before t started
	}
	
	/*
	 * If isAllowRead = false, then set it to true, 
	 * now that there is a current new update to it Set lastCommittedVersion to the currentVersion
	 * Add a before image and timestamp to the list of versions
	 * @committingTransaction indicates which transaction is committing
	 */
	public void commit(int committingTransaction){
		//This means setting the versions written by the committing transaction and set that 
		//as the current version
		
		//Get the latest timestamp by @committingTransaction
		int maxTimestamp = -1;
		Version newCurrVersion = currentVersion; 
		for(Version version: this.allVersions){
			if(version.getFromTransactionNumber()==committingTransaction 
					&& version.getTimestamp()>maxTimestamp){
				newCurrVersion = version;
			}
		}
		this.currentVersion = newCurrVersion;
	}
	/*
	 * Getter for the current version of the variable
	 */
	public Version getCurrentVersion(){
		return this.currentVersion;
	}
	/*
	 * toString function for Variable
	 */
	public String toString(){ 
		return currentVersion.getTimestamp()+" " +currentVersion.getValue() + "\n";
	}
	
	/*
	 * Setter and getters for @isAllowRead
	 */
	public boolean isAllowRead() {
		return isAllowRead;
	}
	public void setAllowRead(boolean isAllowRead) {
		this.isAllowRead = isAllowRead;
	}
	/*
	 * Setters and getters for @indexVariable
	 */
	public int getIndexVariable() {
		return indexVariable;
	}
	public void setIndexVariable(int indexVariable) {
		this.indexVariable = indexVariable;
	}
	
	
	
}
