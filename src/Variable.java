import java.util.ArrayList;
import java.util.List;

public class Variable {
	//An index, a value from 1 to 20, referring to one of the 20 distinct variables
	private int indexVariable = -1;
	
	//current version is the latest version, but the last committed version the latest committed version
	private Version latestVersion = null;
	private Version lastCommittedVersion = null;
	//Don't need versions when everything is in the site's log
	//private List<Version> versions = new ArrayList<Version>();
	
	// isAllowRead true, by default, until fail()
	private boolean isAllowRead = true;
	
	// indicates whether there is a replication of itself at other sites
	//private boolean isVariableReplicated; 
	
	//	isLock has access control by a semaphore i.e. synchronized, ensuring that only one write thread has access
	// TODO: locks must be kept seperate from variable because there can be upgrades from read to write
	//private boolean isLock = false;
	
	//List of all version of this variable
	private List<Version> allVersions = new ArrayList<Version>();
	
	public Variable(int indexVariable, int value, int timestamp, boolean isCommitted){
		this.setIndexVariable(indexVariable);
		this.latestVersion = new Version(value, timestamp, -1, isCommitted);

		if(isCommitted){
			lastCommittedVersion = this.latestVersion;
		}
	}
	/*
	 * Read the version of this variable where the version's timestamp
	 * is less than @timestampBefore i.e. read the COMMITTED version before @timestampBefore
	 * This method is used by the multiversion protocol.
	 * @return is the value read
	 */
	public int readCommitted(int timestampBefore){
		//Search the versions backwards
		for(int i=this.allVersions.size()-1; i>=0; i--){
			if(allVersions.get(i).isCommitted() 
					&& allVersions.get(i).getTimestamp()<timestampBefore){
				return this.allVersions.get(i).getValue();
			}
		}
		return -1;
	}
	/*
	 * @return the latest version, which may not necessarily be committed
	 */
	public int readLatest(){
		return this.lastCommittedVersion.getValue();
	}
	/*
	 * When a currently active transaction writes to a variable, it create a new version
	 * @value is the new value
	 * @timestamp is the current timestamp
	 * @transactionNumber is the current transaction doing this operation
	 */
	public void write(int value, int timestamp, int transactionNumber){
		boolean isCommitted = false;
		this.latestVersion = new Version(value, timestamp, transactionNumber, isCommitted);
		this.allVersions.add(this.latestVersion);
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
		Version newCurrVersion = lastCommittedVersion; 
		for(Version version: this.allVersions){
			if(version.getFromTransactionNumber()==committingTransaction 
					&& version.getTimestamp()>maxTimestamp){
				newCurrVersion = version;
			}
		}
		newCurrVersion.setCommitted();
		this.lastCommittedVersion = newCurrVersion;
	}
	/*
	 * Getter for the current latest version of the variable
	 */
	/*public Version getLatestCommittedVersion(){
		return this.lastCommittedVersion;
	}
	/*
	 * toString function for Variable
	 */
	public String toString(){ 
		return "Latest version:"+this.latestVersion.getTimestamp()+" " +this.latestVersion.getValue() + "\n"+
				"Latest committed version:"+this.lastCommittedVersion.getTimestamp()+" "+this.lastCommittedVersion.getValue()+"\n";
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
