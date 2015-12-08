import java.util.ArrayList;
import java.util.List;

public class Variable {
	private boolean debug = false;
	
	//An index, a value from 1 to 20, referring to one of the 20 distinct variables
	private int indexVariable = -1;
	
	//current version is the latest version, but the last committed version the latest committed version
	private int latestVersion = -1;
	private int lastCommittedVersion = -1;
	
	// isAllowRead true, by default, until fail()
	private boolean isAllowRead = true;
	
	//	isLock has access control by a semaphore i.e. synchronized, ensuring that only one write thread has access
	// TODO: locks must be kept seperate from variable because there can be upgrades from read to write
	//private boolean isLock = false;
	
	//List of all version of this variable
	private List<Version> allVersions = new ArrayList<Version>();
	
	public Variable(int indexVariable, int value, int timestamp, boolean isCommitted){
		this.setIndexVariable(indexVariable);
		this.allVersions.add(new Version(value, timestamp, -1, isCommitted));
		this.latestVersion = this.allVersions.size()-1;

		if(isCommitted){
			lastCommittedVersion = this.latestVersion;
		}
	}
	/******************************************************************************************************
	 * Read and writes and commit methods. The methods assume the Managers have already ensured the locks are there
	 *  and protocols are followed. So no locks are used. These methods are called only by Site.
	 ******************************************************************************************************/
	/*
	 * Read the version of this variable where the version's timestamp
	 * is less than @timestampBefore i.e. read the COMMITTED version before @timestampBefore
	 * This method is used by the multiversion protocol.
	 * @return is the value read
	 */
	public int readCommitted(int timestampBefore){
		if(this.isAllowRead){
			//Search the versions backwards
			for(int i=this.allVersions.size()-1; i>=0; i--){
				if(debug) System.out.println("Variable: Read is trying to find last committed version "+allVersions.get(i).toString());
				
				if(allVersions.get(i).isCommitted() && allVersions.get(i).getTimestamp()<timestampBefore){
					return this.allVersions.get(i).getValue();
				}
			}
		}
		return -1;
	}
	/*
	 * @return the latest version, which may not necessarily be committed
	 */
	public int readLatest(){
		if(this.isAllowRead){
			return this.allVersions.get(this.lastCommittedVersion).getValue();
		}
		return -1;
	}
	/*
	 * When a currently active transaction writes to a variable, it create a new version
	 * @value is the new value
	 * @timestamp is the current timestamp
	 * @transactionNumber is the current transaction doing this operation
	 */
	public void write(int value, int timestamp, int transactionNumber){
		boolean isCommitted = false;
		this.allVersions.add(new Version(value, timestamp, transactionNumber, isCommitted));
		this.latestVersion = this.allVersions.size()-1;

	}
	/*
	 * For recovery purposes, if Variable.isAllowRead = false, then set it to true, 
	 * now that there is a current new update to it Set lastCommittedVersion to the currentVersion
	 * Add a before image and timestamp to the list of versions
	 * @committingTransaction indicates which transaction is committing
	 * @return the committed value
	 */
	public int commit(int committingTransaction){
		//This means setting the versions written by the committing transaction and set that 
		//as the current version
		
		//Get the version with the latest timestamp of @committingTransaction from the list of versions
		int maxTimestamp = -1;
		int newCurrVersionIndex = -1; 
		for(int i=0; i<this.allVersions.size(); i++){
			Version version = this.allVersions.get(i);

			//A version with the transaction number of -1 means that it was loaded by the database
			if((version.getFromTransactionNumber()==committingTransaction || version.getFromTransactionNumber()==-1)
					&& version.getTimestamp()>maxTimestamp
					){
				newCurrVersionIndex = i;
				maxTimestamp = version.getTimestamp();
			}
		}
		if(debug) System.out.println("Variable: Last committed version located at "+lastCommittedVersion+" All versions="+toStringAllVersions());
		
		this.lastCommittedVersion = newCurrVersionIndex;
		
		if(debug) System.out.println("Variable: T"+committingTransaction+" is committing "+this.allVersions.get(lastCommittedVersion).toString());

		this.allVersions.get(lastCommittedVersion).setCommitted();
		return this.allVersions.get(lastCommittedVersion).getValue();
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
	/******************************************************************************************************
	 * Recovery methods
	 ******************************************************************************************************/
	/*
	 * If this variable is replicated at other sites, then reset the pointers to the versions on this variable,
	 * setting them unreadable until a transaction writes to it.
	 */
	public void resetVersionIndexes(){
		this.lastCommittedVersion = -1;
		this.latestVersion = -1;
	}
	/******************************************************************************************************
	 * Setter, getter methods
	 ******************************************************************************************************/
	/*
	 * toString function for Variable
	 */
	public String toString(){ 
		return "Latest version:"+allVersions.get(this.latestVersion).getTimestamp()+" " +allVersions.get(this.latestVersion).getValue() + "\n"+
				"Latest committed version:"+allVersions.get(this.lastCommittedVersion).getTimestamp()+" "+allVersions.get(this.lastCommittedVersion).getValue()+"\n";
	}
	public String toStringLatestCommitted(){
		return ""+allVersions.get(this.lastCommittedVersion).getValue();
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
	private String toStringAllVersions(){
		String str = "[";
		for(Version v: this.allVersions){
			str = str + " "+v.toString();
		}
		str=str+"]";
		return str;
	}
	
	
}
