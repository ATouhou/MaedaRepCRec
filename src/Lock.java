/*
 * This is a lock object, which contains information on
 * - which transaction holds it,
 * - which site the lock is on
 * - which variable it is locking
 * - whether it is read or read-write lock
 */
public class Lock{
	private int transactionNumber = -1;
	private int variableIndex = -1;
	private int siteIndex = -1;
	
	//False indicates that it is a read-write lock, otherwise only a read lock
	private boolean isReadOnly = false;
	
	/*
	 * Instantiates the Lock object
	 */
	public Lock(int transactionNumber, int siteIndex, int variableIndex, boolean isReadOnly){
		this.transactionNumber = transactionNumber;
		this.variableIndex = variableIndex;
		this.siteIndex = siteIndex;
		this.isReadOnly = isReadOnly;
	}
	/*
	 * Tests if a given lock is the exact same lock as this, which is useful for authenticating an operation requiring a lock
	 */
	public boolean isEqual(Lock testLock){
		if(testLock.isReadOnly() == isReadOnly
				&& testLock.getTransactionNumber() == this.transactionNumber
				&& testLock.getLockedVariableIndex() == this.getLockedVariableIndex()
				&& testLock.getSiteIndex() == this.siteIndex){
			return true;
		}
		return false;
	}
	/*
	 * Getter functions for the Lock's members
	 */
	public int getTransactionNumber() {
		return transactionNumber;
	}

	public int getLockedVariableIndex(){
		return this.variableIndex;
	}
	
	public int getSiteIndex(){
		return this.siteIndex;
	}
	
	public boolean isReadOnly(){
		return this.isReadOnly;
	}
	
	public void setIsReadOnly(boolean newSetting){
		this.isReadOnly = newSetting;
	}
	
	
}