import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class DataManager {
	//Integer refers to the site index
	private Map<Integer, Site> allSites = new HashMap<Integer, Site>();
	
	/*
	 * Loads the sites and their variables, including the variable's initial values.
	 */
	public void loadDatabase(){
		//20 Variables
		//10 sites
		//Create 10 sites
		for(int i=1; i<=10; i++){
			allSites.put(i, new Site(i));
		}
				
		//load even variables into the right one
		//Even indexed variables are at all sites.
		for(Integer key: allSites.keySet()){
			Site site = allSites.get(key);
			for(int indexVariable=2; indexVariable<=20; indexVariable = indexVariable+2){
				int value = 10 * indexVariable;
				Variable evenVariable = new Variable(indexVariable, value, 0, true);
				site.addVariableAtSite(indexVariable, evenVariable, true);
			}
		}
				
		//Load odd variables into the appropriate site
		//The odd indexed variables are at one site each (i.e. 1 + index number mod 10 )
		for(int indexVariable=1; indexVariable<=20; indexVariable = indexVariable+2){
			int value = 10 * indexVariable;
			Variable oddVariable = new Variable(indexVariable, value, 0, true);
			
			//Store the variable at siteIndex = 1 + index number mod 10
			int siteIndex = 1 + (indexVariable % 10);
			allSites.get(siteIndex).addVariableAtSite(indexVariable, oddVariable, false);
			
		}
	}
	
	/*
	 * @return the site indexes where @variableIndex is in and where the sites aren't down
	 * i.e. @variableIndex = 2, then [1, 2, ..., 20] since it is at all sites
	 */
	public List<Integer> getAvailableSitesVariablesWhere(int variableIndex){
		List<Integer> siteIndexes = new ArrayList<Integer>();
		for(Integer key: allSites.keySet()){
			Site site = allSites.get(key);
			if(site.isSiteContainVariableIndex(variableIndex) && !site.isSiteDown()){
				siteIndexes.add(key);
			}
		}
		return siteIndexes;
	}
	
	/******************************************************************************************************
	 * The following methods must be called by Transaction. 
	 * This way the variables are not changed without locks or read without protocols.
	 ******************************************************************************************************/
	/*
	 * Read the version of @variableIndex at site, @siteIndex, where the version's timestamp
	 * is less than @timestampBefore i.e. read the COMMITTED version before @timestampBefore.
	 * This method is used by the multiversion protocol.
	 * @siteIndex is the site index to read from, which is decided from Transaction.
	 * @variableIndex is the variable index to read, also given from Transaction.
	 * @return is the value read
	 */
	public int readCommitted(int siteIndex, int variableIndex, int timestampBefore){
		return this.allSites.get(siteIndex).readCommitted( variableIndex, timestampBefore);
	}
	/*
	 * @siteIndex is the site index to read from, which is decided from Transaction.
	 * @variableIndex is the variable index to read, also given from Transaction.
	 * @return the latest version, which may not necessarily be committed.
	 */
	public int readLatest(int siteIndex, int variableIndex){
		return this.allSites.get(siteIndex).readLatest(variableIndex);
	}
	/*
	 * When a currently active transaction writes to a variable, it create a new version.
	 * @siteIndex is the site index to write to, which is decided from Transaction.
	 * @variableIndex is the variable index to write to, also given from Transaction.
	 * @value is the new value
	 * @currentTimestamp is the current timestamp
	 * @transactionNumber is the current transaction doing this operation
	 */
	public void write(int siteIndex, int variableIndex, int value, int currentTimestamp, int transactionNumber){
		this.allSites.get(siteIndex).write(variableIndex, value, currentTimestamp, transactionNumber);
	}
	/*
	 * For recovery purposes, if Variable.isAllowRead = false, then set it to true, 
	 * now that there is a current new update to it Set lastCommittedVersion to the currentVersion
	 * Add a before image and timestamp to the list of versions
	 * @siteIndex is the site index, which is decided from Transaction.
	 * @variableIndex is the variable index, also given from Transaction.
	 * @committingTransaction indicates which transaction is committing
	 * @currentTimestamp is the current timestamp
	 * @transactionNumber is the current transaction doing this operation
	 */
	public void commit(int siteIndex, int variableIndex, int committingTransaction, int currentTimestamp, int transactionNumber){
		//int variableIndex, int committingTransaction, int currentTimestamp, int transactionNumber
		this.allSites.get(siteIndex).commit(variableIndex, committingTransaction, currentTimestamp, transactionNumber);

	}
	/*
	 * Return the lock that is conflicting with a hypothetical lock of the following properties:
	 * @transactionNumber is the transaction number of the transaction requesting the lock
	 * @variableIndex is the variable index the hypothetical lock would hold on
	 * @isLockRequestReadOnly refers to the type of the hypothetical lock
	 * @return the lock or null if not successful
	 */
	public Lock getConflictingLock(int transactionNumber, int variableIndex, boolean isLockRequestReadOnly){
		List<Integer> siteIndexesToWrite  = this.getAvailableSitesVariablesWhere(variableIndex);
		Lock conflictingLock = null;
		for(Integer siteIndex: siteIndexesToWrite){
			conflictingLock = this.getSite(siteIndex).getConflictingLock(transactionNumber, variableIndex, false);
			if(conflictingLock!=null){
				return conflictingLock;
			}
		}
		return conflictingLock;
	}
	/******************************************************************************************************
	 * Setter, getter, and dump method
	 ******************************************************************************************************/
	/*
	 * @return the site given the @siteIndex
	 */
	public Site getSite(int siteIndex){
		return this.allSites.get(siteIndex);
	}
	
	/*
	 * Give the committed values of all copies of all variables at all sites, sorted per site
	 */
	public void dump(){
		//Go through each site
		SortedSet<Integer> keys = new TreeSet<Integer>(this.allSites.keySet());
		for(Integer siteIndex: keys){
			Site site = this.allSites.get(siteIndex);
			System.out.println("DM: Site Index: "+siteIndex);
			System.out.println(site.getVariableToString());
		}
	}
	/*
	 * Gives the committed values of all copies of all variables at site @i
	 */
	public void dumpSite(int i){
		Site site = this.allSites.get(i);
		System.out.println("DM: Site Index: "+i);
		System.out.println("\tVariables in site "+i+": "+site.getVariableToString());
	}
	/*
	 * Gives the committed values of all copies of variable @xj at all sites
	 */
	public void dumpVariable(int xj){
		//Go through each site
		SortedSet<Integer> keys = new TreeSet<Integer>(this.allSites.keySet());
		for(Integer siteIndex: keys){
			Site site = this.allSites.get(siteIndex);
			System.out.println("DM: Site Index: "+siteIndex);
			System.out.println("\tVariables in site "+siteIndex+": "+site.getVariableToString(xj));
		}
	}
}
