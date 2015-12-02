import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionManager {
	//Integer refers to the site index
	Map<Integer, Site> allSites = new HashMap<Integer, Site>();
	
	//Integer refers to the transaction number
	Map<Integer, Transaction> activeTransactions = new HashMap<Integer, Transaction>();
	
	public TransactionManager(){
		//Load the database into the structure
		loadDatabase();
	}
	
	/*
	 * Loads the sites and their variables, including the variable's initial values.
	 */
	private void loadDatabase(){
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
				Variable evenVariable = new Variable(indexVariable, value, 0);
				site.addVariableAtSite(evenVariable);
			}
		}
				
		//Load odd variables into the appropriate site
		//The odd indexed variables are at one site each (i.e. 1 + index number mod 10 )
		for(int indexVariable=1; indexVariable<=20; indexVariable = indexVariable+2){
			int value = 10 * indexVariable;
			Variable oddVariable = new Variable(indexVariable, value, 0);
			
			//Store the variable at siteIndex = 1 + index number mod 10
			int siteIndex = 1 + (indexVariable % 10);
			allSites.get(siteIndex).addVariableAtSite(oddVariable);
			
		}
	}
	/*
	 * 	@command is the array of details about the incoming command; it is the output of Parser.parse
	 * 	@timestamp is the timestamp of the current incoming command. The timestamp is given by the Parser.parse(),
	 * 		as it increments
	 */
	public void processOperation( String[] command, int timestamp){
		//TODO:Check conflict with the lock table at each site
		//checkIsConflict();
		//If there aren't any deadlocks continue 
		
		if(command[0].equals("beginRO")){
			//If command = beginRO,
			//Create a Transaction and a thread; add Transaction to activeTransactions.
			int transactionNumber = Integer.parseInt(command[1]);
			this.activeTransactions.put(transactionNumber, value);
		}
		/*
		 * If command = begin,
		Create a Transaction with threads for each site; Add Transaction to activeTransactions. 
		If command = write,
		Check directory to see which sites the copies are. Send the write to the appropriate transaction.
		If command = read,
		Spawn a ReadTransaction thread and read a recent copy
		If command = end,
		If the transaction was a read write transaction:
		validate(transactionNumber)
		If valid, commit all transactions and release the transaction’s locks i.e. Transaction.releaseLocks() Otherwise, abort(current transaction)
		If command = recover, Recover.recover(Site)
		If command = fail,
		Call Site.fail() for the site specified in the command
		If command = dump,
		for all Sites, Site.dump()
		*/
	}
	/*
	 * Wait-die protocol here
	 * @return Set<Transactions> conflicting transactions
	 */
	private List<Transaction> checkIsConflict(){ 
		//TODO: wait-die protocol here
		return null;
	}
	/*
	 * Restore before images of all sites accessed by t Release the locks held by t
	 */
	private void abort(Transaction t){
		//Restore before images of all sites accessed by t Release the locks held by t
	}
	/*
	 * If one of the sites accessed has failed after the transaction began, return false
	 * @return boolean
	 */
	private boolean validate(int transactionNumber){ 
		//If one of the sites accessed has failed after the transaction ran, return false
		Transaction transaction = this.activeTransactions.get(transactionNumber);
		
		//Loop through all the sites the transaction has accessed
		Map<Integer, Site> sitesAccessed = transaction.getSitesAccessed();
		for(int key: sitesAccessed.keySet()){
			//If one of the sites has failed, then return false
			sitesAccessed.get(key).isSiteDownAfter(transaction.getBeginningTimestamp());
			
		}
		return true;
	}
}
