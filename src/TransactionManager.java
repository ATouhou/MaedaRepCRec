import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionManager {
	
	//Data Manager manages all the sites and knows where all the variables are
	DataManager dataManager = new DataManager();
	
	//Integer refers to the transaction number
	Map<Integer, Transaction> activeTransactions = new HashMap<Integer, Transaction>();
	
	Map<Integer, Transaction> queuedTransactions = new HashMap<Integer, Transaction>();

	public TransactionManager(){
		//Load the database into the structure
		dataManager.loadDatabase();
	}
	
	
	/*
	 * 	@command is the array of details about the incoming command; it is the output of Parser.parse
	 * 	@currentTimestamp is the currentTimestamp.
	 * 	@return true, if the operation is not queued
	 */
	public boolean processOperation( String[] command, int currentTimestamp){
		//TODO check if the transaction number is active
		
		if(command[0].equals("beginRO")){
			
			// command = ["beginRO", transactionNum]
			//If command = beginRO,
			//Create a Transaction and a thread; add Transaction to activeTransactions.
			int transactionNumber = Integer.parseInt(command[1]);
			ROTransaction roT = new ROTransaction(this.dataManager, transactionNumber, currentTimestamp );
			this.activeTransactions.put(transactionNumber, roT);
			
			System.out.println("TM: New ROTransaction: T"+transactionNumber);
			
			return true;
		}else if(command[0].equals("begin")){
			
			// command = ["begin", transactionNum]
			//Create a Transaction with threads for each site; Add Transaction to activeTransactions. 
			int transactionNumber = Integer.parseInt(command[1]);
			RWTransaction rwT = new RWTransaction(this.dataManager, transactionNumber, currentTimestamp);
			this.activeTransactions.put(transactionNumber, rwT);
			
			System.out.println("TM: New RWTransaction: T"+transactionNumber);

			return true;
		}else if(command[0].equals("W")){
			//First check if the transaction is even created. If it was aborted before, we may still receive instructions
			if(isTransactionExist(Integer.parseInt(command[1]))){
				return processWrite( command, currentTimestamp);
			}
			return true;
		}else if(command[0].equals("R")){
			if(isTransactionExist(Integer.parseInt(command[1]))){
				return processRead( command, currentTimestamp);
			}
			return true;
		}else if(command[0].equals("end")){
			//command = ["end", transaction number]
			
			int transactionNumber = Integer.parseInt(command[1]);
			//First check if the transaction is active or queued
			if(this.activeTransactions.containsKey(transactionNumber)){
				
				Transaction currentTransaction = this.activeTransactions.get(transactionNumber);
	
				//Report whether the transaction can commit
				//If the transaction was a read write transaction: validate 
				if(!currentTransaction.getReadOnly()){
					
					//If valid, commit all transactions and release the transaction’s locks i.e. Transaction.releaseLocks() 
					//Otherwise, abort(current transaction)
					RWTransaction rwTransaction = (RWTransaction) this.activeTransactions.get(transactionNumber);
					
					if(validate(transactionNumber)){
						System.out.println("TM: T"+transactionNumber+" is valid. Commit.");
						rwTransaction.commit(currentTimestamp);
					}else{
						//If it is not valid, abort it
						abort(transactionNumber, currentTimestamp);
					}
					
				}else{
					//Otherwise, if the transaction was read only, then say yes
					System.out.println("TM: T"+transactionNumber+" is valid. Commit.");
					ROTransaction roTransaction = (ROTransaction) this.activeTransactions.get(transactionNumber);
					roTransaction.commit(currentTimestamp);
				}
				
				//Executing the transactions that were waiting for this transaction
				int transactionNumberWait = getTransactionWaitingFor(transactionNumber);
				if(transactionNumberWait!=-1){
					System.out.println("TM: Locks are released, run T"+transactionNumberWait+" who waited on T"+transactionNumber);
					runQueuedTransaction(transactionNumberWait, currentTimestamp);
				}
				//At end, remove this transaction from the lists transaction
				this.activeTransactions.remove(transactionNumber);
				
			}else{
				//If the transaction is queued, then
				//add this command to the queue
				//run the queued commands
				this.queuedTransactions.get(transactionNumber).addQueuedOperation(command);
				runQueuedTransaction(transactionNumber, currentTimestamp);
			}
			return true;
		}else if(command[0].equals("dump()")){
			
			//Give the committed values of all copies of all variables at all sites, sorted per site
			this.dataManager.dump();
			
		}else if(command[0].equals("dumpSite")){
			
			//command = ["dumpSite", site Index]
			int siteIndex = Integer.parseInt(command[1]);
			//Gives the committed values of all copies of all variables at site i
			this.dataManager.dumpSite(siteIndex);
			
		}else if(command[0].equals("dumpVariable")){
			
			//command = ["dumpSite", variable Index]
			int variableIndex = Integer.parseInt(command[1]);
			//Gives the committed values of all copies of variable xj at all sites
			this.dataManager.dumpVariable(variableIndex);

		}else if(command[0].equals("fail")){
			//command = ["fail", siteIndex]
			int siteIndexToFail = Integer.parseInt(command[1]);
			this.dataManager.getSite(siteIndexToFail).fail(currentTimestamp);
		}else if(command[0].equals("recover")){
			//command = ["recover", siteIndex]
			int siteIndexToRecover = Integer.parseInt(command[1]);
			this.dataManager.getSite(siteIndexToRecover).recover(currentTimestamp);
		}
		return true;
		
	}
	/******************************************************************************************************
	 * Execute the commands given by parser
	 ******************************************************************************************************/
	/*
	 *  This method obtains necessary locks if the transaction is read-write and if those locks are not obtained
	 *  then the wait-die protocol kicks in.
	 * 	@command is the array of details about the incoming command; it is the output of Parser.parse
	 * 	@currentTimestamp is the currentTimestamp.
	 * 	@return true, if the operation is not queued
	 */
	public boolean processWrite( String[] command, int currentTimestamp){
		// command = ["W", transaction number, index variable, value to write]
		//If command = write,
		//Send the operation to the appropriateTransaction
		int transactionNumber = Integer.parseInt(command[1]);
		
		//First check if the transaction is active or queued
		if(this.activeTransactions.containsKey(transactionNumber)){

			//Check if the locks are already acquired
			//If the lock is acquired successfully, then the write operation happens successfully.
			//If the lock isn't acquired successfully, then either wait or die, depending on the 
			//timestamps of the conflicting transactions
			
			//Retrieve the current transaction 
			RWTransaction currentTransaction = (RWTransaction) this.activeTransactions.get(transactionNumber);
			
			//Check data manager to see which sites the copies are. 
			//Acquire locks and perform the write operation on ALL sites
			int variableIndex = Integer.parseInt(command[2]);
			List<Integer> siteIndexesToWrite  = this.dataManager.getAvailableSitesVariablesWhere(variableIndex);
			
			for(Integer siteIndex: siteIndexesToWrite){
				if(!currentTransaction.isHasWriteLock(siteIndex, variableIndex)){
					
					Lock lock = null;
					
					//If the transaction does have a read lock on the data, then upgrade from read to write lock
					if(currentTransaction.isHasReadOnlyLock(siteIndex, variableIndex)){
						//Request an upgrade from the site's lock manager
						//Upgrade the lock in the transaction
						lock = this.dataManager.getSite(siteIndex).upgradeRequest(currentTimestamp, transactionNumber, variableIndex);
						currentTransaction.upgradeLock(transactionNumber, siteIndex, variableIndex);
					}else{
						//Acquire a write lock from the site's lock manager
						lock = this.dataManager.getSite(siteIndex).requestLock(currentTimestamp, transactionNumber, variableIndex, false);
					}
					
					//If the lock was not acquired, then either wait or die, based on the timestamps of the 
					//conflicting transactions
					if(lock==null){
						
						//Get the transaction conflicting with this transaction
						Transaction conflictingTransaction = this.getConflictingTransaction(transactionNumber, siteIndex, variableIndex);
								
						//If T tries to access a lock held by an older transaction (one with a lesser timestamp), 
						//then T aborts. 
						if(currentTransaction.getBeginningTimestamp()>conflictingTransaction.getBeginningTimestamp()){
							System.out.println("TM: Abort T"+transactionNumber+" because it is younger than T"+conflictingTransaction.getTransactionNumber());
							abort(transactionNumber, currentTimestamp);
							return true;
						}else{
							//Otherwise T waits for the other transaction to complete.
							//Add this command to the queue
							System.out.println("TM: T"+transactionNumber+" waits for T"+conflictingTransaction.getTransactionNumber());
							this.activeTransactions.get(transactionNumber).addQueuedOperation(command);
							queueTransaction(transactionNumber, conflictingTransaction.getTransactionNumber());
							return false;
						}
					}else{
						//If the lock was acquired, make sure that the transaction knows about it
						currentTransaction.addLock(lock);
					}
				}
			}
			
			//At this point all needed locks are acquired, so write to a new version of the variables at ALL sites
			currentTransaction.processOperation("W", command, currentTimestamp);
		}else{
			//If the transaction is queued, then
			//add this command to the queue
			//run the queued commands
			this.queuedTransactions.get(transactionNumber).addQueuedOperation(command);
			runQueuedTransaction(transactionNumber, currentTimestamp);
		}
		return true;
	}
	/*
	 *  This method deals with multiversion reads or lock-required reads, depending on the transaction.
	 *  Queueing of transaction via wait-die protocol is also done here.
	 * 	@command is the array of details about the incoming command; it is the output of Parser.parse
	 * 	@currentTimestamp is the currentTimestamp.
	 * 	@return true, if the operation is not queued
	 */
	public boolean processRead( String[] command, int currentTimestamp){
		//command = [ "R", transaction number, index variable]
		
		//If the transaction is a read-only transaction, 
		// 	read a committed copy before the transaction began (multiversion protocol).
		//If the transaction is a read-write transaction,
		//	acquires a lock as in 2pl and read from one copy (available copies algorithm)

		int transactionNumber = Integer.parseInt(command[1]);
		
		//First check if the transaction is active or queued
		if(this.activeTransactions.containsKey(transactionNumber)){
			Transaction currentTransaction = this.activeTransactions.get(transactionNumber);

			if(currentTransaction.getReadOnly()){
				ROTransaction roTransaction = (ROTransaction) this.activeTransactions.get(transactionNumber);
				roTransaction.processOperation("R", command, currentTimestamp);
				
			}else{
				RWTransaction rwTransaction = (RWTransaction) this.activeTransactions.get(transactionNumber);
				
				//Check data manager to see which sites the copies are.
				//And which locks are already acquired
				//If there is no locks for one site, then acquire the lock
				
				int variableIndex = Integer.parseInt(command[2]);
				List<Integer> siteIndexesToWrite  = this.dataManager.getAvailableSitesVariablesWhere(variableIndex);
				
				boolean isRead = false;
				for(Integer siteIndex: siteIndexesToWrite){
					if(rwTransaction.isHasReadOrWriteLock(siteIndex, variableIndex)){
						//Once a read lock has been found, then read it, and then stop.
						
						//Add the index site to the command details
						String[] modifyCommand = new String[]{command[0], command[1], command[2], siteIndex+""};
						rwTransaction.processOperation("R", modifyCommand, currentTimestamp);
												
						isRead = true;
						break;
					}
				}
				
				if(!isRead){
					//If no read is done, because there aren't any locks, acquire a lock on any one site
					boolean isLockAcquired = false;
					for(Integer siteIndex: siteIndexesToWrite){
						Lock lock = this.dataManager.getSite(siteIndex).requestLock(currentTimestamp, transactionNumber, variableIndex, true);
						if(lock!=null){
							//Tell the transaction that the lock was acquired
							rwTransaction.addLock(lock);
							
							String[] modifyCommand = new String[]{command[0], command[1], command[2], siteIndex+""};
							rwTransaction.processOperation("R", modifyCommand, currentTimestamp);
														
							isLockAcquired = true;
							break;
						}
					}
					
					//If the lock was not acquired, then either wait or die, based on the timestamps of the 
					//conflicting transactions
					if(!isLockAcquired){

						//Get the transaction conflicting with this transaction
						Transaction conflictingTransaction = this.getConflictingTransaction(transactionNumber, -1, variableIndex);
												
						//If T tries to access a lock held by an older transaction (one with a lesser timestamp), 
						//then T aborts. 
						if(rwTransaction.getBeginningTimestamp()>conflictingTransaction.getBeginningTimestamp()){
							//rwTransaction.abort( currentTimestamp);
							abort(transactionNumber, currentTimestamp);
						}else{
							//Otherwise T waits for the other transaction to complete.
							//Add this command to the queue
							this.activeTransactions.get(transactionNumber).addQueuedOperation(command);
							queueTransaction(transactionNumber, conflictingTransaction.getTransactionNumber());
							return false;
						}
					}
				}
			}
		}else{
			//If the transaction is queued, 
			//add the command to the queue
			//run the queued commands
			this.queuedTransactions.get(transactionNumber).addQueuedOperation(command);
			runQueuedTransaction(transactionNumber, currentTimestamp);
		}
		return true;
	}
	
	/*
	 * TODO: ?Restore before images of all sites accessed by t Release the locks held by t
	 * Abort a given transaction and remove it from any lists we have i.e. activeTransaction, etc
	 */
	private void abort(int transactionNumber, int currentTimestamp){
		Transaction transaction = this.activeTransactions.get(transactionNumber);
		transaction.abort( currentTimestamp);
		
		//Remove from the list of transactions
		if (this.activeTransactions.containsKey(transactionNumber) ){
			this.activeTransactions.put(transactionNumber, null);
			this.activeTransactions.remove(transactionNumber) ; 
		}else{
			this.queuedTransactions.put(transactionNumber, null);
			this.queuedTransactions.remove(transactionNumber);
		}
		
	}
	/*
	 * If one of the sites accessed has failed after the transaction began, return false
	 * @return boolean
	 */
	private boolean validate(int transactionNumber){ 
		//If one of the sites accessed has failed after the transaction ran, return false
		Transaction transaction = this.activeTransactions.get(transactionNumber);
		
		//Loop through all the sites the transaction has accessed
		List<Integer> sitesAccessed = transaction.getSiteIndexesAccessed();
		for(int key: sitesAccessed){
			//If one of the sites has failed, then return false
			if(this.dataManager.getSite(key).isSiteDownAfter(transaction.getBeginningTimestamp())){
				return false;
			}
			
		}
		return true;
	}
	/******************************************************************************************************
	 * Dealing with queued transactions
	 ******************************************************************************************************/
	/*
	 * Try to run the queued transactions. Return false, if there any commands left in the queue. 
	 * Otherwise return true if all queued commands were executed.
	 * @transactionNumber is the transaction in the queue to execute
	 */
	private boolean runQueuedTransaction(int transactionNumber, int currentTimestamp){
		//The transaction number that is causing @transactionNumber to wait in the first place
		//int transactionNumberWait = this.queuedTransactions.get(transactionNumber).getTransactionWaitFor();
		
		//Set the queued transaction to active
		activateTransaction(transactionNumber);

		System.out.println("TM: Running queued commands of T"+transactionNumber+" "+ this.activeTransactions.get(transactionNumber).getQueuedOperationsToString());

		List<String[]> executed = new ArrayList<String[]>();
		
		for(String[] command: this.activeTransactions.get(transactionNumber).getQueuedOperations()){
			System.out.println("TM: Executing "+Arrays.toString(command));
			//isExecuted = true, if the operation is not queued
			boolean isExecuted = processOperation( command, currentTimestamp);
			if(!isExecuted){
				//If it wasn't executed, then continue to wait
				//Get the new conflicting transaction
				// command = ["W", transaction number, index variable, value to write] | [ "R", transaction number, index variable]
				int siteIndexToAccess = -1;
				int variableIndexToAccess = Integer.parseInt(command[2]);
				Transaction transactionWait = this.getConflictingTransaction(transactionNumber, siteIndexToAccess, variableIndexToAccess);
				
				queueTransaction(transactionNumber, transactionWait.getTransactionNumber());
				System.out.println("TM: T"+transactionNumber+" back to queue and wait for T"+transactionWait.getTransactionNumber());

				//Remove the already executed commands from queue
				for(String[] execs: executed){
					this.activeTransactions.get(transactionNumber).removeQueuedOperation(execs);
				}
				return false;
			}else{
				System.out.println("TM: T"+transactionNumber+" successfully executed queued "+Arrays.toString(command));
				//Otherwise, remove that command from the queue
				executed.add(command);
			}
		}
		return true;
	}
	/*
	 * Queue the transaction and remove it from the active transactions
	 * @transactionNumber is the transaction waiting for the transaction with @transactionNumberWait
	 */
	private void queueTransaction(int transactionNumber, int transactionNumberWait){
		//Move the transaction from active to queue
		this.queuedTransactions.put(transactionNumber, this.activeTransactions.get(transactionNumber));
		this.activeTransactions.remove(transactionNumber);
		System.out.println("TM: T"+transactionNumber+" moved from active to queue." );
		
		//Set the transaction number that @transactionNumber is waiting for
		this.queuedTransactions.get(transactionNumber).setToWaitFor(transactionNumberWait);
	}
	/*
	 * Set transaction to active and remove it from the queue transactions
	 */
	private void activateTransaction(int transactionNumber){
		//Move the transaction from queue to active
		this.activeTransactions.put(transactionNumber, this.queuedTransactions.get(transactionNumber));
		this.queuedTransactions.remove(transactionNumber);
		System.out.println("TM: T"+transactionNumber+" moved from queue to active. ");

		//Unset the transaction from wait to active
		this.activeTransactions.get(transactionNumber).setTransactionToActive();
	}
	/*
	 * @transactionNumber is the transaction denied the access due to a conflict
	 * @siteIndex and @variableIndex is what @transactionNumber wants to access.
	 *  Note! sometimes @siteIndex = -1, which means any conflict at any site.
	 * @return the transaction conflicting with @transactionNumber
	 */
	private Transaction getConflictingTransaction(int transactionNumber, int siteIndex, int variableIndex){
		if(siteIndex!=-1){
			//Get the conflicting lock that is preventing this transaction from getting a lock at a PARTICULAR site.
			Lock conflictingLock = this.dataManager.getSite(siteIndex).getConflictingLock(transactionNumber, variableIndex, false);
			
			//In some cases, the conflicting transaction is already queued, 
			//so it couldn't be found in active transactions --> also search in queued transactions
			Transaction conflictingTransaction = this.activeTransactions.containsKey(conflictingLock.getTransactionNumber()) ? 
					this.activeTransactions.get(conflictingLock.getTransactionNumber()) : this.queuedTransactions.get(conflictingLock.getTransactionNumber());
			return conflictingTransaction;
		}else{
			//Get the conflicting lock that is preventing this transaction from getting a lock.
			Lock conflictingLock = this.dataManager.getConflictingLock(transactionNumber, variableIndex, false);
			
			//In some cases, the conflicting transaction is already queued, 
			//so it couldn't be found in active transactions --> also search in queued transactions to retrieve transaction info
			Transaction conflictingTransaction = this.activeTransactions.containsKey(conflictingLock.getTransactionNumber()) ? 
					this.activeTransactions.get(conflictingLock.getTransactionNumber()) : this.queuedTransactions.get(conflictingLock.getTransactionNumber());
			return conflictingTransaction;
		}
	}
	
	/*
	 * @return the transaction number of the one waiting for @transactionNumberWait i.e. t1 waits for t2, input = 2, and return 1
	 */
	private int getTransactionWaitingFor(int transactionNumberWait){
		//Iterate through all the queued transaction
		for(int key: this.queuedTransactions.keySet()){
			Transaction trans = this.queuedTransactions.get(key);
			//return the one waiting for @transactionNumberWait
			if(trans.getTransactionWaitFor() == transactionNumberWait){
				return trans.getTransactionNumber();
			}
		}
		return -1;
	}
	/*
	 * Does the transaction exist? This manager may receive instructions for a particular transaction, but those transactions may have been aborted i.e. 
	 * not exists anymore.
	 */
	private boolean isTransactionExist(int transactionNumber){
		if(this.activeTransactions.containsKey(transactionNumber) || this.queuedTransactions.containsKey(transactionNumber)){
			return true;
		}
		System.out.println("TM: T"+transactionNumber+" doesn't exists.");
		return false;
	}
}
