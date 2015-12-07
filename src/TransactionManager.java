import java.util.ArrayList;
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
		//TODO:Check conflict with the lock table at each site
		//checkIsConflict();
		//If there aren't any deadlocks continue 
		//TODO check if the transaction number is active
		
		if(command[0].equals("beginRO")){
			
			// command = ["beginRO", transactionNum]
			//If command = beginRO,
			//Create a Transaction and a thread; add Transaction to activeTransactions.
			int transactionNumber = Integer.parseInt(command[1]);
			ROTransaction roT = new ROTransaction(this.dataManager, transactionNumber, currentTimestamp );
			this.activeTransactions.put(transactionNumber, roT);
			
			System.out.println("New ROTransaction: T"+transactionNumber);
			
			return true;
		}else if(command[0].equals("begin")){
			
			// command = ["begin", transactionNum]
			//Create a Transaction with threads for each site; Add Transaction to activeTransactions. 
			int transactionNumber = Integer.parseInt(command[1]);
			RWTransaction rwT = new RWTransaction(this.dataManager, transactionNumber, currentTimestamp);
			this.activeTransactions.put(transactionNumber, rwT);
			
			System.out.println("New RWTransaction: T"+transactionNumber);

			return true;
		}else if(command[0].equals("W")){
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
						//Acquire a write lock from the site's lock manager
						Lock lock = this.dataManager.getSite(siteIndex).requestLock(currentTimestamp, transactionNumber, variableIndex, false);
						
						//If the lock was not acquired, then either wait or die, based on the timestamps of the 
						//conflicting transactions
						if(lock==null){
							
							//Get the conflicting lock that is preventing this transaction from getting a lock at a PARTICULAR site.
							Lock conflictingLock = this.dataManager.getSite(siteIndex).getConflictingLock(transactionNumber, variableIndex, false);
							Transaction conflictingTransaction = this.activeTransactions.get(conflictingLock.getTransactionNumber());
							
							//If T tries to access a lock held by an older transaction (one with a lesser timestamp), 
							//then T aborts. 
							if(currentTransaction.getBeginningTimestamp()>conflictingTransaction.getBeginningTimestamp()){
								currentTransaction.abort();
							}else{
								//Otherwise T waits for the other transaction to complete.
								//Add this command to the queue
								this.activeTransactions.get(transactionNumber).addQueuedOperation(command);
								queueTransaction(transactionNumber);
								return false;
							}
							
							
						}
						
					}
					
				}
				
				//At this point all needed locks are acquired, so write to a new version of the variables at ALL sites
				currentTransaction.processOperation("W", command, currentTimestamp);
			}else{
				//Add this command to the queue
				this.queuedTransactions.get(transactionNumber).addQueuedOperation(command);
				runQueuedTransaction(transactionNumber, currentTimestamp);
			}
			return true;
		}else if(command[0].equals("R")){
			
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
						if(rwTransaction.isHasReadLock(siteIndex, variableIndex)){
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
							Lock lock = this.dataManager.getSite(siteIndex).requestLock(currentTimestamp, transactionNumber, variableIndex, false);
							if(lock!=null){
								
								String[] modifyCommand = new String[]{command[0], command[1], command[2], siteIndex+""};
								rwTransaction.processOperation("R", modifyCommand, currentTimestamp);
															
								isLockAcquired = true;
								break;
								
							}
						}
						
						//If the lock was not acquired, then either wait or die, based on the timestamps of the 
						//conflicting transactions
						if(!isLockAcquired){
	
							//Get the conflicting lock that is preventing this transaction from getting a lock.
							Lock conflictingLock = this.dataManager.getConflictingLock(transactionNumber, variableIndex, false);
							Transaction conflictingTransaction = this.activeTransactions.get(conflictingLock.getTransactionNumber());
							
							//If T tries to access a lock held by an older transaction (one with a lesser timestamp), 
							//then T aborts. 
							if(rwTransaction.getBeginningTimestamp()>conflictingTransaction.getBeginningTimestamp()){
								rwTransaction.abort();
							}else{
								//Otherwise T waits for the other transaction to complete.
								//Add this command to the queue
								this.activeTransactions.get(transactionNumber).addQueuedOperation(command);
								queueTransaction(transactionNumber);
								return false;
							}
							
							
						}
						
					}
					
				}
			}else{
				//Add the command to the queue
				this.queuedTransactions.get(transactionNumber).addQueuedOperation(command);
				runQueuedTransaction(transactionNumber, currentTimestamp);
			}
			return true;
		}else if(command[0].equals("end")){
			//command = ["end", transaction number]
			
			int transactionNumber = Integer.parseInt(command[1]);
			Transaction currentTransaction = this.activeTransactions.get(transactionNumber);

			//Report whether the transaction can commit
			//If the transaction was a read write transaction: validate 
			if(!currentTransaction.getReadOnly()){
				
				//If valid, commit all transactions and release the transaction’s locks i.e. Transaction.releaseLocks() 
				//Otherwise, abort(current transaction)
				RWTransaction rwTransaction = (RWTransaction) this.activeTransactions.get(transactionNumber);
				
				if(validate(transactionNumber)){
					rwTransaction.commit(currentTimestamp);
				}else{
					//If it is not valid, abort it
					rwTransaction.abort();
				}
				
				//Once the transaction commits/aborts, it can release the locks
				rwTransaction.releaseLocks(currentTimestamp);
				
				
			}else{
				//Otherwise, if the transaction was read only, then say yes
				System.out.println("T"+transactionNumber+" can commit");
				ROTransaction roTransaction = (ROTransaction) this.activeTransactions.get(transactionNumber);
				roTransaction.commit(currentTimestamp);
			}
			
			//At end, remove this transaction from active transaction
			this.activeTransactions.remove(transactionNumber);
			
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

		}
		return true;
		/*
		 * TODO:
		If command = recover, Recover.recover(Site)
		If command = fail,
		Call Site.fail() for the site specified in the command
		
		*/
	}
	/*
	 * Wait-die protocol here
	 * @return Set<Transactions> conflicting transactions
	 */
	/*private List<Transaction> checkIsConflict(){ 
		//TODO: wait-die protocol here
		return null;
	}
	/*
	 * Restore before images of all sites accessed by t Release the locks held by t
	 */
	/*private void abort(Transaction t){
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
		List<Integer> sitesAccessed = transaction.getSiteIndexesAccessed();
		for(int key: sitesAccessed){
			//If one of the sites has failed, then return false
			this.dataManager.getSite(key).isSiteDownAfter(transaction.getBeginningTimestamp());
			
		}
		return true;
	}
	/*
	 * Try to run the queued transactions
	 */
	private void runQueuedTransaction(int transactionNumber, int currentTimestamp){
		//Set the queued transaction to active
		activateTransaction(transactionNumber);
		
		for(String[] command: this.activeTransactions.get(transactionNumber).getQueuedOperations()){
			//isExecuted = true, if the operation is not queued
			boolean isExecuted = processOperation( command, currentTimestamp);
			if(!isExecuted){
				//If it wasn't executed, then continue to wait
				queueTransaction(transactionNumber);
				break;
			}else{
				//Otherwise, remove that command from the queue
				this.activeTransactions.get(transactionNumber).removeQueuedOperation(command);
			}
		}
	}
	/*
	 * Queue the transaction and remove it from the active transactions
	 */
	private void queueTransaction(int transactionNumber){
		this.queuedTransactions.put(transactionNumber, this.activeTransactions.get(transactionNumber));
		this.activeTransactions.remove(transactionNumber);
	}
	/*
	 * Set transaction to active and remove it from the queue transactions
	 */
	private void activateTransaction(int transactionNumber){
		this.activeTransactions.put(transactionNumber, this.queuedTransactions.get(transactionNumber));
		this.queuedTransactions.remove(transactionNumber);
	}
}
