import java.util.List;
import java.util.Map;

public class ROTransaction implements Transaction{
	private boolean isReadOnly = true;
	private DataManager dm = null; 
	
	private int transactionNumber = -1;

	//This is timestamp at which the transaction was created
	private int beginningTimestamp = -1;
	
	public ROTransaction(DataManager dm, int transactionNumber, int beginningTimestamp){
		this.dm = dm;
		this.transactionNumber = transactionNumber;
		this.beginningTimestamp = beginningTimestamp;
	}
	/*
	 * processOperation() = read(Variable x) - return the version from when this transaction started.
	 */
	@Override
	public void processOperation(String operation, String[] inputs, int currentTimestamp) {

		//command = [ "R", transaction number, index variable]

		//Get the possible site indexes the transaction to read from
		int variableIndex = Integer.parseInt(inputs[2]);
		List<Integer> siteIndexesToReadFrom  = this.dm.getAvailableSitesVariablesWhere(variableIndex);

		//Read from any one site
		Site siteToRead = this.dm.getSite(siteIndexesToReadFrom.get(0));
		siteToRead.getVariable(variableIndex).read();
		
		
	}

	@Override
	public void releaseLocks() {
		// TODO Auto-generated method stub
		
	}

	/*
	 * Since this is a read-only transaction, there is no chance of abort since according to the available 
	 * copies algorithm a read-only transaction need not to check for validation at commit (see slides).
	 */
	@Override
	public void commit() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<Integer, Site> getSitesAccessed() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getBeginningTimestamp() {
		return this.beginningTimestamp;
	}

	@Override
	public void setReadOnly(boolean input) {
		this.isReadOnly = input;
	}

	@Override
	public boolean getReadOnly() {
		return this.isReadOnly;
	}

}
