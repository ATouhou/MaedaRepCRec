import java.util.HashMap;
import java.util.Map;

//This is the basic template for every type of transaction
public interface Transaction {
	/*
		A transaction can be read only or read-write. 
		If it is an ROTransaction, then getReadOnly() = true.
		If it is an RWTransaction, then getReadOnly() = false.
	 */
	void setReadOnly(boolean input);
	boolean getReadOnly();
	
	//Integer refers to the site index
	Map<Integer, Site> siteAccessed = new HashMap<Integer, Site>();
	
	void processOperation(String operation, String[] inputs, int currentTimestamp);
	
	void releaseLocks();
	
	void commit();
	
	Map<Integer, Site> getSitesAccessed();
	
	//This returns the timstamp at which the transaction began
	int getBeginningTimestamp();
}
