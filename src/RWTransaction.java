import java.util.Map;

public class RWTransaction implements Transaction{
	boolean isReadOnly = false;

	@Override
	public void processOperation(String operation, String[] inputs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void releaseLocks() {
		// TODO Auto-generated method stub
		
	}

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
		// TODO Auto-generated method stub
		return 0;
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
