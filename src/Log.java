import java.util.ArrayList;
import java.util.List;

/*
 * The log keeps track of all actions on a site:
 * - commit. details = ["commit", transactionNumber, variableIndex, valueCommitted]
 * - abort
 * - locks given. details = ["give lock",transactionNumber, variableIndex, isLockRequestReadOnly]
 * - lock release. details = ["release lock",transactionNumber, variableIndex, isLockRequestReadOnly]
 * - recover. details = ["recover"]
 * - fail. details = ["fail"]
 */
public class Log {
	
	private List<Event> logs = new ArrayList<Event>();
	
	/*
	 * Return the timestamp in which the last fail began for this site
	 */
	public int findLastFailTimestamp(){
		int timestampFind = -1;
		//Iterate backwards
		for(int i=logs.size()-1; i>=0; i--){
			Event event = logs.get(i);
			if(event.details[0].equals("fail")){
				return event.timestamp;
			}
		}
		return timestampFind;
	}
	public void addEvent(int timestamp, String[] details){
		logs.add(new Event(timestamp, details));
	}
	
	private class Event{
		int timestamp;
		String[] details;
		
		Event(int timestamp, String[] details){
			this.timestamp = timestamp;
			this.details = details;
		}
	}
}
