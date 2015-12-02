import java.util.ArrayList;
import java.util.List;

public class Log {
	private List<Event> logs = new ArrayList<Event>();
	
	/*
	 * Return the timestamp in which the last fail began for this site
	 */
	public int findLastFailTimestamp(){
		int timestampFind = -1;
		//Iterate backwards
		for(int i=logs.size()-1; i>=0; i++){
			if(logs.get(i).details[0].equals("fail")){
				return logs.get(i).timestamp;
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
