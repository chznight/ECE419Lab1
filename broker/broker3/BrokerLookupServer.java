import java.net.*;
import java.io.*;
import java.util.HashMap;

/* class used by BrokerLookupServer */
/* relating name e.g. "nasdaq" with location e.g. {"localhost", 1111} */

class LookupTable implements Serializable {
	public String  broker_name;
	public BrokerLocation broker_location;
	
	/* constructors */
	public LookupTable() { 
		this.broker_name = ""; /*name initialized to empty string*/
	}
	
}

public class BrokerLookupServer {
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = null;
        boolean listening = true;
        LookupTable[] BrokerLookupTable=new LookupTable[2]; /*Created a lookup table here*/
        
        try {
        	if(args.length == 1) {
        		serverSocket = new ServerSocket(Integer.parseInt(args[0]));
        	} else {
        		System.err.println("ERROR: Invalid arguments!");
        		System.exit(-1);
        	}
        } catch (IOException e) {
            System.err.println("ERROR: Could not listen on port!");
            System.exit(-1);
        }

        while (listening) {
        	new BrokerLookupServerHandlerThread(serverSocket.accept(), BrokerLookupTable).start();
        }

        serverSocket.close();
    }
}
