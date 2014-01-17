import java.net.*;
import java.io.*;
import java.util.HashMap;


public class OnlineBroker {
    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = null;
        boolean listening = true;
        HashMap<String, Long> BrokerTable = new HashMap<String, Long>();

        FileReader nasdaq = new FileReader("nasdaq");
        BufferedReader reader = new BufferedReader(nasdaq);
        String line;
        String[] tokens;
        line = reader.readLine();
        while (line != null) {
            tokens = line.split(" ");
            BrokerTable.put (tokens[0].toLowerCase(), Long.parseLong(tokens[1]));
            line = reader.readLine();
        }
        reader.close();
        
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
        	new OnlineBrokerHandlerThread(serverSocket.accept(), BrokerTable).start();
        }

        serverSocket.close();
    }
}
