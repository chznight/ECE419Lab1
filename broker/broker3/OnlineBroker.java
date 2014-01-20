import java.net.*;
import java.io.*;
import java.util.HashMap;


public class OnlineBroker {
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        ServerSocket serverSocket = null;
        Socket lookupSocket = null;
        ObjectOutputStream lookup_out = null;
        ObjectInputStream lookup_in = null;
        boolean listening = true;
        HashMap<String, Long> brokerTable = new HashMap<String, Long>();
        String broker_name = "nasdaq";
        String lookup_name = "localhost";
        int lookup_port = 1111;
        if(args.length == 4) {
            lookup_name = args[0];
            lookup_port = Integer.parseInt(args[1]);
            broker_name = args[3];
        } else {
            System.err.println("ERROR: Invalid arguments!");
            System.exit(-1);
        }

        FileReader broker_table_file = new FileReader(broker_name);
        BufferedReader reader = new BufferedReader(broker_table_file);
        String line;
        String[] tokens;
        line = reader.readLine();
        while (line != null) {
            tokens = line.split(" ");
            brokerTable.put (tokens[0].toLowerCase(), Long.parseLong(tokens[1]));
            line = reader.readLine();
        }
        reader.close();

        /* Attempt to register*/
        BrokerPacket packetToServer = new BrokerPacket();
        BrokerPacket packetFromServer = null;
        
        try {
            lookupSocket = new Socket(lookup_name, lookup_port);
            lookup_out = new ObjectOutputStream(lookupSocket.getOutputStream());
            lookup_in = new ObjectInputStream(lookupSocket.getInputStream());
        } catch (UnknownHostException e) {
            System.err.println("ERROR: Don't know where to connect!!");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("ERROR: Couldn't get I/O for the connection.");
            System.exit(1);
        }
            BrokerLocation brokerLocation = new BrokerLocation(InetAddress.getLocalHost().getHostAddress(), Integer.parseInt(args[2]));
            packetToServer.type = BrokerPacket.LOOKUP_REGISTER;
            packetToServer.symbol = broker_name;
            packetToServer.locations = new BrokerLocation[1];
            packetToServer.locations[0] = brokerLocation;
            lookup_out.writeObject(packetToServer);
            packetFromServer = (BrokerPacket) lookup_in.readObject();
            if (packetFromServer.type != BrokerPacket.LOOKUP_REPLY) {
                System.out.println ("Register with naming service failed");
            } else {
                System.out.println ("Register successful");
            }

        try {
            serverSocket = new ServerSocket(Integer.parseInt(args[2]));
        } catch (IOException e) {
            System.err.println("ERROR: Could not listen on port!");
            System.exit(-1);
        }

        while (listening) {
        	new OnlineBrokerHandlerThread(serverSocket.accept(), brokerTable, broker_name,lookup_name,lookup_port).start();
        }

        serverSocket.close();
    }
}
