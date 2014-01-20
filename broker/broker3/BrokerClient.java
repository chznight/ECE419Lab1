import java.io.*;
import java.net.*;

public class BrokerClient {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {

		Socket LookupSocket = null;
		ObjectOutputStream lookup_out = null;
		ObjectInputStream lookup_in = null;

		Socket BrokerSocket = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;

		boolean connected_to_broker = false;

			/* variables for hostname/port */
		String hostname = "localhost";
		int port = 4444;
			
		if(args.length == 2 ) {
			hostname = args[0];
			port = Integer.parseInt(args[1]);
		} else {
			System.err.println("ERROR: Invalid arguments!");
			System.exit(-1);
		}

		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String userInput;
		String[] tokens;
		System.out.println("Enter queries or x for exit: ");
		System.out.print(">");
		while ((userInput = stdIn.readLine()) != null
				&& userInput.toLowerCase().indexOf("x") == -1) {
			/* make a new request packet */
			BrokerPacket packetToServer = new BrokerPacket();
			BrokerPacket packetFromServer;
			tokens = userInput.split(" ");
			if (tokens[0].toLowerCase().equals ("local")) {
				try {
					LookupSocket = new Socket(hostname, port);
					lookup_out = new ObjectOutputStream(LookupSocket.getOutputStream());
					lookup_in = new ObjectInputStream(LookupSocket.getInputStream());
				} catch (UnknownHostException e) {
					System.err.println("ERROR: Don't know where to connect!!");
					System.exit(1);
				} catch (IOException e) {
					System.err.println("ERROR: Couldn't get I/O for the connection.");
					System.exit(1);
				}
				packetToServer.type = BrokerPacket.LOOKUP_REQUEST;
				packetToServer.symbol = args[1];
				lookup_out.writeObject(packetToServer);
				packetFromServer = (BrokerPacket) lookup_in.readObject();
			} else {
				packetToServer.type = BrokerPacket.BROKER_REQUEST;
				packetToServer.symbol = userInput;
				if (connected_to_broker == false) {
					System.out.println ("Not connected to broker");
					System.out.print(">");
					continue;
				}
				out.writeObject(packetToServer);
				packetFromServer = (BrokerPacket) in.readObject();
			}


			if (packetFromServer.type == BrokerPacket.BROKER_QUOTE) {
				System.out.println("Quote from broker: " + packetFromServer.quote);
			} else if (packetFromServer.type == BrokerPacket.LOOKUP_REPLY) {
				if (connected_to_broker == true) {
					out.close();
					in.close();
					BrokerSocket.close();
				}
				BrokerSocket = new Socket(packetFromServer.locations[0].broker_host, packetFromServer.locations[0].broker_port);
				out = new ObjectOutputStream(BrokerSocket.getOutputStream());
				in = new ObjectInputStream(BrokerSocket.getInputStream());
				connected_to_broker = true;
				lookup_out.close();
				lookup_in.close();
				LookupSocket.close();
				System.out.println (args[1] + " as local");
			} else if (packetFromServer.type == BrokerPacket.BROKER_ERROR) {
				if (packetFromServer.error_code == BrokerPacket.ERROR_INVALID_SYMBOL) {
					System.out.println (packetFromServer.symbol + " invalid");
				}
			} else {
				System.out.println ("ERROR: PACKET NOT RECOGNIZED");
			}

			/* re-print console prompt */
			System.out.print(">");
		}

		/* tell server that i'm quitting */
		BrokerPacket packetToServer = new BrokerPacket();
		packetToServer.type = BrokerPacket.BROKER_BYE;
		out.writeObject(packetToServer);

		out.close();
		in.close();
		stdIn.close();
		BrokerSocket.close();
	}
}
