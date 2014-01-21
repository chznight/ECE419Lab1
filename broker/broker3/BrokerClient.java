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
		System.out.print("> ");
		while ((userInput = stdIn.readLine()) != null
				&& userInput.toLowerCase().indexOf("x") == -1) {
			/* make a new request packet */
			BrokerPacket packetToServer = new BrokerPacket();
			BrokerPacket packetFromServer;
			tokens = userInput.split(" ");
			if (tokens[0].toLowerCase().equals ("local")) {
				try {
					/*if (connected_to_broker == true) {
						packetToServer = new BrokerPacket();
						packetToServer.type = BrokerPacket.BROKER_BYE;
						out.writeObject(packetToServer);
						out.close();
						in.close();
						BrokerSocket.close();
						connected_to_broker = false;
					}*/
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
				packetToServer.symbol = tokens[1];
				lookup_out.writeObject(packetToServer);
				packetFromServer = (BrokerPacket) lookup_in.readObject();
			} else {
				if (connected_to_broker == false) {
					System.out.println ("Not connected to broker");
					System.out.print(">");
					continue;
				}
				packetToServer.type = BrokerPacket.BROKER_REQUEST;
				packetToServer.symbol = userInput;
				out.writeObject(packetToServer);
				packetFromServer = (BrokerPacket) in.readObject();
			}


			if (packetFromServer.type == BrokerPacket.BROKER_QUOTE) {
				System.out.println("Quote from broker: " + packetFromServer.quote);
			} else if (packetFromServer.type == BrokerPacket.LOOKUP_REPLY) {
				packetToServer = new BrokerPacket();
				packetToServer.type = BrokerPacket.BROKER_BYE;
				lookup_out.writeObject(packetToServer);
				//lookup_in.readObject();
				lookup_out.close();
				lookup_in.close();
				LookupSocket.close();
				LookupSocket=null;
				if (connected_to_broker == true) {
					packetToServer = new BrokerPacket();
					packetToServer.type = BrokerPacket.BROKER_BYE;
					out.writeObject(packetToServer);
					out.close();
					in.close();
					BrokerSocket.close();
					connected_to_broker = false;
				}
				BrokerSocket = new Socket(packetFromServer.locations[0].broker_host, packetFromServer.locations[0].broker_port);
				out = new ObjectOutputStream(BrokerSocket.getOutputStream());
				in = new ObjectInputStream(BrokerSocket.getInputStream());
				connected_to_broker = true;
				System.out.println (packetFromServer.symbol + " as local");
			} else if (packetFromServer.type == BrokerPacket.BROKER_ERROR) {
				if (packetFromServer.error_code == BrokerPacket.ERROR_INVALID_SYMBOL) {
					System.out.println (packetFromServer.symbol + " invalid");
				}
				if (packetFromServer.error_code == BrokerPacket.ERROR_INVALID_EXCHANGE) {
					System.out.println (packetFromServer.symbol + " invalid, can not find broker");
					if (LookupSocket != null) {
						packetToServer = new BrokerPacket();
						packetToServer.type = BrokerPacket.BROKER_BYE;
						lookup_out.writeObject(packetToServer);
						
						lookup_out.close();
						lookup_in.close();
						LookupSocket.close();
						LookupSocket = null;
					}
				}
			} else {
				System.out.println ("ERROR: PACKET NOT RECOGNIZED");
			}

			/* re-print console prompt */
			System.out.print("> ");
		}

		/* tell server that i'm quitting */
		
		if (connected_to_broker == true) {
			BrokerPacket packetToServer = new BrokerPacket();
			packetToServer.type = BrokerPacket.BROKER_BYE;
			out.writeObject(packetToServer);

			out.close();
			in.close();
			stdIn.close();
			BrokerSocket.close();	
		}

	}
}
