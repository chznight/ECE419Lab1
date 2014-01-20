import java.io.*;
import java.net.*;

public class BrokerExchange {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {

		Socket NamingSocket = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;

		try {
			/* variables for hostname/port */
			String hostname = "localhost";
			int port = 4444;
			String brokername=new String();
			
			if(args.length == 3 ) {
				hostname = args[0];
				port = Integer.parseInt(args[1]);
				brokername=args[2];
			} else {
				System.err.println("ERROR: Invalid arguments!");
				System.exit(-1);
			}
			NamingSocket = new Socket(hostname, port);

			out = new ObjectOutputStream(NamingSocket.getOutputStream());
			in = new ObjectInputStream(NamingSocket.getInputStream());

		/*send a packet to naming server to know the hostname and port of broker*/
		BrokerPacket packetToServer = new BrokerPacket();
		packetToServer.type=BrokerPacket.LOOKUP_REQUEST;
		packetToServer.symbol=brokername;
		out.writeObject(packetToServer);

		/*process reply from naming server*/
		Socket BrokerSocket=null;
		boolean errorOccurred=false;

		BrokerPacket packetFromServer;
		packetFromServer = (BrokerPacket) in.readObject();
		if (packetFromServer.type == BrokerPacket.LOOKUP_REPLY) {
			System.out.println ("GOT REPLY");
			hostname=packetFromServer.locations[0].broker_host;
			port=packetFromServer.locations[0].broker_port;
		}else{
			System.out.println ("ERROR PACKET NOT RECOGNIZED");
			errorOccurred=true;
		}
		
		/*tear down naming server*/
		packetToServer=new BrokerPacket();
		packetToServer.type = BrokerPacket.BROKER_BYE;
		out.writeObject(packetToServer);

		in.close();
		out.close();
		NamingSocket.close();

		if(errorOccurred){
			System.exit(-1);
		}
		
		/*set up new socket and io stream*/
		BrokerSocket=new Socket(hostname, port);
		out = new ObjectOutputStream(BrokerSocket.getOutputStream());
		in = new ObjectInputStream(BrokerSocket.getInputStream());

		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String userInput;
		String[] tokens;
		System.out.println("Enter queries or x for exit: ");
		System.out.print(">");
		while ((userInput = stdIn.readLine()) != null
				&& userInput.toLowerCase().indexOf("x") == -1) {

			//BrokerPacket packetToServer = new BrokerPacket();
			tokens = userInput.split(" ");

			if (tokens[0].toLowerCase().equals ("add")) {
				if (tokens.length != 2) {
					System.out.println ("Invalid arguments, re-try");
					System.out.print(">");
					continue;
				}
				packetToServer.type = BrokerPacket.EXCHANGE_ADD;
				packetToServer.symbol = tokens[1];

			} else if (tokens[0].toLowerCase().equals ("remove")) {
				if (tokens.length != 2) {
					System.out.println ("Invalid arguments, re-try");
					System.out.print(">");
					continue;
				}
				packetToServer.type = BrokerPacket.EXCHANGE_REMOVE;
				packetToServer.symbol = tokens[1];

			} else if (tokens[0].toLowerCase().equals ("update")) {
				if (tokens.length != 3) {
					System.out.println ("Invalid arguments, re-try");
					System.out.print(">");
					continue;
				}
				packetToServer.type = BrokerPacket.EXCHANGE_UPDATE;
				packetToServer.symbol = tokens[1];
				packetToServer.quote = Long.parseLong(tokens[2]);

			} else {
				System.out.println ("ERROR: Command not recognized, retry.");
				System.out.print(">");
				continue;
			}
			/* make a new request packet */

			out.writeObject(packetToServer);

			/* print server reply */
			//BrokerPacket packetFromServer;
			packetFromServer = (BrokerPacket) in.readObject();

			if (packetFromServer.type == BrokerPacket.EXCHANGE_ADD) {
				System.out.println (packetFromServer.symbol + " " + "added");
			} else if (packetFromServer.type == BrokerPacket.EXCHANGE_REMOVE) {
				System.out.println (packetFromServer.symbol + " " + "removed");
			} else if (packetFromServer.type == BrokerPacket.EXCHANGE_UPDATE) {
				System.out.println (packetFromServer.symbol + " updated to " + packetFromServer.quote);
			} else if (packetFromServer.type == BrokerPacket.BROKER_ERROR) {
				if (packetFromServer.error_code == BrokerPacket.ERROR_INVALID_SYMBOL) {
					System.out.println (packetFromServer.symbol + " invalid");
				} else if (packetFromServer.error_code == BrokerPacket.ERROR_OUT_OF_RANGE) {
					System.out.println (packetFromServer.symbol + " out of range");
				} else if (packetFromServer.error_code == BrokerPacket.ERROR_SYMBOL_EXISTS) {
					System.out.println (packetFromServer.symbol + " exists");
				} else if (packetFromServer.error_code == BrokerPacket.ERROR_INVALID_EXCHANGE) {
					System.out.println ("Invalid exchange");
				} else {
					System.out.println ("ERROR CODE NOT RECOGNIZED");
				}
			} else {
				System.out.println ("ERROR PACKET NOT RECOGNIZED");
			}

			/* re-print console prompt */
			System.out.print(">");
		}

		/* tell server that i'm quitting */
		//BrokerPacket packetToServer = new BrokerPacket();
		packetToServer.type = BrokerPacket.BROKER_BYE;
		out.writeObject(packetToServer);

		out.close();
		in.close();
		stdIn.close();
		BrokerSocket.close();

		} catch (UnknownHostException e) {
			System.err.println("ERROR: Don't know where to connect!!");
			System.exit(1);
		} catch (IOException e) {
			System.err.println("ERROR: Couldn't get I/O for the connection.");
			System.exit(1);
		}

	}
}
