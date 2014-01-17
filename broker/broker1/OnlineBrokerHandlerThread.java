import java.net.*;
import java.io.*;
import java.util.HashMap;

public class OnlineBrokerHandlerThread extends Thread {
	private Socket socket = null;

	HashMap<String, Long> BrokerTable;
	public OnlineBrokerHandlerThread(Socket socket, HashMap<String, Long> BrokerTable_) {
		super("EchoServerHandlerThread");
		this.socket = socket;
		BrokerTable = BrokerTable_;
		System.out.println("Created new Thread to handle client");
	}

	public void run() {

		boolean gotByePacket = false;
		
		try {
			/* stream to read from client */
			ObjectInputStream fromClient = new ObjectInputStream(socket.getInputStream());
			BrokerPacket packetFromClient;
			
			/* stream to write back to client */
			ObjectOutputStream toClient = new ObjectOutputStream(socket.getOutputStream());
			

			while (( packetFromClient = (BrokerPacket) fromClient.readObject()) != null) {
				/* create a packet to send reply back to client */
				BrokerPacket packetToClient = new BrokerPacket();
				
				/* process request */
				
				if(packetFromClient.type == BrokerPacket.BROKER_REQUEST) {
					packetToClient.type = BrokerPacket.BROKER_QUOTE;
					Long quote = BrokerTable.get(packetFromClient.symbol.toLowerCase());
					if (quote == null) {
						packetToClient.quote = Long.valueOf(0);
					} else {
						packetToClient.quote = quote;
					}
					
					/* send reply back to client */
					toClient.writeObject(packetToClient);
					
					/* wait for next packet */
					continue;
				}
				
				/* Sending an ECHO_NULL || ECHO_BYE means quit */
				if (packetFromClient.type == BrokerPacket.BROKER_BYE || packetFromClient.type == BrokerPacket.BROKER_NULL) {
					gotByePacket = true;
					break;
				}
				
				/* if code comes here, there is an error in the packet */
				System.err.println("ERROR: Unknown ECHO_* packet!!");
				System.exit(-1);
			}
			
			/* cleanup when client exits */
			fromClient.close();
			toClient.close();
			socket.close();

		} catch (IOException e) {
			if(!gotByePacket)
				e.printStackTrace();
		} catch (ClassNotFoundException e) {
			if(!gotByePacket)
				e.printStackTrace();
		}
	}

}
