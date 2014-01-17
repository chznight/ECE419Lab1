import java.net.*;
import java.io.*;
import java.util.HashMap;

//Things to work on:
//error code use flexibility
//file updates
//concurrency

public class OnlineBrokerHandlerThread extends Thread {
	private Socket socket = null;

	private HashMap<String, Long> BrokerTable;
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
					Long quote = BrokerTable.get(packetFromClient.symbol.toLowerCase());
					packetToClient.symbol = packetFromClient.symbol;
					if (quote == null) {
						packetToClient.type = BrokerPacket.BROKER_ERROR;
						packetToClient.error_code = BrokerPacket.ERROR_INVALID_SYMBOL;
					} else {
						packetToClient.type = BrokerPacket.BROKER_QUOTE;
						packetToClient.quote = quote;
					}
					/* send reply back to client */
					toClient.writeObject(packetToClient);
					/* wait for next packet */
					continue;
				} else if (packetFromClient.type == BrokerPacket.EXCHANGE_ADD) {
					BrokerTable.put (packetFromClient.symbol.toLowerCase(), Long.valueOf(0));
				} else if (packetFromClient.type == BrokerPacket.EXCHANGE_REMOVE) {
					packetToClient.type = BrokerPacket.EXCHANGE_REPLY;
					if (BrokerTable.remove (packetFromClient.symbol.toLowerCase()) == null) {
						packetToClient.type = BrokerPacket.BROKER_ERROR;
						packetToClient.error_code = BrokerPacket.ERROR_INVALID_SYMBOL;
					}
				} else if (packetFromClient.type == BrokerPacket.EXCHANGE_UPDATE) {

				} else {

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
