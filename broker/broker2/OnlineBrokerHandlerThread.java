import java.net.*;
import java.io.*;
import java.util.*;

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
		int num_of_edits = 0;
		
		try {
			/* stream to read from client */
			ObjectInputStream fromClient = new ObjectInputStream(socket.getInputStream());
			BrokerPacket packetFromClient;
			/* stream to write back to client */
			ObjectOutputStream toClient = new ObjectOutputStream(socket.getOutputStream());

			while (( packetFromClient = (BrokerPacket) fromClient.readObject()) != null) {

				if (num_of_edits >= 3) {
				//If the hashmap has been modified more than three times, then we update the file in hard disk
					synchronized (BrokerTable) {
						File newFile = new File ("nasdaq");
				        FileWriter fileW = new FileWriter(newFile);
				        BufferedWriter buffW = new BufferedWriter (fileW);
				        for (Map.Entry<String, Long> entry : BrokerTable.entrySet()) {
							buffW.write(entry.getKey() + " " + entry.getValue() + "\n");
						}
				        buffW.close();
				        num_of_edits = 0;
					}
				}

				/* create a packet to send reply back to client */
				BrokerPacket packetToClient = new BrokerPacket();
				/* process request */
				
				if(packetFromClient.type == BrokerPacket.BROKER_REQUEST) {
					Long quote;
					synchronized (BrokerTable) {
						quote = BrokerTable.get(packetFromClient.symbol.toLowerCase());
					}
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
					packetToClient.symbol = packetFromClient.symbol;
					synchronized (BrokerTable){
						if (BrokerTable.containsKey(packetFromClient.symbol.toLowerCase())) {
							packetToClient.type = BrokerPacket.BROKER_ERROR;
							packetToClient.error_code = BrokerPacket.ERROR_SYMBOL_EXISTS;

						} else {
							BrokerTable.put (packetFromClient.symbol.toLowerCase(), Long.valueOf(0));
							packetToClient.type = BrokerPacket.EXCHANGE_ADD;
							num_of_edits++;
						}
					}
					/* send reply back to client */
					toClient.writeObject(packetToClient);
					/* wait for next packet */
					continue;
				} else if (packetFromClient.type == BrokerPacket.EXCHANGE_REMOVE) {
					packetToClient.symbol = packetFromClient.symbol;
					synchronized (BrokerTable) {
						if (BrokerTable.remove (packetFromClient.symbol.toLowerCase()) == null) {
							packetToClient.type = BrokerPacket.BROKER_ERROR;
							packetToClient.error_code = BrokerPacket.ERROR_INVALID_SYMBOL;
						} else {
							packetToClient.type = BrokerPacket.EXCHANGE_REMOVE;
							num_of_edits++;
						}
					}
					/* send reply back to client */
					toClient.writeObject(packetToClient);
					/* wait for next packet */
					continue;
				} else if (packetFromClient.type == BrokerPacket.EXCHANGE_UPDATE) {
					packetToClient.symbol = packetFromClient.symbol;
					synchronized (BrokerTable) {
						if ((BrokerTable.containsKey(packetFromClient.symbol.toLowerCase())) == false) {
							packetToClient.type = BrokerPacket.BROKER_ERROR;
							packetToClient.error_code = BrokerPacket.ERROR_INVALID_SYMBOL;
						} else if (packetFromClient.quote.intValue() < 1 || packetFromClient.quote.intValue() > 300){
							packetToClient.type = BrokerPacket.BROKER_ERROR;
							packetToClient.error_code = BrokerPacket.ERROR_OUT_OF_RANGE;
						} else {
							packetToClient.type = BrokerPacket.EXCHANGE_UPDATE;
							BrokerTable.put (packetFromClient.symbol.toLowerCase(), packetFromClient.quote);
							packetToClient.quote = packetFromClient.quote;
							num_of_edits++;
						}
					}
					/* send reply back to client */
					toClient.writeObject(packetToClient);
					/* wait for next packet */
					continue;
				} else if (packetFromClient.type == BrokerPacket.BROKER_BYE || packetFromClient.type == BrokerPacket.BROKER_NULL) {
					/* Sending an ECHO_NULL || ECHO_BYE means quit */
					gotByePacket = true;
					break;
				} else {
					/* if code comes here, there is an error in the packet */
					System.err.println("ERROR: Unknown packet!!");
					System.exit(-1);
				}
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
