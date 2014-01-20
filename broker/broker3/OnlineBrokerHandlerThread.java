import java.net.*;
import java.io.*;
import java.util.*;

//Things to work on:
//error code use flexibility
//file updates
//concurrency

public class OnlineBrokerHandlerThread extends Thread {
	private Socket socket = null;
	private String broker_name; //The name of this broker: nasdaq or tse
    private String lookup_name; //Hostname of the naming service
    private int lookup_port;	//Port of the naming service
	private HashMap<String, Long> brokerTable;
	public OnlineBrokerHandlerThread(Socket socket, HashMap<String, Long> brokerTable_, String broker_name_, String lookup_name_, int lookup_port_) {
		super("EchoServerHandlerThread");
		this.socket = socket;
		brokerTable = brokerTable_;
		broker_name = broker_name_;
		lookup_name = lookup_name_;
		lookup_port = lookup_port_;
		System.out.println("Created new Thread to handle client");
	}

	public void run() {

		boolean gotByePacket = false;
		int num_of_edits = 0;
		
		try {
			BrokerPacket packetFromLookup=null;
			BrokerPacket packetToLookup=null;
			BrokerPacket packetFromBroker=null;
			BrokerPacket packetToBroker=null;
			
			ObjectInputStream fromLookup=null;
			ObjectOutputStream toLookup=null;
			ObjectInputStream fromBroker=null;
			ObjectOutputStream toBroker=null;

			Socket lookupSocket=null;
			Socket brokerSocket=null;

			/* stream to write back to client */
			ObjectOutputStream toClient = new ObjectOutputStream(socket.getOutputStream());
			toClient.flush();
			/* stream to read from client */
			ObjectInputStream fromClient = new ObjectInputStream(socket.getInputStream());
			BrokerPacket packetFromClient;

			while (( packetFromClient = (BrokerPacket) fromClient.readObject()) != null) {

				if (num_of_edits >= 3) {
				//If the hashmap has been modified more than three times, then we update the file in hard disk
					synchronized (brokerTable) {
						File newFile = new File (broker_name);
				        FileWriter fileW = new FileWriter(newFile);
				        BufferedWriter buffW = new BufferedWriter (fileW);
				        for (Map.Entry<String, Long> entry : brokerTable.entrySet()) {
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
					System.out.println("Request");
					Long quote;
					synchronized (brokerTable) {
						quote = brokerTable.get(packetFromClient.symbol.toLowerCase());
					}
					packetToClient.symbol = packetFromClient.symbol;
					if (quote == null) { /*cannot quote here*/
						System.out.println("Cannot find");
						packetToLookup=new BrokerPacket();
						packetToLookup.locations=new BrokerLocation[1];
						packetToLookup.type=BrokerPacket.LOOKUP_REQUEST; 
						if(broker_name.equals("nasdaq")){
							System.out.println("finding tse");
							packetToLookup.symbol="tse";
						}else if(broker_name.equals("tse")){
							System.out.println("finding nasdaq");
							packetToLookup.symbol="nasdaq";
						}

						lookupSocket=new Socket(lookup_name, lookup_port);
						toLookup = new ObjectOutputStream(lookupSocket.getOutputStream());
						toLookup.flush();
						fromLookup = new ObjectInputStream(lookupSocket.getInputStream());
						
						System.out.println("To");
						toLookup.writeObject(packetToLookup);
						System.out.println("From");
						packetFromLookup = (BrokerPacket) fromLookup.readObject();
			
						String a=packetFromLookup.locations[0].broker_host;
						int b=packetFromLookup.locations[0].broker_port;
			
						packetToLookup=new BrokerPacket();
						packetToLookup.type=BrokerPacket.BROKER_BYE;
						toLookup.writeObject(packetToLookup);
						fromLookup.close();
						toLookup.close();
						lookupSocket.close();

System.out.println("Lookup: "+packetFromLookup.type+" "+a+" "+b);
						packetToBroker=new BrokerPacket();
						packetToBroker.locations=new BrokerLocation[1];
						packetToBroker.type=BrokerPacket.BROKER_FORWARD; 
						packetToBroker.symbol=packetFromClient.symbol;

						brokerSocket=new Socket(packetFromLookup.locations[0].broker_host, packetFromLookup.locations[0].broker_port);
						toBroker = new ObjectOutputStream(brokerSocket.getOutputStream());
						toBroker.flush();						
						fromBroker = new ObjectInputStream(brokerSocket.getInputStream());
						toBroker.writeObject(packetToBroker);
						packetFromBroker = (BrokerPacket) fromBroker.readObject();						
System.out.println("Broker: "+packetFromBroker.type+" "+packetFromBroker.quote);
						if(packetFromBroker.type==BrokerPacket.BROKER_QUOTE){ /*other broker can quote*/
							packetToClient.type = BrokerPacket.BROKER_QUOTE;
							packetToClient.quote = packetFromBroker.quote;							
						}else{ /*other broker fails too*/
							packetToClient.type = BrokerPacket.BROKER_ERROR;
							packetToClient.error_code = BrokerPacket.ERROR_INVALID_SYMBOL;
						}
						
						packetToBroker=new BrokerPacket();
						packetToBroker.type=BrokerPacket.BROKER_BYE;
						toBroker.writeObject(packetToBroker);
						fromBroker.close();
						toBroker.close();
						brokerSocket.close();
					} else {
						packetToClient.type = BrokerPacket.BROKER_QUOTE;
						packetToClient.quote = quote;
					}
					/* send reply back to client */
					toClient.writeObject(packetToClient);
					/* wait for next packet */
					continue;
				} else if(packetFromClient.type == BrokerPacket.BROKER_FORWARD){ 
					Long quote;
					synchronized (brokerTable) {
						quote = brokerTable.get(packetFromClient.symbol.toLowerCase());
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
					synchronized (brokerTable){
						if (brokerTable.containsKey(packetFromClient.symbol.toLowerCase())) {
							packetToClient.type = BrokerPacket.BROKER_ERROR;
							packetToClient.error_code = BrokerPacket.ERROR_SYMBOL_EXISTS;

						} else {
							brokerTable.put (packetFromClient.symbol.toLowerCase(), Long.valueOf(0));
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
					synchronized (brokerTable) {
						if (brokerTable.remove (packetFromClient.symbol.toLowerCase()) == null) {
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
					synchronized (brokerTable) {
						if ((brokerTable.containsKey(packetFromClient.symbol.toLowerCase())) == false) {
							packetToClient.type = BrokerPacket.BROKER_ERROR;
							packetToClient.error_code = BrokerPacket.ERROR_INVALID_SYMBOL;
						} else if (packetFromClient.quote.intValue() < 1 || packetFromClient.quote.intValue() > 300){
							packetToClient.type = BrokerPacket.BROKER_ERROR;
							packetToClient.error_code = BrokerPacket.ERROR_OUT_OF_RANGE;
						} else {
							packetToClient.type = BrokerPacket.EXCHANGE_UPDATE;
							brokerTable.put (packetFromClient.symbol.toLowerCase(), packetFromClient.quote);
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
			System.out.println ("Broker thread exiting");
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
