import java.net.*;
import java.io.*;
import java.util.HashMap;

public class BrokerLookupServerHandlerThread extends Thread {
	private Socket socket = null;

	private LookupTable[] BrokerLookupTable;
	public BrokerLookupServerHandlerThread(Socket socket, LookupTable[] BrokerLookupTable_) {
		super("BrokerLookupServerHandlerThread");
		this.socket = socket;
		BrokerLookupTable = BrokerLookupTable_;
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
				boolean flag=false;
				/* If you want to register */
				if(packetFromClient.type == BrokerPacket.LOOKUP_REGISTER) { 
					for(int i=0;i<2;i++){
						if(BrokerLookupTable[i].broker_name==""){ /*found empty slot*/
							BrokerLookupTable[i].broker_name=packetFromClient.symbol; /*store name into table*/
							BrokerLookupTable[i].broker_location=packetFromClient.locations[0]; /*store location into table*/
							packetToClient.type = BrokerPacket.LOOKUP_REPLY;
							toClient.writeObject(packetToClient);
							flag=true;
							break;
						}
						if(BrokerLookupTable[i].broker_name==packetFromClient.symbol){/*name already exists*/
							packetToClient.type = BrokerPacket.LOOKUP_REPLY;
							toClient.writeObject(packetToClient);
							flag=true;
							break;											
						}
					}
					/*DANGER: assuming only 2 brokers, if exceed 2, name will not get registered*/
					if(flag){
						continue;
					}else{
						/*idk what i should do here yet (This is when # of brokers > 2)*/
						continue;												
					}
				}
				
				/* If you want to request lookup */
				if(packetFromClient.type == BrokerPacket.LOOKUP_REQUEST) {
					for(int i=0;i<2;i++){
						if(BrokerLookupTable[i].broker_name==packetFromClient.symbol){ /*if there is a match*/
							packetToClient.locations[0]=BrokerLookupTable[i].broker_location; /*tell client the location*/
							packetToClient.type = BrokerPacket.LOOKUP_REPLY;
							toClient.writeObject(packetToClient);
							flag=true;
							break;
						}
					}
					if(flag){
						continue;
					}else{
						packetToClient.error_code=BrokerPacket.ERROR_INVALID_SYMBOL;
						packetToClient.type = BrokerPacket.BROKER_ERROR;					
						toClient.writeObject(packetToClient);
						continue;
					}
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
