import paho.mqtt.client as mqtt
clients=[]
nclients=20
mqtt.Client.connected_flag=False
#create clients
for i  in range(nclients):
   cname="Client"+str(i)
   client= mqtt.Client(cname)
   clients.append(client)
for client in clients:
  client.connect(broker)
  client.loop_start()



  #db_conn = user_data['db_conn']
    

    #db_conn = user_data['db_conn']
    