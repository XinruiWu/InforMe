# How to compile and run in terminal:

    $ javac Project/MainProducer.java
    $ java Project/Mainproducer


# System Setup: (Compile all then run in sequence) :
`MainProducer.java`

**Port** and **Address** are set so you can just run it without type in, same below

`Broker.java`

Enter `Y`  for the first broker, `N` and `{Port}` for backup brokers

`ServerApp5000.java`

If need multiple servers, run `ServerApp5001.java` etc.

`ClientApp6000.java`

If need multiple client, run `ClientApp6001.java` etc.

# To try on some feathers:
`ClientMultipleRegister.java` to register with **one certain** topic

`DeleteClient.java` to unregister with **one certain** topic

`DeleteServer.java` to delete **one certain** server
