## pard-main
### Server
The server provides main entry of pard system.\
When starting up, the server needs act as following:
+ Read configuration and validate it.
+ Start the RPC server and clients.
+ Start the data exchange server and clients.
+ Start the catalog.
+ Start the executor.
+ Start the node keeper.
+ Start the socket server and session manager.
+ Start the web server for UI.
+ Ready for client connection.

As shutting down, the server needs act reversely as above.

### Planner

### Scheduler