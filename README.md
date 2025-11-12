# Distributed Systems, BSc (Autumn 2025) - Assignment 4
## by freju@itu.dk, frvs@itu.dk and alyp@itu.dk


### Prerequisites:
- Golang (1.25+)
- Basic knowledge about running go programs.
- google.golang.org/grpc v1.76.0 compatible installation
- google.golang.org/protobuf v1.36.10 compatible installation

## How to run:

1. Navigate to the directory where you've downloaded the github repository\
   ```cd <directoryOfRepository>/server```
2. Run the server specifying the port it will listen on: \
   ```go run node.go --port <port>```
   e.x: ```go run node.go --port 8080```
3. Repeat step 1 and 2 in a new terminal as many times as you want nodes, each with a different port.
4. Connect to other nodes' servers by writing in the command line. **Note** only a 4 digit port is accepted:\
``` --connect <port>``` or ```-c <port>```
5. Repeat step 4 for each port to connect to for each node terminal opened.
6. To start the Ricart-Agrawala algorithm, write for each node terminal open:
```--start```

### Minimal example:
1. Terminal 1: ```go run node.go --port 8080```
2. Terminal 2: ```go run node.go --port 8081```
3. Terminal 3: ```go run node.go --port 8082```
4. Terminal 1: ```-c 8081``` ```-c 8082```
5. Terminal 2: ```-c 8080``` ```-c 8082```
6. Terminal 3: ```-c 8080``` ```-c 8081```
7. Terminal 1: ```--start```
8. Terminal 2: ```--start```
9. Terminal 3: ```--start```

## Logs
Logs are generated in the folder NodeLogs, as json files.

## Note
Note that the terminals will start immediately attempting to enter the critical section
once ```--start```, has been called, meaning you will likely see a terminal printing that it 
entered the critical section, before you get to start the other terminals. This just means the other 
nodes are replying immediately because they are not themselves yet interested in acquiring the critical section.

Also note that the program currently does not handle a crashing or exiting processes as this is not a part of 
the requirements of the assignment, nor the Ricart-Agrawala algorithm as presented in the lectures.

