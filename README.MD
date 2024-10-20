notes:

dockerfiles:
- defines how to build a single docker image
- specifies the environment, dependencies and commands needed to create an image for a single service/application

docker-compose.yml:
- defines and manages multi-container docker applications
- configures how multiple services interact with each other
- defines services, networks, and volumes
- configures environment variables, port mappings, dependencies between services

system architecture:
- client interacts with system for file operations
- coordinator orchestrates operations
- metadata service manages file metadata
- storage nodes store the file chunks

gRPC:
- framework for remote procedure calls
- uses protocol buffers as it's interface definition language
- we define a service and message types in a .proto file
- the protoc compiler generates client and server code

e.g. upload function:
- client calls `client.UploadFile` to start the streaming RPC
- server `UploadFile` is called
- client reads the file in chunks and sends each chunk using `stream.Send`
- server receives chunks using `stream.Recv` in a loop
- client continues to send chunks until the whole file is sent
- server generates a unique chunkId for each chunk and distributes it to a storage node
- client calls `stream.CloseAndRecv` to finish sending and wait for the response
- when `stream.Recv` returns `io.EOF`, it saves the metadata and calls `stream.SendAndClose` with the reponse to the client
