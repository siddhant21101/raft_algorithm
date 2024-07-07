import grpc
import uuid as ud
from concurrent import futures
import threading
import nodes_pb2_grpc
import nodes_pb2 
nodes=[]
currentLeader="localhost:1111"
if __name__ == '__main__':
    RandomIdx=2
    nodes = ["localhost:1111", "localhost:2222", "localhost:3333"]  
    # nodes = ["localhost:1111", "localhost:2222", "localhost:3333","localhost:4444","localhost:5555"]  
    while(True):
        query=input("enter query")
        while(True):
            try:
                print(f"sending request to {currentLeader}")
                channel = grpc.insecure_channel(currentLeader)
                stub=nodes_pb2_grpc.RaftManagerServicerStub(channel)
                RequestVoteDetails=nodes_pb2.ServeClientArgs (Request=query)
                print("sending request")
                response=stub.ServeClient(RequestVoteDetails)
                
                if response.LeaderID is not None :
                    currentLeader=response.LeaderID
                print(response)
                if response.Success:
                    break
            except:
                print("Failure in absence of leader in the system!")
                currentLeader=nodes[(RandomIdx%3)]
                RandomIdx+=1
                break
        

