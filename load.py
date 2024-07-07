import grpc
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor
import nodes_pb2
import nodes_pb2_grpc
import os
# Constant
HEARTBEAT_INTERVAL = 1  # seconds
ELECTION_TIMEOUT_RANGE = (5, 15)  # seconds
LEADER_LEASE_TIMEOUT = 5  # seconds

class Node(nodes_pb2_grpc.RaftManagerServicer):
    def __init__(self, node, nodes,timeout):
        self.curNode = node
        self.node_ip = node.split(":")[0]
        self.node_port = node.split(":")[1]
        self.nodes = nodes
        self.term = 0
        self.electionTime=timeout
        self.voted_for = None
        self.votingTerm=0
        self.log = []
        self.prevLogIndex = -1
        self.prevLogTerm = 0
        self.commit_index = -1
        self.last_applied = 0 
        self.state = "FOLLOWER"
        self.leader_id =""
        self.leader_lease_timeout = None
        self.votes_received_granted = 0
        self.votes_received_denied = 0
        self.election_timer = None
        self.leader_lease_timer = None
        self.heartbeatThread=None

    def ServeClient(self, request, context):
        
        if self.state!="LEADER":
            return nodes_pb2.ServeClientReply(Success=False,LeaderID=self.leader_id,Data="failed requuest because it is not the leader")
        if self.leader_id is None :
            return nodes_pb2.ServeClientReply(Success=False,LeaderID=None,Data="failed requuest because the leader does no exist")
        print("sending ack to all nodes")
        ack=1
        self.log.append(request.Request)
        for node in self.nodes:
                self.restart_election_timer()
                myport=node.split(":")[1]
                if node != f"{self.node_ip}:{self.node_port}":  # Skip sending heartbeat to self
                    try:
                        channel = grpc.insecure_channel(node)
                        stub = nodes_pb2_grpc.RaftManagerServicerStub(channel)  
                        request = nodes_pb2.EntriesRequest(
                            term=self.term,
                            leader_id=f"{self.curNode}",
                            prev_log_index=self.prevLogIndex,
                            prev_log_term=self.prevLogTerm,
                            leader_commit=self.commit_index,
                            leader_lease_timeout=int(self.leader_lease_timeout),
                            entries=self.log, 
                        )
                        reply = stub.AppendEntries(request)  # Call AppendEntries RPC
                        if reply.success:
                            ack+=1
                            print(f"ack from {node}")
                            # Update leader's lease timeout
                            self.leader_lease_timeout = time.time() + LEADER_LEASE_TIMEOUT
                        else:
                            print(f"Error:no ack appendentry to {node} failed.")
                    except:
                        print(f"unabel to connect to for sending appendentry {myport}")
        if ack>len(self.nodes)/2:
            print("received majority ack from nodes commiting")
            unCommitedLogs=self.log[self.commit_index+1:]
            for logs in unCommitedLogs:
                self.append_log(logs)
            self.commit_index=len(self.log)-1
        return nodes_pb2.ServeClientReply(Success=True,LeaderID=self.leader_id,Data="Success")
    def load_logs(self):
        lines = []
        log_dir = f"nodes_logs_{self.node_port}"
        file_path=f"{log_dir}/logs.txt"
        with open(file_path, 'r') as file:
            for line in file:
                lines.append(line.strip())
        self.logs=lines
    def append_log(self, entry):
        log_dir = f"nodes_logs_{self.node_port}"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        with open(f"{log_dir}/logs.txt", "a") as file:
            file.write(" ".join(str(e) for e in entry) + "\n")
    def load_metadata(self):
        metadata = {}
        log_dir = f"nodes_logs_{self.node_port}"
        file_path = f"{log_dir}/metadata.txt"
        with open(file_path, 'r') as file:
            for line in file:
                key, value = line.strip().split()
                metadata[key] = value  # Store value as string

        self.commit_index = int(metadata.get('commitLength', '0'))  
        self.term = int(metadata.get('Term', '0'))  

        self.voted_for = metadata.get('NodeID')
        if self.voted_for == "None": 
            self.voted_for = None  

        print(f"loaded metadata commitIdx {self.commit_index} term {self.term} voted for {self.voted_for}")

        
    def update_metadata(self,new_commit_length, new_term, new_node_id):
        log_dir = f"nodes_logs_{self.node_port}"
        file_path=f"{log_dir}/metadata.txt"
        with open(file_path, 'r') as file:
            lines = file.readlines()
        for i in range(len(lines)):
            key, value = lines[i].strip().split()
            if key == 'commitLength':
                lines[i] = f'{key} {new_commit_length}\n'
            elif key == 'Term':
                lines[i] = f'{key} {new_term}\n'
            elif key == 'NodeID':
                lines[i] = f'{key} {new_node_id}\n'

        with open(file_path, 'w') as file:
            file.writelines(lines)
        print("updated metadata")
    
    def append_dump(self, entry):
        log_dir = f"nodes_logs_{self.node_port}"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        with open(f"{log_dir}/dumps.txt", "a") as file:
            file.write(" ".join(str(e) for e in entry) + "\n")
    

    def AppendEntries(self, request, context):
    # Check if the received term is less than the current term
        self.restart_election_timer()
        self.restart_leader_lease_timer()
        if request.term < self.term:
            print(f"request term < current term {request.term} {self.term}")
            return nodes_pb2.EntriesReply(term=self.term, success=False)
        # del self.log[request.prev_log_index + 1:]
        self.term = request.term
        self.leader_id = request.leader_id
        self.leader_lease_timeout = request.leader_lease_timeout
        print(f"restarting election timer for {self.node_port}")
        # If the node is not in the follower state, set it to follower
        if self.state != "FOLLOWER":
            self.state = "FOLLOWER"
        # print(request.entries)
        newLogs=(request.entries[self.prevLogIndex+1:])
        # print("newlogs-")
        # print(request)
        # # print(newLogs)
        # print(f"entriues {request.entries}missing logs{newLogs} previdx was{self.prevLogIndex}")
        for logs in newLogs:
            # print(logs)
            self.append_log(logs)
            self.log.append(logs)
        # self.log=request.entries
        # self.log = request.entries
        self.prevLogIndex=len(self.log)-1
        self.commit_index = min(request.leader_commit, len(self.log) - 1)
        text = f"Node {self.node_ip}:{self.node_port} accepted AppendEntries RPC from {request.leader_id}."
        self.append_dump(text)
        # print(f"Node {self.node_ip}:{self.node_port} log: {request.entries}.")

        print(f"Node {self.node_ip}:{self.node_port} received AppendEntries RPC from {request.leader_id}.")
        print(f"Node {self.node_ip}:{self.node_port} log: {self.log}")
        # Reply with success and the current term
        return nodes_pb2.EntriesReply(term=self.term, success=True)

    def Voting(self, request, context):
        # self.restart_leader_lease_timer()
        # self.restart_election_timer()
        if request.term>self.votingTerm:
            self.voted_for=None
        if request.term < self.term:
            print(f"request term < current term {request.term} {self.term}")
            return nodes_pb2.VoteReply(term=self.term, vote_granted=False)

        if (self.voted_for is None or self.voted_for == request.candidate_id):
            # print("Enter")
            self.term = request.term
            self.voted_for = request.candidate_id
            self.state = "FOLLOWER"
            return nodes_pb2.VoteReply(term=self.term, vote_granted=True , leader_lease_timeout=self.leader_lease_timeout)
        
        return nodes_pb2.VoteReply(term=self.term, vote_granted=False)
    
    def start_election(self):
        self.votes_received_granted = 1
        self.term += 1
        self.voted_for = self.curNode
        self.state = "CANDIDATE"
        text = f"Node {self.curNode} election timer timed out, Starting election."
        self.append_dump(text)
        print(f"Node {self.curNode} election timer timed out, Starting election.")
        print(f"Node {self.curNode} status = {self.state}.")
        print(f"Node {self.curNode} voted for itself in term {self.term}.")
        print(f"Node {self.curNode} requesting votes from other nodes..")
        # deadNode=0
        flag=False
        for node in self.nodes:
            myport=node.split(":")[1]
            if node.split(":")[1] != self.node_port:
                try:
                    channel = grpc.insecure_channel(f"localhost:{node.split(':')[1]}")
                    stub = nodes_pb2_grpc.RaftManagerServicerStub(channel)
                    request = nodes_pb2.VoteRequest(term=self.term, candidate_id=self.node_port)
                    reply = stub.Voting(request)
                    if reply.vote_granted:
                        self.votes_received_granted += 1
                        text = f"Vote granted to Node {self.node_ip}:{self.node_port} from {node} in term {self.term}."
                        self.append_dump(text)
                        print(f"Vote granted to Node {self.node_ip}:{self.node_port} from {node} in term {self.term}.")
                        halfVotes=((len(self.nodes)) // 2)
                        # print(f"{halfVotes}half votes and my votes {self.votes_received_granted}")
                       
                    else:
                        if reply.vote_granted == False:
                            self.votes_received_denied += 1
                            text = f"Vote denied to Node {self.node_ip}:{self.node_port} from {node} in term {self.term}."
                            self.append_dump(text)
                            print(f"Vote denied to Node {self.node_ip}:{self.node_port} from {node} in term {self.term}.")
                            if self.votes_received_denied > ((len(self.nodes)+1) // 2):
                                print(f"Node {self.node_ip}:{self.node_port} received majority denied votes of {self.votes_received_denied}. Converting back to FOLLOWER.")
                                self.state = "FOLLOWER"
                except:  
                    # deadNode+=1
                    print(f"unable to connect to {myport}")
        halfVotes=((len(self.nodes)) // 2)
        if (self.votes_received_granted > halfVotes):
            flag=True
            print(f"Node {self.node_ip}:{self.node_port} received majority votes of {self.votes_received_granted}. Becoming Leader..")
            self.become_leader(reply.leader_lease_timeout)
        if flag==False:
            self.restart_election_timer()
    def restart_leader_lease_timer(self):
        if self.leader_lease_timer:
            self.leader_lease_timer.cancel()  # Cancel the existing timer if it exists
        self.leader_lease_timer = threading.Timer(LEADER_LEASE_TIMEOUT, self.leader_lease_expired)
        self.leader_lease_timer.start()               
    def restart_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()  # Cancel the existing timer if it exists
        timeout =self.electionTime
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.start() 

    def become_leader(self, max_old_lease_timeout):
        self.state = "LEADER"
        self.leader_id = self.curNode
        self.log.append("NO OP")
        self.prevLogIndex=len(self.log)-1
        print("appending NO OP to leader")
        # self.votes_received = 0
        old_lease_timeout = max_old_lease_timeout or self.leader_lease_timeout or 0
        text = f"New Leader waiting for Old Leader Lease to timeout: {old_lease_timeout}"
        self.append_dump(text)
        print(f"New Leader waiting for Old Leader Lease to timeout: {old_lease_timeout}")
        # time.sleep(old_lease_timeout)
        self.leader_lease_timeout = time.time() + LEADER_LEASE_TIMEOUT
        print(f"Node {self.node_port} became the leader for term {self.term}.")
        text = f"Node {self.node_port} became the leader for term {self.term}."
        self.append_dump(text)
        self.heartbeatThread = threading.Timer(0, self.send_heartbeats)
        self.heartbeatThread.start()

    def start_leader_lease_timer(self):
        self.leader_lease_timer = threading.Timer(LEADER_LEASE_TIMEOUT, self.leader_lease_expired)
        self.leader_lease_timer.start()

    def leader_lease_expired(self):
        print(f"Leader {self.node_ip}:{self.node_port} lease renewal failed.")
        
        text = f"Leader {self.node_ip}:{self.node_port} lease renewal failed. Stepping down..."
        self.append_dump(text)
        self.state = "FOLLOWER"
        self.leader_id = None
        # self.start_election_timer()

    def start_election_timer(self):
        timeout = self.electionTime
        # print(timeout)
        print(f"{timeout} for {self.node_port}")
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.start() 

    def send_heartbeats(self):
        if self.state == "LEADER":
            # self.restart_election_timer()
            # self.restart_leader_lease_timer()
            ack=1
            for node in self.nodes:
                self.restart_election_timer()
                
                myport=node.split(":")[1]
                if node != f"{self.node_ip}:{self.node_port}":  # Skip sending heartbeat to self
                    try:
                        channel = grpc.insecure_channel(node)
                        stub = nodes_pb2_grpc.RaftManagerServicerStub(channel)  
                        request = nodes_pb2.EntriesRequest(
                            term=self.term,
                            leader_id=f"{self.curNode}",
                            prev_log_index=self.prevLogIndex,
                            prev_log_term=self.prevLogTerm,
                            leader_commit=self.commit_index,
                            leader_lease_timeout=int(self.leader_lease_timeout),
                            entries=self.log, 
                        )
                        reply = stub.AppendEntries(request)  # Call AppendEntries RPC
                        print(f"Leader {self.node_ip}:{self.node_port} sending heartbeat & Renewing Lease to {node}")
                        if reply.success:
                            text = f"Leader {self.node_ip}:{self.node_port} sending heartbeat & Renewing Lease to {node}"
                            self.append_dump(text)
                            ack+=1
                            # Update leader's lease timeout
                            self.leader_lease_timeout = time.time() + LEADER_LEASE_TIMEOUT
                        else:
                            print(f"Error: Heartbeat to {node} failed.")
                    except:
                        print(f"unabel to connect to for sending heartbeat {myport}")
            if ack>len(self.nodes)/2:
                self.restart_leader_lease_timer()
            time.sleep(HEARTBEAT_INTERVAL)
            self.heartbeatThread = threading.Timer(0, self.send_heartbeats)
            self.heartbeatThread.start()

def serve(node):
    server = grpc.server(ThreadPoolExecutor(max_workers=11))
    nodes_pb2_grpc.add_RaftManagerServicerServicer_to_server(node, server)
    server.add_insecure_port(f"localhost:{node.node_port}")
    server.start()
    node.start_election_timer()
    # if node.state == "LEADER":
    #     node.send_heartbeats() jj
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Stopping server due to KeyboardInterrupt...")
        server.stop(None)

if __name__ == "__main__":
    nodes = ["localhost:1111", "localhost:2222", "localhost:3333"]  
    port=input("enter port")
    timeout=int(input("election timeout"))
    log_dir = f"nodes_logs_{port}"
    with open(f"{log_dir}/dumps.txt", "w") as file:
        pass
    with open(f"{log_dir}/logs.txt", "w") as file:
        pass
    with open(f"{log_dir}/metadata.txt", "w") as file:
        pass
    mynode=Node(f"localhost:{port}",nodes,timeout)
    serve(mynode)
