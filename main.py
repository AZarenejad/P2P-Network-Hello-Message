import socket
import threading
import time
import numpy as np
import datetime
import pickle
import random
from random import randrange
import json

number_of_hosts = 0
number_of_maximum_neighbors = 0
hosts_send_hello_message_per_second = 0
treshold_second_for_being_neighbor = 0
seconds_hosts_should_turned_off = 0
period_off_second_for_host = 0
minutes_of_running_program = 0
drop_rate = 0
hosts_info = []
terminate_program = False

def read_config_file(file_path):
	global number_of_hosts, number_of_maximum_neighbors, hosts_send_hello_message_per_second
	global seconds_hosts_should_turned_off, period_off_second_for_host
	global minutes_of_running_program, hosts_info , treshold_second_for_being_neighbor, drop_rate

	with open(file_path) as file_pointer:
		for cnt, line in enumerate(file_pointer):
			if cnt == 0:
				number_of_hosts = int(line)
			elif cnt == 1:
				number_of_maximum_neighbors = int(line)
			elif cnt == 2:
				hosts_send_hello_message_per_second = int(line)
			elif cnt == 3:
				treshold_second_for_being_neighbor = int(line)
			elif cnt == 4:
				seconds_hosts_should_turned_off = int(line)
			elif cnt == 5:
				period_off_second_for_host = int(line)
			elif cnt == 6:
				minutes_of_running_program = int(line)
			elif cnt == 7:
				drop_rate = float(line)
			else:
				host_ip, host_port, host_id = line.strip("\n").split(", ")
				hosts_info.append((int(host_id), host_ip, int(host_port)))


def start_network_run_hosts():
	print("running network ...")
	list_of_hosts = []
	for i in range(number_of_hosts):
		list_of_hosts.append(Host(hosts_info[i][0], hosts_info[i][1], hosts_info[i][2], hosts_info))
		t = threading.Thread(target=list_of_hosts[i].run)
		t.start()
	t1 = threading.Thread(target=choose_random_host_to_be_disable, args=(list_of_hosts,)) 
	t2 = threading.Thread(target=run_program_decide_when_to_finish)
	t1.start()
	t2.start()

def choose_random_host_to_be_disable(list_of_hosts):
	global terminate_program
	while True:
		num = np.random.randint(number_of_hosts)
		if terminate_program:
			break
		list_of_hosts[num].disable()
		time.sleep(seconds_hosts_should_turned_off)

def run_program_decide_when_to_finish():
	global terminate_program
	for i in range(minutes_of_running_program):
		time.sleep(60)
		print("+1 minutes passed")
	print("finish simulating.")
	terminate_program = True


class Host():
	def __init__(self, host_id, host_ip, host_port, all_hosts):
		self.host_id = host_id
		self.host_port = host_port
		self.host_ip = host_ip
		self.all_hosts = all_hosts
		self.is_off = False
		self.socket = None

		self.bidirectional_neighbors = []
		self.unidirectional_neighbors = []
		self.temp_neighbor = None

		self.num_send_to_node = {}
		self.num_recieve_from_node = {}
		self.connection_time = {}
		self.last_message_from_node_to_here = {}
		self.last_message_to_node_from_here = {}
		self.topology = {}
		for host in all_hosts:
			self.topology[host[0]] = {}
			self.num_send_to_node[host[0]] = 0
			self.num_recieve_from_node[host[0]] = 0
			self.connection_time[host[0]] = 0
			self.last_message_from_node_to_here[host[0]] = None
			self.last_message_to_node_from_here[host[0]] = None

	def disable(self):
		self.is_off = True
		t = threading.Thread(target=self.wait_to_enable)
		# print("host with %s:%d with id %d turned off at %s"
		# %(self.host_ip, self.host_port, self.host_id, datetime.datetime.now().time()))
		t.start()
	
	def wait_to_enable(self):
		time.sleep(period_off_second_for_host)
		# print("host with %s:%d with id %d turned on at %s"
		# %(self.host_ip, self.host_port, self.host_id, datetime.datetime.now().time()))
		self.is_off = False
	

	def find_new_neighbor(self):
		self.temp_neighbor = None
		candidate_neighbors = []
		if len(self.unidirectional_neighbors):
			self.temp_neighbor = self.unidirectional_neighbors[randrange(len(self.unidirectional_neighbors))]
		else:
			candidate_neighbors = [x for x in self.all_hosts if x not in self.bidirectional_neighbors and x[0]!=self.host_id]
			self.temp_neighbor = candidate_neighbors[randrange(len(candidate_neighbors))]

	def send_to_all_neighbors(self):
		ready_to_send = []
		for node in self.bidirectional_neighbors:
			ready_to_send.append(node)
		for node in self.unidirectional_neighbors:
			ready_to_send.append(node)
		if self.temp_neighbor is not None:
			ready_to_send.append(self.temp_neighbor)
		
		for node in ready_to_send:
			self.send_hello_message_to_node(node)
			ready_to_send.remove(node)

	def send(self):
		while True:
			if terminate_program:
				break
			if not self.is_off:
				# print("host %s:%d with id %d ready to send message at %s"%(self.host_ip, self.host_port,
				#  self.host_id, datetime.datetime.now().time()))
				if len(self.bidirectional_neighbors) < number_of_maximum_neighbors:
					self.find_new_neighbor()
				self.send_to_all_neighbors()
				

			else:
				# print("jost %s:%d with id %d is off now to send."%(self.host_ip, self.host_port, self.host_id))
				self.unidirectional_neighbors.clear()
				self.bidirectional_neighbors.clear()
		
			time.sleep(hosts_send_hello_message_per_second)
		return

	def recieve(self):
		global terminate_program
		while True:
			if terminate_program:
				break
			if not self.is_off:
				data, addr = self.socket.recvfrom(12048)
				message = pickle.loads(data)
				now = time.time()
				self.num_recieve_from_node[message.sender_id] += 1
				sender_info = (message.sender_id, message.sender_ip, message.sender_port)

				self.topology[sender_info[0]]["bidirectional_neighbors"] = message.bidirectional_neighbors
				self.topology[sender_info[0]]["unidirectional_neighbors"] = message.unidirectional_neighbors

				if ((self.host_id, self.host_ip, self.host_port) in message.unidirectional_neighbors
				or (self.host_id, self.host_ip, self.host_port) in message.bidirectional_neighbors):
					if sender_info not in self.bidirectional_neighbors:
						if sender_info in self.unidirectional_neighbors:
							self.unidirectional_neighbors.remove(sender_info)
						if len(self.bidirectional_neighbors) < number_of_maximum_neighbors:
							self.bidirectional_neighbors.append(sender_info)
							if self.connection_time[sender_info[0]] == None:
								self.connection_time[sender_info[0]] = now
							t = threading.Thread(target = self.check_to_remove_host_if_not_hear_for_long_time, args=(sender_info,))
							self.last_message_from_node_to_here[message.sender_id] = now
							t.start()
							
				else:
					if sender_info not in self.unidirectional_neighbors:
						if sender_info in self.bidirectional_neighbors:
							self.bidirectional_neighbors.remove(sender_info)
							self.connection_time[message.sender_id] += now - self.last_message_from_node_to_here[message.sender_id]
						self.unidirectional_neighbors.append(sender_info)
						self.last_message_from_node_to_here[message.sender_id] = now
						t = threading.Thread(target = self.check_to_remove_host_if_not_hear_for_long_time, args=(sender_info,))
						t.start()
			else:
				self.unidirectional_neighbors.clear()
				self.bidirectional_neighbors.clear()
		return

	def check_to_remove_host_if_not_hear_for_long_time(self, host):
		while True:
			if terminate_program:
				break
			time.sleep(treshold_second_for_being_neighbor)
			now = time.time()
			if now - self.last_message_from_node_to_here[host[0]] > treshold_second_for_being_neighbor:
				if host in self.bidirectional_neighbors:
					self.bidirectional_neighbors.remove(host)
					self.connection_time[host[0]] += now - self.last_message_from_node_to_here[host[0]]
					break
				elif host in self.unidirectional_neighbors:
					self.unidirectional_neighbors.remove(host)
					break
		return

	def send_hello_message_to_node(self, host):
		now = time.time()
		message = Message(sender_id = self.host_id, sender_ip = self.host_ip, sender_port = self.host_port
		, bidirectional_neighbors = self.bidirectional_neighbors
		, unidirectional_neighbors = self.unidirectional_neighbors
		, last_message_from_sender_to_reciever = now
		, last_message_from_reciever_to_sender = self.last_message_from_node_to_here[host[0]])
		data = pickle.dumps(message)
		self.send_data_with_loss(data, (host[1], host[2]))
		self.num_send_to_node[host[0]] += 1
		self.last_message_to_node_from_here[host[0]] = now


	def send_data_with_loss(self, data, addr):
		if random.uniform(0,1) >= drop_rate:
			self.socket.sendto(data, addr)
		else:
			pass
			# print("drop message from host %s:%d to host %s:%s"%(self.host_ip, self.host_port, addr[0], addr[1]))
	
	def run(self):
		# print("host %s:%d with id %s start"%(self.host_id, self.host_port, self.host_id))
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.socket.bind((self.host_ip, self.host_port))
		t1 = threading.Thread(target=self.recieve)
		t2 = threading.Thread(target=self.send)
		t1.start()
		t2.start()
		while not terminate_program:
			pass
		self.report()

	def report(self):
		self.topology[self.host_id]["unidirectional_neighbors"] = self.unidirectional_neighbors
		self.topology[self.host_id]["bidirectional_neighbors"] = self.bidirectional_neighbors
		report = {}
		report['current_neighbors'] = self.bidirectional_neighbors
		report['topology'] = self.topology
		neighbors_report = {}
		for host in self.all_hosts:
			if not host[0] == self.host_id:
				neighbors_report[str(host)] = {}
				neighbors_report[str(host)]["available time"] = self.connection_time[host[0]]
				neighbors_report[str(host)]["available time percent"] = 100* self.connection_time[host[0]] / float(300)
				neighbors_report[str(host)]["send to"] = self.num_send_to_node[host[0]]
				neighbors_report[str(host)]["revieve from"] = self.num_recieve_from_node[host[0]]
		report["neighbors_report"] = neighbors_report
		with open(str(self.host_id) + ".json", "w") as outfile:
			json.dump(report, outfile, indent = 4)

class Message:
	def __init__(self, sender_id, sender_ip, sender_port, message_type = "hello"
	, bidirectional_neighbors = []
	, unidirectional_neighbors = []
	, last_message_from_reciever_to_sender = 0
	, last_message_from_sender_to_reciever = 0):
		self.sender_id = sender_id
		self.sender_ip = sender_ip
		self.sender_port = sender_port
		self.bidirectional_neighbors = bidirectional_neighbors
		self.unidirectional_neighbors = unidirectional_neighbors
		self.last_message_from_reciever_to_sender = last_message_from_reciever_to_sender
		self.last_message_from_sender_to_reciever = last_message_from_sender_to_reciever


if __name__ == "__main__":
	read_config_file("config.txt")
	start_network_run_hosts()