import re
import random

from calvin.utilities import calvinresponse
from calvin.utilities.calvin_callback import CalvinCB


class LostActorManager(object):

	"""
	Class for helping counting and stop replication in case of that an actor is lost
	and a certain reliability level is desired
	"""

	def __init__(self, node, lost_actor_id, lost_actor_info, required_reliability, uuid_re):
		self.node = node
		self.lost_actor_id = lost_actor_id
		self.lost_actor_info = lost_actor_info
		self.name = re.sub(uuid_re, "", lost_actor_info['name'])
		self.required_reliability = required_reliability
		self.uuid_re = uuid_re

		self.current_reliability = 0
		self.replica_id = 0

	def find_and_replicate(self, actor_ids):
		for i, actor_id in enumerate(actor_ids):
			self.node.storage.get_actor(actor_id, cb=CalvinCB(self._is_replica, stop = i==len(actor_ids)-1))    

	def _is_replica(self, key, value, stop):
		actor_name = re.sub(self.uuid_re, "", value['name'])
		if actor_name == self.name and not key == self.lost_actor_id:
			self._found_replica(key, value)

		if stop:
			self._replicate()

	def _found_replica(self, actor_id, actor_info):
		self.replica_id = actor_id
		self.replica_info = actor_info
		self.current_reliability += 1

	def _replicate(self):
		print 'We need to do', self.required_reliability - self.current_reliability, 'replicas'
		if self.replica_id != 0:
			self.node.storage.get_actor(self.replica_id, CalvinCB(self._replicate_cb))

	def _replicate_cb(self, key, value):
		linklist = [self.node.id]
		linklist.extend(self.node.network.list_links())
		
		#while self.required_reliability > self.current_reliability:
		peer_node_id = random.choice(linklist)
		self.node.proto.actor_replication_request(key, value['node_id'], peer_node_id, None)
		#	self.current_reliability += 1

		self._delete_lost_actor_info()

	def _status_cb(self, status, *args, **kwargs):
		if status.status == calvinresponse.OK:
			print 'Ev. update status' 
			#self.current_reliability += 1

	def _delete_lost_actor_info(self):
		# Delete information about the lost actor
		if self.node.id == self.lost_actor_info['node_id']:
			try:
				self.node.am.delete_actor(self.lost_actor_id)
				self.node.storage.trigger_flush(delay=0)
			except Exception as e:
				print e
				#_log.exception("Destroy actor info failed")
		else:
			#Request deletion of actor info on another node
			self.node.proto.actor_destroy(self.lost_actor_info['node_id'], self.lost_actor_id)