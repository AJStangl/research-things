import logging
import os
from typing import Any

from azure.core.paging import ItemPaged
from azure.storage.queue import TextBase64EncodePolicy, QueueServiceClient, QueueMessage, QueueProperties

logging.getLogger("azure.storage").setLevel(logging.WARNING)


class QueueAdapter(object):
	def __init__(self):
		self.connection_string: str = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
		self.service: QueueServiceClient = QueueServiceClient.from_connection_string(self.connection_string,
																					 encode_policy=TextBase64EncodePolicy())


	def put_message(self, queue_name: str, content: Any, time_to_live=None) -> QueueMessage:
		if time_to_live is None:
			return self.service.get_queue_client(queue_name).send_message(content=content)
		else:
			return self.service.get_queue_client(queue_name).send_message(content=content, time_to_live=time_to_live)


	def get_message(self, queue_name: str) -> QueueMessage:
		return self.service.get_queue_client(queue_name).receive_message()


	def delete_message(self, queue_name: str, q, pop_receipt=None):
		return self.service.get_queue_client(queue_name).delete_message(q, pop_receipt)


	def get_queues(self) -> ItemPaged[QueueProperties]:
		return self.service.list_queues()


	def delete_queue(self, queue_name: str):
		return self.service.delete_queue(queue_name)