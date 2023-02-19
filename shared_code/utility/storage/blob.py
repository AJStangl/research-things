import logging
import os

from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)
logging.getLogger("azure.storage").setLevel(logging.WARNING)


class BlobAdapter:
	"""

	"""

	def __init__(self, container_name):
		self.blob_service_client = BlobServiceClient.from_connection_string(
			os.environ["AZURE_STORAGE_CONNECTION_STRING"])
		self.container_name = container_name

	def download_blob(self, blob_name: str):
		return self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name) \
			.download_blob()

	def upload_blob(self, data: bytes, blob_name: str):
		return self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name) \
			.upload_blob(data, overwrite=True)

	def get_container_client(self) -> BlobServiceClient:
		return self.blob_service_client.get_container_client(container=self.container_name)
