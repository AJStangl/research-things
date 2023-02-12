import logging
import os

from azure.core.credentials import AzureNamedKeyCredential
from azure.core.paging import ItemPaged
from azure.data.tables import TableClient, TableEntity
from azure.storage.blob import BlobServiceClient

logging.getLogger("azure.storage").setLevel(logging.WARNING)

from azure.data.tables import TableServiceClient


class TableBroker(object):
	credential = AzureNamedKeyCredential(os.environ["AZURE_ACCOUNT_NAME"], os.environ["AZURE_ACCOUNT_KEY"])

	def __init__(self):
		self.service = TableServiceClient(endpoint=os.environ["AZURE_TABLE_ENDPOINT"], credential=self.credential)

	from azure.data.tables import TableServiceClient

	def get_table_service_client(self) -> TableServiceClient:
		return self.service

	def get_table_client(self, table_name: str) -> TableClient:
		service: TableServiceClient = self.get_table_service_client()
		return service.get_table_client(table_name=table_name)

	def upsert_entity_to_table(self, table_name: str, entity: dict):
		table_client: TableClient = self.get_table_client(table_name=table_name)
		table_client.upsert_entity(entity=entity)
		return

	def get_all_entities(self, table_name: str, select: str = None) -> list[dict]:
		table_client: TableClient = self.get_table_client(table_name=table_name)
		if select is not None:
			entities: ItemPaged[TableEntity] = list(table_client.list_entities(select=select))
			return entities
		entities: ItemPaged[TableEntity] = list(table_client.list_entities())
		return entities


class BlobBroker(object):
	logging.getLogger("azure.storage").setLevel(logging.WARNING)

	def __init__(self, container_name, blob_name):
		self.blob_service_client = BlobServiceClient.from_connection_string(
			os.environ["AZURE_STORAGE_CONNECTION_STRING"])
		self.container_name = container_name
		self.blob_name = blob_name

	def download_blob(self):
		return self.blob_service_client.get_blob_client(container=self.container_name,
														blob=self.blob_name).download_blob()

	def upload_blob(self, data):
		return self.blob_service_client.get_blob_client(container=self.container_name, blob=self.blob_name).upload_blob(
			data, overwrite=True)
