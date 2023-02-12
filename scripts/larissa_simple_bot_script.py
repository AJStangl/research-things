import hashlib
import json
import logging
import random

import praw
import sys
import threading
from multiprocessing import Process
from typing import Any
import time
import torch
from azure.core.paging import ItemPaged
from azure.data.tables import TableClient
from azure.data.tables import TableServiceClient
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueMessage, QueueServiceClient, TextBase64EncodePolicy, QueueProperties
from diffusers import StableDiffusionPipeline
from praw import Reddit
from praw.exceptions import RedditAPIException
from transformers import GPT2Tokenizer, GPT2LMHeadModel


class BlobBroker(object):
	logging.getLogger("azure.storage").setLevel(logging.WARNING)

	def __init__(self, container_name, blob_name):
		self.blob_service_client = BlobServiceClient.from_connection_string(
			"DefaultEndpointsProtocol=https;AccountName=ajdevreddit;AccountKey=+9066TCgdeVignRdy50G4qjmNoUJuibl9ERiTGzdV4fwkvgdV3aSVqgLwldgZxj/UpKLkkfXg+3k+AStjFI33Q==;BlobEndpoint=https://ajdevreddit.blob.core.windows.net/;QueueEndpoint=https://ajdevreddit.queue.core.windows.net/;TableEndpoint=https://ajdevreddit.table.core.windows.net/;FileEndpoint=https://ajdevreddit.file.core.windows.net/;")
		self.container_name = container_name
		self.blob_name = blob_name

	def download_blob(self):
		return self.blob_service_client.get_blob_client(container=self.container_name,
														blob=self.blob_name).download_blob()

	def upload_blob(self, data):
		return self.blob_service_client.get_blob_client(container=self.container_name, blob=self.blob_name).upload_blob(
			data, overwrite=True)


class TableBroker(object):
	def __init__(self):
		self.connection_string = "DefaultEndpointsProtocol=https;AccountName=ajdevreddit;AccountKey=+9066TCgdeVignRdy50G4qjmNoUJuibl9ERiTGzdV4fwkvgdV3aSVqgLwldgZxj/UpKLkkfXg+3k+AStjFI33Q==;BlobEndpoint=https://ajdevreddit.blob.core.windows.net/;QueueEndpoint=https://ajdevreddit.queue.core.windows.net/;TableEndpoint=https://ajdevreddit.table.core.windows.net/;FileEndpoint=https://ajdevreddit.file.core.windows.net/;"

	def get_table_service_client(self) -> TableServiceClient:
		service = TableServiceClient.from_connection_string(conn_str=self.connection_string)
		return service

	def get_table_client(self, table_name: str) -> TableClient:
		service: TableServiceClient = self.get_table_service_client()
		return service.get_table_client(table_name=table_name)

	def write_to_table(self, table_name: str, entity: dict):
		table_client: TableClient = self.get_table_client(table_name=table_name)
		table_client.create_entity(entity=entity)
		return


class MessageBroker(object):
	logging.getLogger("azure.storage").setLevel(logging.WARNING)

	def __init__(self):
		self.connection_string: str = "DefaultEndpointsProtocol=https;AccountName=ajdevreddit;AccountKey=+9066TCgdeVignRdy50G4qjmNoUJuibl9ERiTGzdV4fwkvgdV3aSVqgLwldgZxj/UpKLkkfXg+3k+AStjFI33Q==;BlobEndpoint=https://ajdevreddit.blob.core.windows.net/;QueueEndpoint=https://ajdevreddit.queue.core.windows.net/;TableEndpoint=https://ajdevreddit.table.core.windows.net/;FileEndpoint=https://ajdevreddit.file.core.windows.net/;"
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


class ProcessManager(threading.Thread):
	def __init__(self, proc_name: str):
		super().__init__(name=proc_name, daemon=True)
		self.message_broker_instance: MessageBroker = MessageBroker()
		self.poll_for_message_worker_thread = threading.Thread(target=self.poll_for_reply_queue, args=(), daemon=True,
															   name=proc_name)

	@staticmethod
	def reply_to_thing(q: dict):
		message_broker_instance = MessageBroker()
		print(f"Got reply: {q}")
		data_dict = q
		text = data_dict.get("Text")
		prompt = data_dict.get("Prompt")
		sender = data_dict.get("Sender")
		topic = data_dict.get("Topic")
		channel = data_dict.get("Channel")
		comment_id = data_dict.get("CommentId")
		connection_id = data_dict.get("ConnectionId")
		out_queue = data_dict.get("ConnectionId")
		reply = None
		try:
			sd_pipeline = StableDiffusionPipeline.from_pretrained("D:\\models\\SexyDiffusion-V2", revision="fp16",
																  torch_dtype=torch.float16, safety_checker=None)

			output_images = SimpleBot.create_image(prompt, sd_pipeline, "0")
			if output_images is not None:
				local_path = f"D://images//{output_images}"
				with open(local_path, "rb") as f:
					image_data = f.read()
					BlobBroker(container_name='images', blob_name=output_images).upload_blob(image_data)
					final_remote_path = f"https://ajdevreddit.blob.core.windows.net/images/{output_images}"
					reply = final_remote_path
					print(final_remote_path)
		except Exception as e:
			print(f"Error generating reply: {e}")
			reply = "I'm sorry, I'm not feeling well today. I'll be back later."
			pass

		out_put = json.dumps(
			{
				"text": reply,
				"prompt": prompt,
				"sender": sender,
				"commentId": comment_id,
				"topic": topic,
				"connectionId": connection_id,
				"isBot": True,
				"isThinking": False,
				"channel": channel
			}
		)
		try:
			print(f"Sending reply: {out_put}")
			message_broker_instance.put_message(out_queue, out_put)
		except:
			print(f"Error putting message on queue: {out_queue}")

	def poll_for_reply_queue(self):
		while True:
			try:
				message: QueueMessage = self.message_broker_instance.get_message("chat-input")
				if message is not None:
					q = json.loads(message.content)
					p = Process(target=self.reply_to_thing, args=(q,), daemon=True)
					p.start()
					self.message_broker_instance.delete_message("chat-input", message)
					p.join()
			finally:
				time.sleep(10)

	def run(self):
		self.poll_for_message_worker_thread.start()

	def stop(self):
		sys.exit(0)


class SimpleBot(threading.Thread):
	def __init__(self, proc_name: str, stable_diffusion_model):
		super().__init__(name=proc_name, daemon=True)
		self.language_model_path = "D:\\models\\sexy-prompt-bot"
		self.tokenizer = GPT2Tokenizer.from_pretrained(self.language_model_path)
		self.model = GPT2LMHeadModel.from_pretrained(self.language_model_path)
		self.sd_pipeline = stable_diffusion_model
		self.table_broker: TableBroker = TableBroker()
		self.poll_for_message_worker_thread = threading.Thread(target=self.main_process, args=(), daemon=True, name=proc_name)
		self.things_to_say: [str] = []

	@staticmethod
	def create_image(prompt: str, sd_pipeline: StableDiffusionPipeline, device: str) -> str:
		sd_pipeline.to("cuda:" + device)
		image = sd_pipeline(prompt, height=512, width=512, guidance_scale=10, num_inference_steps=90).images[0]
		hash_name = f"{hashlib.md5(prompt.encode()).hexdigest()}"
		upload_file = f"{hash_name}.png"
		image_path = f"D://images//{upload_file}"
		image.save(image_path)
		return upload_file

	def create_prompt(self):
		if len(self.things_to_say) > 0:
			random.shuffle(self.things_to_say)
			return self.things_to_say.pop()
		try:
			model_name = f"sexy-prompt-bot"

			parent_directory = "/models/"

			model_output_dir = f"{parent_directory}/{model_name}"

			question = "<|startoftext|>"

			prompt = f"{question}"

			device = torch.device(f"cuda" if torch.cuda.is_available() else "cpu")

			tokenizer = GPT2Tokenizer.from_pretrained(model_output_dir)

			model = GPT2LMHeadModel.from_pretrained(model_output_dir)

			generation_prompt = tokenizer(prompt, add_special_tokens=False, return_tensors="pt")

			model.to(device)

			generation_prompt.to(device)

			inputs = generation_prompt.input_ids

			attention_mask = generation_prompt['attention_mask']

			sample_outputs = model.generate(inputs=inputs,
											attention_mask=attention_mask,
											do_sample=True,
											max_length=50,
											num_return_sequences=1000,
											repetition_penalty=1.1)

			for i, sample_output in enumerate(sample_outputs):
				result = tokenizer.decode(sample_output, skip_special_tokens=True)
				if result in self.things_to_say:
					continue
				else:
					self.things_to_say.append(result)

			random.shuffle(self.things_to_say)
			return self.things_to_say.pop()

		except Exception as e:
			print(e)
			return self.create_prompt()

	def write_image_to_cloud(self, image_name):
		local_path = f"D://images//{image_name}"
		with open(local_path, "rb") as f:
			image_data = f.read()
			BlobBroker(container_name='images', blob_name=image_name).upload_blob(image_data)
			final_remote_path = f"https://ajdevreddit.blob.core.windows.net/images/{image_name}"
			return final_remote_path

	def write_output_to_table_storage_row(self, final_remote_path, prompt):
		random_id = random.randint(0, 123456890)
		entity = {
			"PartitionKey": "General",
			"RowKey": str(random_id),
			"Text": final_remote_path,
			"Prompt": prompt,
			"Sender": "ImageBot",
			"CommentId": random_id,
			"Topic": "General",
			"ConnectionId": "chat-output",
			"IsBot": True,
			"IsThinking": False,
			"Channel": "General"
		}

		TableBroker().write_to_table(table_name="messages", entity=entity)

	def main_process(self):
		while True:
			image_prompt: str = self.create_prompt()
			print("Prompt: " + image_prompt)
			image_output: str = self.create_image(image_prompt, sd_model, "1")
			remote_path: str = self.write_image_to_cloud(image_output)
			bot.write_output_to_table_storage_row(remote_path, image_prompt)

			try:
				instance: Reddit = praw.Reddit(site_name="KimmieBotGPT")
				print(instance.user.me())
				sub = instance.subreddit("CoopAndPabloArtHouse")
				print(sub)
				sub.submit_image(title=image_prompt, image_path=f"D://images//{image_output}", nsfw=True)
			except Exception as e:
				print(e)
				continue
			time.sleep(60)

	def run(self):
		self.poll_for_message_worker_thread.start()

	def stop(self):
		sys.exit(0)


if __name__ == '__main__':
	sd_model = StableDiffusionPipeline.from_pretrained("D:\\models\\SexyDiffusion-V2", revision="fp16",
													   torch_dtype=torch.float16, safety_checker=None)

	proc = ProcessManager(f"ProcessManager-1")
	proc.start()

	bot: SimpleBot = SimpleBot(sd_model, "SimpleBot-1")
	bot.start()

	while True:
		try:
			time.sleep(1)
		except KeyboardInterrupt:
			logging.info('Shutdown')
			proc.stop()
			bot.stop()
			exit(0)
