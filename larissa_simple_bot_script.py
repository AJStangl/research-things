import hashlib
import json
import logging
import random
import threading

import random
import praw
import sys
import time
import torch
from azure.storage.queue import QueueMessage
from diffusers import StableDiffusionPipeline, StableDiffusionLatentUpscalePipeline
from transformers import pipeline
from praw import Reddit
from praw.reddit import Submission
from transformers import GPT2Tokenizer, GPT2LMHeadModel
from shared_code.utility.spark import set_environ
import os
import subprocess

set_environ.set_azure_env()

from shared_code.utility.storage.blob import BlobAdapter
from shared_code.utility.storage.queue import QueueAdapter
from shared_code.utility.storage.table import TableAdapter

logging.getLogger("diffusers").setLevel(logging.WARNING)
logging.getLogger("azure.storage").setLevel(logging.WARNING)

import os
os.environ["CUDA_VISIBLE_DEVICES"] = "0,1"

class FuckingStatic:
	@staticmethod
	def validate_message(message):
		try:
			import re
			start_end_regex = re.compile("<\|startoftext\|>(.+?)<\|endoftext\|>")
			prompt_regex = re.compile("<\|prompt\|>(.+?)<\|text\|>")
			text_regex = re.compile("<\|text\|>(.+?)<\|endoftext\|>")
			found_start_end = start_end_regex.findall(message)
			if len(found_start_end) == 0:
				return "", ""

			generated_prompt = ""
			generated_text = ""

			found_prompt = prompt_regex.findall(message)
			if len(found_prompt) > 0:
				generated_prompt = found_prompt[0]

			found_text = text_regex.findall(message)
			if len(found_text) > 0:
				generated_text = found_text[0]

			return generated_prompt.strip(), generated_text.strip()
		except Exception as e:
			print(e)
			return "", ""

	@staticmethod
	def create_enhanced_image(image_path) -> str:
		out_path = "D:\\images\\results"

		env_copy = os.environ.copy()
		env_copy['CUDA_VISIBLE_DEVICES'] = "''"
		print(f"Starting Script For Face Restoration for {image_path}")

		print(f"Torch Status: {torch.cuda.is_available()}")

		command = f"python D:\\code\\repos\\GFPGAN\\inference_gfpgan.py -i {image_path} -v 1.4 -s 2 -o {out_path}"

		print(f"Running command: {command}")

		result = subprocess.call(command, shell=True, env={**env_copy})

		if result == 0:
			final_path = f"{out_path}\\restored_imgs\\{os.path.basename(image_path)}"
			print(
				f"Success: Image Processed and located in {final_path}")
			return final_path
		else:
			print(f"Error: Image Processing Failed")
			return image_path

	@staticmethod
	def create_image(prompt: str, pipe: StableDiffusionPipeline, device_name: str) -> (str, int, int):
		try:
			pipe.to("cuda:" + device_name)
			guidance_scale = random.randint(5, 20)
			num_inference_steps = random.randint(50, 200)
			args = {
				"model": pipe.config_name,
				"guidance_scale": guidance_scale,
				"num_inference_steps": num_inference_steps
			}
			print(json.dumps(args, indent=4))

			initial_image = pipe(prompt, height=512, width=512, guidance_scale=guidance_scale, num_inference_steps=num_inference_steps).images[0]

			hash_name = f"{hashlib.md5(prompt.encode()).hexdigest()}"

			upload_file = f"{hash_name}.png"

			image_path = f"D://images//{upload_file}"

			initial_image.save(image_path)

			final_image = FuckingStatic.create_enhanced_image(image_path)

			return final_image, guidance_scale, num_inference_steps

		except Exception as e:
			print(e)
			return None
		finally:
			del pipe
			torch.cuda.empty_cache()


class PipeLineHolder(object):
	pipe_line_name: str
	diffusion_pipeline_path: str
	text_model_path: str

	def __init__(self, pipe_line_name: str, diffusion_pipeline_path: str, text_model_path: str):
		self.pipe_line_name: str = pipe_line_name
		self.diffusion_pipeline_path: str = diffusion_pipeline_path
		self.text_model_path: str = text_model_path


class SimpleBot(threading.Thread):
	def __init__(self, holder: [PipeLineHolder], proc_name: str, instance: int):
		super().__init__(name=proc_name, daemon=True)
		self.holders: [] = holder
		self.table_broker: TableAdapter = TableAdapter()
		self.poll_for_message_worker_thread = threading.Thread(target=self.main_process, args=(), daemon=True, name=proc_name)
		self.things_to_say: [str] = []
		self.counter = 0
		self.instance = instance

	def get_gpt_model(self, model_to_use: PipeLineHolder) -> (GPT2Tokenizer, GPT2LMHeadModel):
		tokenizer = GPT2Tokenizer.from_pretrained(model_to_use.text_model_path)
		model = GPT2LMHeadModel.from_pretrained(model_to_use.text_model_path)
		return tokenizer, model

	def create_prompt(self, pipe_line_holder: PipeLineHolder):

		try:
			device = torch.device(f"cuda:{self.instance}")

			tokenizer, model = self.get_gpt_model(pipe_line_holder)

			question = f"<|startoftext|> <|model|> {pipe_line_holder.pipe_line_name} <|prompt|>"

			prompt = f"{question}"

			generation_prompt = tokenizer(prompt, add_special_tokens=False, return_tensors="pt")

			model.to(device)

			generation_prompt.to(device)

			inputs = generation_prompt.input_ids

			attention_mask = generation_prompt['attention_mask']

			sample_outputs = model.generate(inputs=inputs,
											attention_mask=attention_mask,
											do_sample=True,
											max_length=50,
											num_return_sequences=1,
											repetition_penalty=1.1)

			prompt_for_reddit = ""
			prompt_for_image_generation = ""
			for i, sample_output in enumerate(sample_outputs):
				result = tokenizer.decode(sample_output, skip_special_tokens=False)
				print(result)
				prompt, text = FuckingStatic.validate_message(result)
				prompt_for_reddit = prompt
				prompt_for_image_generation = text

			model.to("cpu")
			generation_prompt.to("cpu")
			torch.cuda.empty_cache()

			return prompt_for_reddit, prompt_for_image_generation

		except Exception as e:
			print(e)
			return self.create_prompt(pipe_line_holder)

	def write_image_to_cloud(self, image_name):
		with open(image_name, "rb") as f:
			image_data = f.read()

			blob_adapter = BlobAdapter("images")

			blob_adapter.upload_blob(blob_name=os.path.basename(image_name), data=image_data)

			final_remote_path = f"https://ajdevreddit.blob.core.windows.net/images/{os.path.basename(image_name)}"

			print(final_remote_path)

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
		table_adapter: TableAdapter = TableAdapter()
		table_adapter.upsert_entity_to_table("messages", entity)

	def main_process(self):
		while True:
			model_index = self.counter % len(self.holders)

			holder: PipeLineHolder = self.holders[model_index]

			print(f":: Using model: {holder.diffusion_pipeline_path}")
			print(f":: Using Device: {self.instance}")


			pipe = StableDiffusionPipeline.from_pretrained(holder.diffusion_pipeline_path, revision="fp16", torch_dtype=torch.float16, safety_checker=None)
			print(":: Model Loaded")

			reddit_text, image_prompt = self.create_prompt(holder)
			print(":: Prompt Created")
			image_prompt = image_prompt.replace("little girl", "petite women")

			print("Reddit Text: " + reddit_text)

			print("Prompt: " + image_prompt)

			gen = f"{reddit_text}, {image_prompt}"

			try:
				(image_output, guidance, num_steps) = FuckingStatic.create_image(gen, pipe, str(self.instance))
			except Exception as e:
				print(e)
				continue

			try:
				instance: Reddit = praw.Reddit(site_name="KimmieBotGPT")
				sub = instance.subreddit("CoopAndPabloArtHouse")
				submission: Submission = sub.submit_image(
					title=f"{reddit_text}",
					image_path=image_output, nsfw=True)

				submission.mod.approve()
				body = f"""
| Prompt         |       Model Name        | Guidance   | Number Of Inference Steps |
|:---------------|:-----------------------:|------------|--------------------------:|
| {gen}          | {os.path.split(holder.diffusion_pipeline_path)[-1]} | {guidance} |               {num_steps} |
				"""

				submission.reply(body)
				self.counter += 1

				final_remote_path = self.write_image_to_cloud(image_output)
				self.write_output_to_table_storage_row(final_remote_path, image_prompt)


			except Exception as e:
				print(e)
				self.counter += 1
				continue


	def run(self):
		self.poll_for_message_worker_thread.start()

	def stop(self):
		sys.exit(0)


if __name__ == '__main__':

	pipeline_1 = PipeLineHolder("SexyDiffusion", "D:\\models\\SexyDiffusion-15500", "D:\\models\\sd-prompt-bot-2")

	pipeline_2 = PipeLineHolder("SexyDiffusion", "D:\\models\\SexyDiffusion-15500", "D:\\models\\sd-prompt-bot-2")

	pipeline_3 = PipeLineHolder("SexyDiffusion", "D:\\models\\SexyDiffusion-15500", "D:\\models\\sd-prompt-bot-2")

	pipeline_4 = PipeLineHolder("SexyDiffusion", "D:\\models\\SexyDiffusion-15500", "D:\\models\\sd-prompt-bot-2")

	# pipeline_5 = PipeLineHolder("CosmicDiffusion", "D:\\models\\CosmicDiffusion", "D:\\models\\sd-prompt-bot-2")
	pipeline_5 = PipeLineHolder("SexyDiffusion", "D:\\models\\SexyDiffusion-15500", "D:\\models\\sd-prompt-bot-2")

	pipe_line_holder_list = [pipeline_1, pipeline_2, pipeline_3, pipeline_4]

	random.shuffle(pipe_line_holder_list)

	bot: SimpleBot = SimpleBot(pipe_line_holder_list, "SimpleBot", 1)
	bot.start()

	time.sleep(60 * 7)
	random.shuffle(pipe_line_holder_list)
	bot_2: SimpleBot = SimpleBot(pipe_line_holder_list, "SimpleBot", 0)
	bot_2.start()

	while True:
		try:
			time.sleep(1)
		except KeyboardInterrupt:
			logging.info('Shutdown')
			bot.stop()
			bot_2.stop()
			exit(0)




# class WebManager(threading.Thread):
# 	def __init__(self, holder: [PipeLineHolder], proc_name: str):
# 		super().__init__(name=proc_name, daemon=True)
# 		self.message_broker_instance: QueueAdapter = QueueAdapter()
# 		self.poll_for_message_worker_thread = threading.Thread(target=self.poll_for_reply_queue, args=(), daemon=True, name=proc_name)
# 		self.holders: [PipeLineHolder] = holder
#
# 	def reply_to_thing(self, q: dict):
# 		message_broker_instance = QueueAdapter()
# 		print(f"Got reply: {q}")
# 		data_dict = q
# 		text = data_dict.get("Text")
# 		prompt = data_dict.get("Prompt")
# 		sender = data_dict.get("Sender")
# 		topic = data_dict.get("Topic")
# 		channel = data_dict.get("Channel")
# 		comment_id = data_dict.get("CommentId")
# 		connection_id = data_dict.get("ConnectionId")
# 		out_queue = data_dict.get("ConnectionId")
# 		reply = None
# 		try:
# 			current_holder: PipeLineHolder = random.choice(self.holders)
# 			model = StableDiffusionPipeline.from_pretrained(current_holder.diffusion_pipeline_path, revision="fp16", torch_dtype=torch.float16, safety_checker=None)
# 			upscaler = StableDiffusionLatentUpscalePipeline.from_pretrained("stabilityai/sd-x2-latent-upscaler", torch_dtype=torch.float16)
# 			output_images, guidance, num_steps = FuckingStatic.create_image(prompt, model, "0", upscaler)
# 			prompt = prompt + " " + "-model-name-" + current_holder.pipe_line_name + " " + "-num-steps-" + str(
# 				num_steps) + " " + "-guidance-" + str(guidance)
#
# 			torch.cuda.empty_cache()
# 			del model
#
# 			if output_images is not None:
# 				local_path = f"D://images//{output_images}"
# 				with open(local_path, "rb") as f:
# 					image_data = f.read()
# 					blob_adapter: BlobAdapter = BlobAdapter("images")
# 					blob_adapter.upload_blob(data=image_data, blob_name=output_images)
# 					final_remote_path = f"https://ajdevreddit.blob.core.windows.net/images/{output_images}"
# 					reply = final_remote_path
# 					print(final_remote_path)
# 		except Exception as e:
# 			del model
# 			print(f":: Error generating reply: {e}")
# 			reply = "I'm sorry, I'm not feeling well today. I'll be back later."
# 			pass
#
# 		out_put = json.dumps(
# 			{
# 				"text": reply,
# 				"prompt": prompt,
# 				"sender": sender,
# 				"commentId": comment_id,
# 				"topic": topic,
# 				"connectionId": connection_id,
# 				"isBot": True,
# 				"isThinking": False,
# 				"channel": channel
# 			}
# 		)
# 		try:
# 			print(f":: Sending reply: {out_put}")
# 			message_broker_instance.put_message(out_queue, out_put)
# 		except Exception as e:
# 			print(f":: Error putting message on queue: {e}")
# 			print(f":: Error putting message on queue: {out_queue}")
#
# 	def poll_for_reply_queue(self):
# 		while True:
# 			try:
# 				message: QueueMessage = self.message_broker_instance.get_message("chat-input")
# 				if message is not None:
# 					q = json.loads(message.content)
# 					self.reply_to_thing(q)
# 					self.message_broker_instance.delete_message("chat-input", message)
# 					time.sleep(30)
# 			except Exception as e:
# 				print(f":: Error polling for reply queue: {e}")
# 				pass
# 			finally:
# 				pass
#
# 	def run(self):
# 		self.poll_for_message_worker_thread.start()
#
# 	def stop(self):
# 		sys.exit(0)
