import logging
import os
import random

import torch
from torch.utils.data import random_split
from transformers import GPT2Tokenizer, GPT2LMHeadModel
from transformers import TrainingArguments, Trainer
from transformers.pipelines.base import Dataset

from scripts.utility import azure_table_adaper
from scripts.utility.azure_table_adaper import TableBroker

os.environ["WANDB_DISABLED"] = "true"
logging.basicConfig(level=logging.INFO)


class CustomDataset(Dataset):
	_input_id: str = 'input_ids'
	_attention_mask: str = 'attention_mask'

	def __init__(self, text_list, tokenizer, max_length, truncation=False):
		self.input_ids = []
		self.attention_mask = []
		self.labels = []
		for text in text_list:
			encodings_dict = tokenizer(text, truncation=truncation, max_length=max_length)
			self.input_ids.append(torch.tensor(encodings_dict[self._input_id]))
			self.attention_mask.append(torch.tensor(encodings_dict[self._attention_mask]))

	def __len__(self):
		return len(self.input_ids)

	def __getitem__(self, index):
		return self.input_ids[index], self.attention_mask[index]


def create_training_file_for_gpt_model():
	broker: TableBroker = azure_table_adaper.TableBroker()
	all_training_data = broker.get_all_entities("training")
	with open("training.txt", "wb") as f:
		for text_line in all_training_data:
			text = text_line.get("text")
			line = "<|startoftext|>" + f"{text}" + "<|endoftext|>" + "\n"
			f.write(line.encode("utf-8"))


def train_gpt_model():

	model_type = ""
	model_name = f"sexy-prompt-bot{model_type}"

	parent_directory = "/content/model_base"

	model_output_dir = f"{parent_directory}/{model_name}"

	tokenizer_path = f"{model_output_dir}"

	data_lines = []
	with open('training.txt', 'r', encoding="UTF-8") as f:
		lines = f.readlines()
		for line in lines:
			data_lines.append(line)

	random.shuffle(data_lines)

	tokenizer = GPT2Tokenizer.from_pretrained(f"gpt2{model_type}")

	model = GPT2LMHeadModel.from_pretrained(f"gpt2{model_type}")

	special_tokens_dict = {
		"bos_token": "<|startoftext|>",
		"eos_token": "<|endoftext|>",
		"additional_special_tokens": [
			"<|endoftext|>",
			"<|startoftext|>"
		]
	}

	print(tokenizer.eos_token)

	num_added_toks = tokenizer.add_special_tokens(special_tokens_dict)

	print(":: We have added", num_added_toks, "tokens")

	print(tokenizer.eos_token)

	# Notice: resize_token_embeddings expect to receive the full size of the new vocabulary,
	# i.e., the length of the tokenizer.
	model.resize_token_embeddings(len(tokenizer))

	model.save_pretrained(model_output_dir)

	tokenizer.save_pretrained(tokenizer_path)

	model = GPT2LMHeadModel.from_pretrained(model_output_dir)

	tokenizer = GPT2Tokenizer.from_pretrained(tokenizer_path)

	model.cuda()

	generator = torch.Generator()

	generator.manual_seed(0)

	print(f":: Total Number Of Samples {len(data_lines)}")

	max_length = max([len(tokenizer.encode(prompt)) for prompt in data_lines])

	print(f":: Max Length Of Sample {max_length}")

	dataset = CustomDataset(data_lines, tokenizer, max_length=max_length)

	train_size = int(0.9 * len(dataset))

	train_dataset, eval_dataset = random_split(dataset, [train_size, len(dataset) - train_size], generator=generator)

	training_args = TrainingArguments(output_dir=model_output_dir)
	training_args.num_train_epochs = 5
	training_args.per_device_train_batch_size = 1
	training_args.per_device_eval_batch_size = 1
	training_args.logging_steps = 50
	training_args.save_steps = 1000
	training_args.weight_decay = 0.0
	training_args.logging_dir = './logs'
	training_args.fp16 = True
	training_args.auto_find_batch_size = True
	training_args.gradient_accumulation_steps = 50
	training_args.learning_rate = 1e-4

	trainer: Trainer = Trainer(
		model=model,
		args=training_args,
		train_dataset=train_dataset,
		eval_dataset=eval_dataset,
		data_collator=lambda data: {
			'input_ids': torch.stack([f[0] for f in data]),
			'attention_mask': torch.stack([f[1] for f in data]),
			'labels': torch.stack([f[0] for f in data])
		}
	)

	trainer.train()

	trainer.save_model(model_output_dir)


def talk_to_gpt_model():
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

	result_distinct = []
	sample_outputs = model.generate(inputs=inputs,
									attention_mask=attention_mask,
									do_sample=True,
									max_length=50,
									num_return_sequences=100,
									repetition_penalty=1.1)

	for i, sample_output in enumerate(sample_outputs):
		result = tokenizer.decode(sample_output, skip_special_tokens=True)
		if result not in result_distinct:
			result_distinct.append(result)

	for elem in result_distinct:
		print(elem)

if __name__ == '__main__':
	# create_training_file_for_gpt_model()
	talk_to_gpt_model()