from typing import OrderedDict

import nltk
import torch
from PIL import Image
from nltk import TweetTokenizer, sent_tokenize
from transformers import VisionEncoderDecoderModel, ViTFeatureExtractor, GPT2TokenizerFast, AutoTokenizer


class ImageCaption:
	def __init__(self):
		self.tokenizer = None
		self.model = None
		self.feature_extractor = None
		self.device = torch.device("cpu")

	def get_nlk_tokens(self, text: str):
		try:
			tokenizer = TweetTokenizer()

			first_sentence = sent_tokenize(text)[0]

			# remove numbers and tokenize the text
			tokenized = tokenizer.tokenize(first_sentence.translate({ord(ch): None for ch in '0123456789'}))

			tokenized = [i for i in tokenized if len(i) > 1]

			tokenized = list(OrderedDict.fromkeys(tokenized))

			pos_tagged_text = nltk.pos_tag(tokenized)

			# Extract all nouns, verbs and adverbs and append to the existing
			prompt_keywords = [i[0] for i in pos_tagged_text if i[1][:2] in ['NN', 'VB', 'RB']]

			prompt_keywords = list(OrderedDict.fromkeys(prompt_keywords))

			return prompt_keywords
		except Exception as e:
			print(e)
			return []

	def get_vit_caption(self):
		model = VisionEncoderDecoderModel.from_pretrained("D:\\models\\vit-gpt2-image-captioning")
		feature_extractor = ViTFeatureExtractor.from_pretrained("D:\\models\\vit-gpt2-image-captioning")
		tokenizer = AutoTokenizer.from_pretrained("D:\\models\\vit-gpt2-image-captioning")
		return model, feature_extractor, tokenizer

	def get_default_caption(self):
		model = VisionEncoderDecoderModel.from_pretrained("D:\\models\\image-caption-generator")
		feature_extractor = ViTFeatureExtractor.from_pretrained("D:\\models\\image-caption-generator")
		tokenizer = GPT2TokenizerFast.from_pretrained("D:\\models\\image-caption-generator\\tokenizer")
		return model, feature_extractor, tokenizer

	def set_pipeline(self, model_name: str):
		if model_name == "vit":
			self.model, self.feature_extractor, self.tokenizer = self.get_vit_caption()
		else:
			self.model, self.feature_extractor, self.tokenizer = self.get_default_caption()

	def caption_image(self, image_path: str, index: int) -> str:
		print(f":: Captioning image: {image_path}")
		try:
			device_index: str = str(index % 2)
			self.device = torch.device(f"cuda" + ":" + device_index)
			self.set_pipeline("default")
			self.model.to(self.device)

			img = Image.open(image_path)
			if img.mode != 'RGB':
				img = img.convert(mode="RGB")

			pixel_values = self.feature_extractor(images=[img], return_tensors="pt").pixel_values
			pixel_values = pixel_values.to(self.device)

			max_length = 128
			num_beams = 4

			# get model prediction
			output_ids = self.model.generate(pixel_values, num_beams=num_beams, max_length=max_length)

			# decode the generated prediction
			predictions = self.tokenizer.decode(output_ids[0], skip_special_tokens=True)
			print(f":: Completed captioning image: {image_path}\t{predictions}")
			return predictions

		except Exception as e:
			print(f":: Error in caption_image: {e}")
			return "bruh"

	def caption_image_vit(self, image_path: str) -> str:
		try:
			self.device = torch.device(f"cpu")
			self.set_pipeline("vit")
			self.model.to(self.device)

			max_length = 32

			num_beams = 4

			gen_kwargs = {"max_length": max_length, "num_beams": num_beams}

			images = []

			i_image = Image.open(image_path)
			if i_image.mode != "RGB":
				i_image = i_image.convert(mode="RGB")

			images.append(i_image)

			print(f":: Predicting image: {image_path}")

			pixel_values = self.feature_extractor(images=images, return_tensors="pt").pixel_values

			pixel_values = pixel_values.to(self.device)

			output_ids = self.model.generate(pixel_values, **gen_kwargs)

			print(f":: Decoding output for image: {image_path}")
			predictions = self.tokenizer.batch_decode(output_ids, skip_special_tokens=True)

			prediction = [prediction.strip() for prediction in predictions]

			print(f":: Completed prediction for image: {image_path}")
			if len(prediction) > 0:
				return prediction[0]
			else:
				return None

		except Exception as e:
			print(f":: Process Failed For {image_path} with {e}")
			return None

