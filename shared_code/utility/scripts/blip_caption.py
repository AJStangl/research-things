from collections import OrderedDict

import nltk
import torch
from PIL import Image
from nltk import TweetTokenizer, sent_tokenize
from transformers import BlipProcessor, BlipForConditionalGeneration
class BlipCaption:
	def __init__(self, device_number):
		self.processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-large")
		self.model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-large", torch_dtype=torch.float16).to("cuda:1")
		self.device = torch.device(f"cuda:{str(device_number)}")

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
			prompt_keywords = [i for i in pos_tagged_text if i[1][:2] in ['NN', 'VB', 'RB']]

			prompt_keywords = list(OrderedDict.fromkeys(prompt_keywords))

			return pos_tagged_text
		except Exception as e:
			print(e)
			return []

	def caption_image(self, image_path: str) -> str:
		try:
			raw_image = Image.open(image_path).convert('RGB')

			inputs = self.processor(raw_image, return_tensors="pt").to(self.device, torch.float16)

			out = self.model.generate(**inputs)
			return self.processor.decode(out[0], skip_special_tokens=True, max_new_tokens=50)

		except Exception as e:
			print(e)
			return ""