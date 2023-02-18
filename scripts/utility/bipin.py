import logging

import torch
from PIL import Image
from transformers import VisionEncoderDecoderModel, ViTFeatureExtractor, GPT2TokenizerFast, AutoTokenizer

logger = logging.getLogger(__name__)

class ImageCaption:
	"""

	"""

	def __init__(self):
		self.tokenizer = None
		self.model = None
		self.feature_extractor = None
		self.device = torch.device("cpu")

	def get_vit_caption(self):
		model = VisionEncoderDecoderModel.from_pretrained("/models/vit-gpt2-image-captioning")
		feature_extractor = ViTFeatureExtractor.from_pretrained("/models/vit-gpt2-image-captioning")
		tokenizer = AutoTokenizer.from_pretrained("/models/vit-gpt2-image-captioning")
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

	def caption_image(self, image_path: str) -> str:
		try:
			self.set_pipeline("default")
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
			return predictions

		except Exception as e:
			print(f"Error in caption_image: {e}")
			return "Error in captioning image"

	def caption_image_vit(self, image_path: str) -> str:
		try:
			self.set_pipeline("vit")
			self.device = torch.device("cpu")
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