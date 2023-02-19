class ImageTransformer(object):
	def __init__(self, image, transform):
		self.image = image
		self.transform = transform

	def __call__(self, *args, **kwargs):
		return self.transform(self.image)