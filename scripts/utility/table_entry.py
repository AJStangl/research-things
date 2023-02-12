from typing import Any
from dataclasses import dataclass
import json


@dataclass
class TableEntry:
	PartitionKey: str
	RowKey: str
	image: str
	text: str
	id: str
	author: str
	url: str
	subreddit: str
	flair: str
	permalink: str
	hash: str
	caption: str

	@staticmethod
	def from_dict(obj: Any) -> 'TableEntry':
		_PartitionKey = str(obj.get("PartitionKey"))
		_RowKey = str(obj.get("RowKey"))
		_image = str(obj.get("image"))
		_text = str(obj.get("text"))
		_id = str(obj.get("id"))
		_author = str(obj.get("author"))
		_url = str(obj.get("url"))
		_subreddit = str(obj.get("subreddit"))
		_flair = str(obj.get("flair"))
		_permalink = str(obj.get("permalink"))
		_hash = str(obj.get("hash"))
		_caption = str(obj.get("caption"))
		return TableEntry(_PartitionKey, _RowKey, _image, _text, _id, _author, _url, _subreddit, _flair, _permalink, _hash, _caption)
