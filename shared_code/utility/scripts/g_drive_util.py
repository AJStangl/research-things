from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


class gHelper:
	def __init__(self):
		gauth = GoogleAuth()
		gauth.LocalWebserverAuth()
		self.drive: GoogleDrive = GoogleDrive(gauth)

	def create_and_upload(self):
		file = self.drive.CreateFile({'title': 'out.zip'})
		file.SetContentFile('out.zip')
		file.Upload()
