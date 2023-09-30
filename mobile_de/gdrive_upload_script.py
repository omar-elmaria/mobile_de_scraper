import logging
import os
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload


def upload_file_to_gdrive(filename, folder_id):
    """
    Uploads a file to Google Drive using v3 of Google Drive API
    """
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds = None
    path_to_token = os.path.expanduser("~") + '/gdrive_token.json'

    # The file gdrive_token.json stores the user's access and refresh tokens, and is created automatically when the authorization flow completes for the first time.
    if os.path.exists(path_to_token):
        creds = Credentials.from_authorized_user_file(path_to_token, SCOPES) # If you modify the scope, delete the gdrive_token.json file
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(os.path.expanduser("~") + '/email_client_secret.json', SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open(path_to_token, 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('drive', 'v3', credentials=creds)

        # Define the file's metadata
        file_metadata = {
            'name': filename,
            'mimeType': '*/*',
            'parents': [folder_id] # The ID of the folder in which the file will be uploaded. You can get it from the URL
        }

        # Create a media object
        media = MediaFileUpload(filename, mimetype='*/*', resumable=True)

        # Upload the file to Google Drive
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        logging.info(F'File with ID: "{file.get("id")}" has been uploaded.')
    except HttpError as error:
        logging.info(F'An error occurred: {error}')
        file = None
    return file.get('id')

# Folder ID is the ID of the folder called "vm_logs"
# upload_file_to_gdrive(filename=f"mobile_logs_cat_all_{datetime.strftime(datetime.now().date(), '%Y%m%d')}.log", folder_id="16e4f41zhwV67Pm01I0jHwL2l59kpn8WY")