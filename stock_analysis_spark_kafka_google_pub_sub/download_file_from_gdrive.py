import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# Folder ID from your URL
folder_id = "1bt94v6pTR6s58c8HFX1FU_9zMoXFX4cf"

# Setup credentials using service account
credentials = service_account.Credentials.from_service_account_file(
    'service-account-key.json', 
    scopes=['https://www.googleapis.com/auth/drive.readonly']
)

# Build the Drive API client
service = build('drive', 'v3', credentials=credentials)

def download_folder(folder_id, output_dir='.'):
    # Create output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Get list of all files in the folder
    results = service.files().list(
        q=f"'{folder_id}' in parents",
        fields="files(id, name, mimeType)").execute()
    items = results.get('files', [])
    
    if not items:
        print(f'No files found in folder {folder_id}')
        return
    
    # Download each file
    for item in items:
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            # If item is a folder, download recursively
            subfolder_path = os.path.join(output_dir, item['name'])
            print(f"Processing subfolder: {item['name']}")
            download_folder(item['id'], subfolder_path)
        else:
            # If item is a file, download it
            print(f"Downloading file: {item['name']}")
            request = service.files().get_media(fileId=item['id'])
            
            file_path = os.path.join(output_dir, item['name'])
            with io.FileIO(file_path, 'wb') as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    print(f"Download {int(status.progress() * 100)}%")

# Start downloading
download_folder(folder_id, 'downloaded_files')