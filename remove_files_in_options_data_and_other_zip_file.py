import zipfile
import os
from tqdm import tqdm

option_zip = "C:/Users/Franck/Desktop/options.zip"


def remove_small_files_from_zip(zip_file_path, max_size):
    temp_zip_file_path = "C:/Users/Franck/Desktop/temp.zip"
    with zipfile.ZipFile(zip_file_path, 'r') as original_zip:
        with zipfile.ZipFile(temp_zip_file_path, 'w') as temp_zip:
            for item in tqdm(original_zip.infolist()):
                if item.file_size >= max_size:
                    data = original_zip.read(item.filename)
                    temp_zip.writestr(item, data)

    os.replace(temp_zip_file_path, zip_file_path)


zip_file_path = option_zip
max_size = 50 * 1024

remove_small_files_from_zip(zip_file_path, max_size)
