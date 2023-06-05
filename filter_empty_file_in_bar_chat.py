import os.path
import os


def remove_empty_files(path):
    for root, dirs, files in os.walk(path):
        for filename in files:
            file_path = os.path.join(root, filename)
            try:
                if os.path.getsize(file_path) <= 1000:
                    os.remove(file_path)
            except OSError:
                print(f"Error deleting {file_path}")


remove_empty_files("Bar_chat_report")
