import os
import shutil

src_dir = "C:/Users/Franck/Desktop/All_yahoo_finance_historical_data"
dst_dir = "C:/Users/Franck/Desktop/Yahoo_Sample_for_Stats"

for filename in os.listdir(src_dir):
    file_path = os.path.join(src_dir, filename)

    if os.path.isfile(file_path) and os.path.getsize(file_path) > 500000:
        shutil.move(file_path, dst_dir)
