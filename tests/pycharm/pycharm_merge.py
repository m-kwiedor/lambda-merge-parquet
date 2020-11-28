import os
import pyarrow.parquet as pq

def combine_parquet_files(input_folder, target_path):
    print('Input Folder: ' + input_folder)
    print('Target Path: ' + target_path)
    try:
        files = []
        for file_name in os.listdir(input_folder):
            print('File ' + file_name)
            files.append(pq.read_table(os.path.join(input_folder, file_name)))
        with pq.ParquetWriter(target_path,
                files[0].schema,
                version='2.0',
                compression='gzip',
                use_dictionary=True,
                data_page_size=2097152, #2MB
                write_statistics=True) as writer:
            for f in files:
                writer.write_table(f)
    except Exception as e:
        print(e)
        
if __name__ == '__main__':
    combine_parquet_files('data', 'combined.parquet')
