import os
import glob
import pandas as pd
import time


def concat_csv_pandas(all_csv_files):
    start = time.time()
    df_all_csv_files = pd.concat([pd.read_csv(f) for f in all_csv_files])
    df_all_csv_files.to_csv("output.csv", index=False, encoding='utf-8-sig')
    end = time.time()
    print('Pandas run time:', end - start)


def concat_csv(all_csv_files):
    start = time.time()
    cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    header = "%s\n" % ",".join(cols)
    with open('output1.csv', 'w') as out_file:
        out_file.write(header)
        for file in all_csv_files:
            with open(file) as in_file:
                for i, line in enumerate(in_file):
                    if i > 0:
                        out_file.write(line)
    end = time.time()
    print('Python run time:', end - start)


if __name__ == '__main__':
    path = '../data'
    extension = 'csv'
    os.chdir(path)

    all_csv_files = [file for file in glob.glob('*.{}'.format(extension), recursive=True)]
    print(len(all_csv_files))

    concat_csv(all_csv_files)
    
    concat_csv_pandas(all_csv_files)
