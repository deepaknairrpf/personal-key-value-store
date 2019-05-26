import os


def utf8len(s):
    return len(s.encode('utf-8'))


def create_dir(dir_path):
    if not os.path.exists(dir_path):
        print("Creating storage directory: " + dir_path)
        os.makedirs(dir_path)
