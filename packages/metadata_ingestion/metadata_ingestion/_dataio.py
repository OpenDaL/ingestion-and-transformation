# -*- coding: utf-8 -*-
"""
DATA IO SUB-MODULE

Functions to handle local filesystem data IO
"""
import json
import re
import datetime
from pathlib import Path
import yaml


def list_files(in_folder, filetype):
    """
    Generate a list of all files of a specific filetype in a directory.

    Input:
        in_folder --- The location of the folder that should be searched
        (string)

        filetype --- The type of files to list (string; e.g. 'json')

    Returns:
        list of  file locations --- returns a list of all file locations with
        the filetype in the directory
    """
    file_ext = '.' + filetype
    return [f for f in Path(in_folder).iterdir() if
            f.is_file() and f.suffix == file_ext]


def remove_if_exists(fileloc):
    """
    Removes the file specified, if it exists

    Input:
        fileloc --- The location of the file to remove (string)
    """
    file_path = Path(fileloc)
    if file_path.is_file():  # Should not remove folders
        file_path.unlink()


def rename_if_exists(old_fileloc, new_fileloc):
    """
    Removes the file specified, if it exists

    Input:
        fileloc --- The location of the file to remove (string)
    """
    old = Path(old_fileloc)
    if old.is_file():
        old.rename(Path(new_fileloc))


def loadjson(in_filepath):
    """
    Load data from a JSON file

    Input:
        in_filepath --- str/pathlib.Path: The path to the json file

    Returns:
        dict/list/str: The data from the json-file
    """
    with open(in_filepath, 'r', encoding='utf8') as jsonfile:
        data = json.load(jsonfile)

    return data


def loadyaml(in_filepath):
    """
    Load data from a YAML file

    Input:
        in_filepath --- str/pathlib.Path: The path to the yaml file

    Returns:
        dict/list/str: The data from the yaml-file
    """
    with open(in_filepath, 'r', encoding='utf8') as yamlfile:
        data = yaml.safe_load(yamlfile)

    return data


def loadjsonlines(in_filepath):
    """
    Load data from a JSON lines file

    Input:
        in_filepath --- str/pathlib.Path: The path to the json-lines file

    Returns:
        data --- A list with the lines of data in the JSON lines file
    """
    data = []
    with open(in_filepath, 'r', encoding='utf8') as jsonlinesfile:
        for line in jsonlinesfile:
            data.append(json.loads(line))

    return data


def savejson(data, out_filepath):
    """
    Saves an object to a json file

    Input:
        data --- The object containing the data

        out_filepath --- str/pathlib.Path: The path to store the json file

        encoding = None --- The encoding to use when saving the json file

    Output:
        JSON file --- The object 'data' is stored to the location specified by
        out_fileloc
    """
    with open(out_filepath, 'w', encoding='utf8') as outfile:
        json.dump(data, outfile)


def savejsonlines(data, out_filepath, mode='w'):
    """
    Saves data to a json lines file with utf8 encoding by default

    Input:
        data --- A list with resource descriptions

        out_filepath --- str/pathlib.Path: The path to store the json-lines
        file

        mode  = 'w' --- The mode to use. If 'w' a new file is written with the
        data, if 'a' is used, data is appended to an existing file.

    Output:
        json lines file --- The json lines file with on each line a new
        resource description
    """
    with open(out_filepath, mode, encoding='utf8') as jsonlines_file:
        for item in data:
            jsonlines_file.write(json.dumps(item, ensure_ascii=False) + '\n')


def save_queue_on_exceedance(list_, out_filepath, on_length):
    """
    Saves data in a list to a json-lines file, in case the size of the list
    equals or exceeds a specified length

    Arguments:
        list_ --- list: The list data to be saved

        out_filepath --- str/pathlib.Path: The path to store the json-lines
        file

        on_length --- int: The length that should be equaled/exceeded before
        saving the data

    Returns:
        list --- The input list, if on_length is not exceeded, or and empty
        list in case it is, and the input list is written to a file.
    """
    if len(list_) >= on_length:
        savejsonlines(list_, out_filepath, mode='a')
        return []
    else:
        return list_


def URL2fileloc(folder, URL, filetype):
    """
    Creates a filename from a URL, by removing http(s)://, traling / and
    replacing / by _. This is appended to the folder location, to yield a file
    location. Filetype is appended to the end.

    Input:
        folder --- str/pathlib.Path: The folder path

        URL --- The URL to use for the filename (string)

        filetype --- The filetype to append to the end
    Returns:
        file location --- The complete file location of the file
    """
    filename = re.sub(r'(https?:\/\/)|(\/$)', '', URL)
    filename = filename.replace('/', '_') + '.' + filetype
    filepath = Path(folder, filename)

    return filepath


def current_date_fileloc(folder, name, extention):
    """
    Creates a file name containing the current date: (name)_(YYYYMMDD).(ext),
    and puts the folder in front of it

    Input:
        folder --- str/pathlib.Path: The folder path

        name --- The name of the file, beofre the underscore

        extention --- The file extention (None for no extention)

    Returns:
        filename --- The generated filename as a string
    """
    now = datetime.datetime.now()
    filename = '{name}_{date}.{ext}'.format(name=name,
                                            date=now.strftime('%Y%m%d'),
                                            ext=extention)

    filepath = Path(folder, filename)

    return filepath


def savejson_confirm_overwrite(data, out_filepath):
    """
    Saves an object to a json file, asks to overwrite data

    Input:
        data --- The object containing the data

        out_filepath --- str/pathlib.Path: The path to store the json-lines
        file

        encoding = None --- The encoding to use when saving the json file
        (string).

    Output:
        JSON file --- The object 'data' is stored to the location specified by
        out_fileloc, or entered by the user
    """
    out_filepath = Path(out_filepath)
    if out_filepath.is_file():
        folder = out_filepath.parent
        filename = out_filepath.name
        choice = input("The file '%s' already exists. Overwrite? \
            (Y,N or alternative filename)" % filename)
        if choice.lower() == 'y':
            savejson(data, out_filepath)
        elif choice.lower() == 'n':
            print('File not saved!')
        else:
            savejson(data, Path(folder, choice))
    else:
        savejson(data, out_filepath)


def iterate_jsonlines(in_filepath):
    """
    Iterator that returns objects for each line in a json-lines file

    Input:
        in_filepath --- str/pathlib.Path: The path to the json-lines file

    Yields:
        Objects --- The data from a single jsonlines file line as an object
    """
    with open(in_filepath, 'r', encoding='utf8') as jsonlinesfile:
        for line in jsonlinesfile:
            yield json.loads(line)
