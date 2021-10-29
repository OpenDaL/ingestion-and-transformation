# -*- coding: utf-8 -*-
"""
Module with functions related to data in/output on the local filesystem

Copyright (C) 2021  Tom Brouwer

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import json
from pathlib import Path
import yaml
from typing import Union, Iterator, Any


def list_files(in_folder: Union[Path, str], filetype: str) -> list[Path]:
    """
    Generates a list of all files of a specific filetype in a directory.

    Args:
        in_folder:
            The location of the folder that should be searched
        filetype:
            The type of files to list (e.g. 'json')

    Returns:
        list of all file locations with the filetype in the directory
    """
    file_ext = '.' + filetype
    return [f for f in Path(in_folder).iterdir() if
            f.is_file() and f.suffix == file_ext]


def rename_if_exists(
        old_fileloc: Union[Path, str], new_fileloc: Union[Path, str]
        ):
    """
    Removes the file specified, if it exists

    Args:
        fileloc: The location of the file to rename
    """
    old = Path(old_fileloc)
    if old.is_file():
        old.rename(Path(new_fileloc))


def loadjson(in_filepath: Union[Path, str]) -> Any:
    """
    Loads data from a JSON file

    Args:
        in_filepath: The path to the json file

    Return:
        The data from the json-file
    """
    with open(in_filepath, 'r', encoding='utf8') as jsonfile:
        data = json.load(jsonfile)

    return data


def loadyaml(in_filepath: Union[Path, str]) -> Any:
    """
    Loads data from a YAML file

    Args:
        in_filepath: The path to the yaml file

    Returns:
        The data from the yaml-file
    """
    with open(in_filepath, 'r', encoding='utf8') as yamlfile:
        data = yaml.safe_load(yamlfile)

    return data


def loadjsonlines(in_filepath: Union[Path, str]) -> list:
    """
    Loads data from a JSON lines file

    Args:
        in_filepath: The path to the json-lines file

    Return:
        A list with the lines of data in the JSON lines file
    """
    data = []
    with open(in_filepath, 'r', encoding='utf8') as jsonlinesfile:
        for line in jsonlinesfile:
            data.append(json.loads(line))

    return data


def savejson(data, out_filepath: Union[Path, str]):
    """
    Saves an object to a json file

    Args:
        data:
            The object containing the data
        out_filepath:
            The path to store the json file
    """
    with open(out_filepath, 'w', encoding='utf8') as outfile:
        json.dump(data, outfile)


def savejsonlines(data: list, out_filepath: Union[Path, str], mode: str = 'w'):
    """
    Saves data to a json lines file with utf8 encoding by default

    Args:
        data:
            A list with resource descriptions
        out_filepath:
            The path to store the json-lines file
        mode:
            The mode to use. If 'w' a new file is written with the
            data, if 'a' is used, data is appended to an existing file.
    """
    with open(out_filepath, mode, encoding='utf8') as jsonlines_file:
        for item in data:
            jsonlines_file.write(json.dumps(item, ensure_ascii=False) + '\n')


def iterate_jsonlines(in_filepath: Union[Path, str]) -> Iterator[Any]:
    """
    Iterator that returns objects for each line in a json-lines file

    Args:
        in_filepath: The path to the json-lines file

    Yields:
       The data from a single jsonlines file line
    """
    with open(in_filepath, 'r', encoding='utf8') as jsonlinesfile:
        for line in jsonlinesfile:
            yield json.loads(line)
