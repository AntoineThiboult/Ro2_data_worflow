# -*- coding: utf-8 -*-
"""
Created on Sat Dec 30 17:47:54 2023

@author: ANTHI182
"""
import os
import yaml

def yaml_file(path, file):
    """
    Load a YAML file

    Parameters
    ----------
    path : String
        Path to the file
    file : file name
        file name with or without extention

    Returns
    -------
    data : Dictionary
    """

    if '.' not in file:
        file = file + '.yml'

    data = yaml.safe_load(
        open(os.path.join(path,file)))

    return data