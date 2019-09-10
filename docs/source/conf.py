import os
import subprocess

if not os.environ.get('READTHEDOCS', None):
    root = os.path.dirname(os.path.realpath(__file__))
    bson_format_nb = os.path.join(root, 'bson_format.ipynb')
    subprocess.check_call(['jupyter', 'nbconvert', '--to', 'rst', bson_format_nb])

project = 'DataFrame'
copyright = '2019, Fairtide Pte. Ltd.'
author = 'Fairtide Pte. Ltd.'
html_theme = 'sphinx_rtd_theme'
