import os
import subprocess

if not os.environ.get('READTHEDOCS', None):
    root = os.path.dirname(os.path.realpath(__file__))
    bson_format_nb = os.path.join(root, 'bson_format.ipynb')
    bson_format_rst = os.path.join(root, 'bson_format.rst')

    subprocess.check_call(
        ['jupyter', 'nbconvert', '--to', 'rst', bson_format_nb])

    with open(bson_format_rst) as rst:
        txt = rst.read().replace('ipython3', 'python3')

    with open(bson_format_rst, 'w') as rst:
        rst.write(txt)

project = 'DataFrame'
master_doc = 'index'
copyright = '2019, Fairtide Pte. Ltd.'
author = 'Fairtide Pte. Ltd.'
html_theme = 'sphinx_rtd_theme'
