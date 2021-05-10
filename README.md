# Overview

This repository contains code to support the download of images and metadata
from two free and publically available datasets.

## Allen Mouse Brain Atlas
The [Allen Mouse Brain Atlas](https://registry.opendata.aws/allen-mouse-brain-atlas/)
is now hosted on AWS.

The Jupyter notebook ``Visualizing_Images_from_Allen_Mouse_Brain_Atlas.ipynb``
demonstrates all of the helper functions necessary to download and view
images from the atlas. It also demonstrates functions for downloading
metadata and loading them as python data structures for easy searching,
so that users can identify the images that will be most helpful in their
research.

This module uses the open source [boto3](https://github.com/boto/boto3) API
to interface with AWS S3. In order to use boto3, you will need to have valid
AWS credentials stored in a csv file as discussed in at the top of the example
Jupyter notebook.

**Note:** If run without modification, the helper functions provided in this
module will download images into the ```tmp/``` directory in this repository.
The functions are generally smart enough to not download something twice,
however, they will not delete unused data. If you find disk space filling
up, that is a place to look.

## Ivy Glioblastoma Atlas Project

The Jupyter notebook ``Programmatic_access_of_Ivy_Glioblastoma_Atlas_Project_data.ipynb``
demonstrates how to programmatically search, download, and interact with
images from the [Ivy Glioblastoma Atlas Project](https://glioblastoma.alleninstitute.org/).

# Level of Support

This module is provided as a means to give the broader neuroscience community
access to the data generated by the Allen Institute. As such, we are very
interested in maintaining the code so that it remains useful. Please file
any bug reports through GitHub. We will consider pull requests, so long as
they do not conflict with internal Institute policies regarding software.

# Dependencies

* **Boto3** https://github.com/boto/boto3 (installable through PyPI)
* **pillow** https://github.com/python-pillow/Pillow (installable through conda or PyPI)
* **matplotlib** https://matplotlib.org/ (installable through conda or PyPI)
* **pandas** https://pandas.pydata.org/ (installable through conda or PyPI)
* **jupyter notebook** or **jupyterlab** https://jupyter.org/ (installable through conda or PyPI)
