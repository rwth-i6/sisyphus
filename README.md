# Sisyphus

A workflow manager in Python.


# Installation

## Requirements

Sisyphus requires a Python >=3.5 installation with the following additional libraries:

    pip3 install psutil
    pip3 install ipython

  Optional if web interface should be used:

    pip3 install flask

  Optional to compile documentation:

    pip3 install Sphinx

  Optional if virtual file system should be used:

    pip3 install fusepy
    sudo addgroup $USER fuse  # depending on your system

## Setup

* Add `sis` to the `PATH` env, or symlink it, or call it directly.
* The current directory (`pwd`), when you run `sis`, should have a file `settings.py` (see `example` dir).
* Create a directory `work` in the current dir.
  All data created while running the jobs will be stored there.
* Create a directory `output` in the current dir.
  All the registered output will end up here.
* Create a directory `alias` in the current dir.
* Run `sis --config some_config.py m`.


# Documentation
Can be found here: [sisyphus-workflow-manager.readthedocs.io](https://sisyphus-workflow-manager.readthedocs.io/).


# Example 

A short toy workflow example is given in the example directory. 

To run sisyphus on the example workflow change into the `/example` directory and run `../sis manager`

A large realistic workflow will soon be added.


# License

All Source Code in this Project is subject to the terms of the Mozilla
Public License, v. 2.0. If a copy of the MPL was not distributed with
this file, You can obtain one at http://mozilla.org/MPL/2.0/.

https://sisyphus-workflow-manager.readthedocs.io/en/latest/
