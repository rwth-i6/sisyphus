# sisyphus
A Workflow Manager in Python

# INSTALLATION:
Sisyphus requires a Python 3.5 installation with the following additional libraries:
   - sudo pip3 install psutil
   - sudo pip3 install ipython

  Optional if web interface should be used:
   - sudo pip3 install flask

  Optional to compile documentation:
   - sudo pip3 install Sphinx

  Optional if virtual file system should be used:
   - sudo pip3 install fusepy
   - sudo addgroup $USER fuse  # depending on your system
   
# Documentation
Can be created using make in the doc directory.
A prepared html version can be found here: doc/_build/html/index.html

# Example 
A short toy workflow example is given in the example directory. 

To run sisyphus on the example workflow change into the `/example` directory and run `../sis manager`

A large realistic workflow will soon be added.

# LICENSE:
  All Source Code in this Project is subject to the terms of the Mozilla
  Public License, v. 2.0. If a copy of the MPL was not distributed with
  this file, You can obtain one at http://mozilla.org/MPL/2.0/.
