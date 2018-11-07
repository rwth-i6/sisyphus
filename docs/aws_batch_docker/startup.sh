#!/bin/bash
NFS_SERVER=Add the ip to your nfs server here
mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport NFS_SERVER:/ /efs
bash -c "$*"
