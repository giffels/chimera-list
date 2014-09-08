#!/bin/bash

#v2.0 using new chimera dump script

now=$(date +"%Y%m%d%H%M")
l_dest="/pg/chimera-dump/chimera_${now}"
r_dest="/scratch/cmssgm/chimera-dump/chimera_${now}"
r_host="cms-kit"

#create chimera dump
/root/bin/chimera-list/chimera-list.py -s /pnfs/gridka.de/cms -o ${l_dest}

#send file to remote host
scp ${l_dest} ${r_host}:${r_dest}

#set owner to that file
ssh ${r_host} chown cmssgm:cms ${r_dest}

exit 0
