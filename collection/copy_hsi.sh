#!/bin/bash
#| xargs -I {} dirname {} 
hsi -q 'ls -RP' megarun_one_netcdf/ 2> temp.txt
hpss_file_sorter.script temp.txt | uniq > retrieval_list.txt
cat retrieval_list.txt | awk '{print "cget",$1}' > final_retrieval_list.txt
hsi "in final_retrieval_list.txt"
cat temp.txt | grep DIRECTORY | sed  's:^DIRECTORY\s\{0,\}/home/k/karel/megarun_one_netcdf/\(.*\):megarun_one_netcdf/\1:' | sort | uniq |  xargs -I {} mkdir -p {}
cat final_retrieval_list.txt | sed 's:cget /home/k/karel/::' | sed 's:.*/::' | paste - final_retrieval_list.txt | sed 's:cget /home/k/karel/::' | cut -d '/' -f -4 | xargs -tn 2 mv
