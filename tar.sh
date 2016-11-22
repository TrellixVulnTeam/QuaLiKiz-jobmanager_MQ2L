OLDIFS=$IFS
IFS=$'\n'
ENTRIES=$(cat)
for entry in $ENTRIES
do
  IFS=$','

  ele=($entry)
  strat=${ele[0]}
  folder=${ele[1]}
  if [[ "$strat" == targz ]]
  then
    echo moving to $folder
    cd $folder
    for f in smag*
    do
      if [[ "$f" != *.tar.gz ]]
      then
        echo tarring $f
        tar -czf $f.tar.gz $f && rm -r $f
      fi
    done
  fi
done
echo done
