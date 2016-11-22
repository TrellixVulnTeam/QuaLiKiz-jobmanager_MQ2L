for f in smag*
do
  if [[ "$f" != *.tar.gz ]]
  then
    echo tarring $f
    tar -czf $f.tar.gz $f && rm -r $f
  fi
done
