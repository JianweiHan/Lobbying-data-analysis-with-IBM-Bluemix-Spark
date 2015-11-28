#!/bin/bash
for entry in *.xml
do
  echo "$entry"

  filename=$(basename "$entry")
  echo "$filename"
  extension="${filename##*.}"
  filename="${filename%.*}"
  echo "$filename.json"
  
 xml2json -t xml2json -o "$filename.json" "$entry"
done

