#!/bin/bash
for entry in *.json
do
echo "$entry"
echo  "," >> "$entry"

done
