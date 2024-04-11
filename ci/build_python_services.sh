#!/bin/bash

# define all target directories
declare -a dirs=(
    "bec_client"
    "bec_lib"
    "bec_server"
)

mkdir dist

# loop over all directories and run the build command
for dir in "${dirs[@]}"
do
    echo "Building $dir"
    cd $dir
    python setup.py sdist bdist_wheel
    cp dist/* ../dist
    rm -r ./dist
    cd ..
done