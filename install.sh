#!/bin/bash

PYTHON_PATH=`which python2.6`

if [ $PYTHON_PATH != "" ]
    then
        easy_install virtualenv
        virtualenv -p $PYTHON_PATH --distribute --prompt=[$1] .env
        source .env/bin/activate

        if [[ -f 'requirements.txt' ]]; then
            pip install -r requirements.txt
        fi
    else
        echo "Can't find Python executable."
fi
