#!/usr/bin/env bash

MAX=5000
COUNTER=0 


while [ $COUNTER -lt $MAX ]
do 
	curl -H "Content-Type: application/json" -X POST -d '{"Topic": "t1&", "Content": "some content", "Header": "a user header"}' http://localhost:1357/cmessage;
    echo ;
	let COUNTER+=1;
done



