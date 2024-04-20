#!/bin/bash

# Loop through the range of ports and kill the processes
for ((port=8001; port<=8050; port++))
do
    echo "Killing processes occupying port $port..."
    kill $(lsof -t -i:$port)
done

echo "All processes occupying specified ports have been killed."