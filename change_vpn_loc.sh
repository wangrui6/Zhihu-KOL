#!/bin/bash

# Define your ExpressVPN username and password
username="your_username"
password="your_password"

# Define the hardcoded zones to include in the selection list
hardcoded_zones=("sgju" "my" "th" "jpto" "cato", "ausy")

while true
do
    # Get the current zone
    current_zone=$(expressvpn status | grep "Current server location" | awk '{print $4}')
    
    # Get a list of available zones
    available_zones=$(expressvpn list all | awk '{print $1}')
    
    # Add the hardcoded zones to the selection list if they're not already included
    for hardcoded_zone in "${hardcoded_zones[@]}"
    do
        if [[ ! $available_zones =~ (^|[[:space:]])$hardcoded_zone($|[[:space:]]) ]]; then
            available_zones="$hardcoded_zone"$'\n'"$available_zones"
        fi
    done
    
    # Choose a new zone at random (excluding the current zone)
    new_zone=""
    while [[ $new_zone == "" || $new_zone == $current_zone ]]
    do
        new_zone=$(echo "$available_zones" | shuf -n 1)
    done
    
    # Connect to the new zone
    expressvpn disconnect
    echo $new_zone
    expressvpn connect $new_zone
    
    # Sleep for 30 minutes before changing the zone again
    sleep 1800
done