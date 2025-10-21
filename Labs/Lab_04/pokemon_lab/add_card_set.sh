#!/bin/bash

read -rp "TCG Card Set ID): " SET_ID

if [ -z "$SET_ID" ]; then
    echo "Error: Set ID cannot be empty." >&2
    exit 1
fi


echo "Fetching cards for set '$SET_ID' from the Pokemon TCG API..."

curl -s "https://api.pokemontcg.io/v2/cards?q=set.id:$SET_ID" -o "card_set_lookup/$SET_ID.json"
