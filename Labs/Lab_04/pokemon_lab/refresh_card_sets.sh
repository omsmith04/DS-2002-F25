#!/bin/bash

echo "Refreshing all card sets in card_set_lookup/..."

for FILE in card_set_lookup/*.json; do
    SET_ID=$(basename "$FILE" .json)
    echo "Updating set '$SET_ID'..."
    URL="https://api.pokemontcg.io/v2/cards?q=set.id:%22${SET_ID}%22&pageSize=250"
    if curl -fSLs "$URL" -o "$FILE"; then
        echo "Wrote data to $FILE"
    else
        echo "Error: failed to update set '$SET_ID'." >&2
    fi
done

echo "All card sets have been refreshed."
