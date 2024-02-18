#!/usr/bin/env bash
MINER_CONFIG="$MINER_DIR/$MINER_NAME/sd_bridge.conf"
mkfile_from_symlink $MINER_CONFIG


CONF="-log=false "
CONF=$CUSTOM_USER_CONFIG

if [ -z "$var" ]
then
        CONF+=" -sedra=$CUSTOM_URL"
fi

echo -e "$CONF" > $MINER_CONFIG