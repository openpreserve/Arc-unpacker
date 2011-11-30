#!/bin/sh

#
# Usage: ./unpack.sh file.arc extractDir

HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"
echo "Script home dir $HOME"

echo "Using this JDK_HOME=$JDK_HOME"

$JDK_HOME/bin/java -cp "$HOME/lib/*" dk.statsbiblioteket.scape.arcunpacker.Archive "$1" "$2"
