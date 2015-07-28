#!/bin/bash

# Print usage.
usage() {
    cat << EOF

NAME
     fill_queue -- fills a Blu Pen queue

SYNOPSIS
     fill_queue author|collection|source

DESCRIPTION
     Copies author, collection, or source request files from 'do-push'
     to 'queue', without overwriting existing files. All relevant
     actions are logged. Note that the CONTENT_HOME environment
     variable must be exported.

OPTIONS
     -o     Copy files from 'did-pop' rather than 'do-push';

EOF
}

# Parse command line options
USE_DID_POP=0
while getopts ":oh" opt; do
    case $opt in
        o)
            USE_DID_POP=1
            ;;
	h)
	    usage
	    exit 0
	    ;;
	\?)
	    echo "Invalid option: -$OPTARG" >&2
	    usage
	    exit 1
	    ;;
	\:)
	    echo "Option -$OPTARG requires an argument." >&2
	    usage
	    exit 1
	    ;;
    esac
done

# Parse command line arguments
shift `expr $OPTIND - 1`
if [ $# -eq 0 ]; then
    echo "A request type must be provided"
    exit 1
else
    REQUEST_TYPE="$1"
fi
if [ $# -gt 1 ]; then
    echo "Additional command line arguments ignored"
fi

# Check command line arguments.
if ! [[ "$REQUEST_TYPE" =~ ^(author|collection|source)$ ]]; then
    echo "Request type must be 'author', 'collection', or 'source'"
    exit 1
fi

# Check environment variables.
if [ -z "$CONTENT_HOME" ]; then
    echo "CONTENT_HOME environment variable is not exported"
    exit 1
fi

# Check requests home
REQUESTS_HOME="$CONTENT_HOME/requests"
if [ ! -d "$REQUESTS_HOME/$REQUEST_TYPE" ]; then
    echo "$REQUESTS_HOME/$REQUEST_TYPE is not a directory"
    exit 1
fi
if [ $USE_DID_POP == 0 ]; then
    if [ ! -d "$REQUESTS_HOME/$REQUEST_TYPE/do-push" ]; then
        echo "$REQUESTS_HOME/$REQUEST_TYPE/do-push is not a directory"
        exit 1
    fi
else
    if [ ! -d "$REQUESTS_HOME/$REQUEST_TYPE/did-pop" ]; then
        echo "$REQUESTS_HOME/$REQUEST_TYPE/did-pop is not a directory"
        exit 1
    fi
fi    
if [ ! -d "$REQUESTS_HOME/$REQUEST_TYPE/queue" ]; then
    echo "$REQUESTS_HOME/$REQUEST_TYPE/queue is not a directory"
    exit 1
fi

# Copy, but do not overwrite, JSON files from 'do-push', or optionally
# 'did-pop', to 'queue'
cwd=`pwd`
pushd "$REQUESTS_HOME"
msg=`date "+%Y-%m-%d-%H:%M:%S"`": entering fill_queue.sh"
echo "$msg" > "$cwd/fill_queue.log"
if [ $USE_DID_POP == 0 ]; then
    files=`ls $REQUEST_TYPE/do-push/*.json`
else
    files=`ls $REQUEST_TYPE/did-pop/*.json`
fi    
for file in $files; do
    cmd="cp -n $file $REQUEST_TYPE/queue"
    msg=`date "+%Y-%m-%d-%H:%M:%S"`": $cmd"
    echo "$msg" >> "$cwd/fill_queue.log"
    $cmd
done
msg=`date "+%Y-%m-%d-%H:%M:%S"`": exiting fill_queue.sh"
echo "$msg" >> "$cwd/fill_queue.log"
popd

# Notify admin
msg=`date "+%Y-%m-%d-%H:%M:%S"`": Filled $REQUEST_TYPE queue"
cat fill_queue.log | mail -s "$msg" raymond.leclair@blue-peninsula.com
