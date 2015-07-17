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
     actions are logged. Note that the REQUESTS_HOME environment
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
    request_type="$1"
fi
if [ $# -gt 1 ]; then
    echo "Additional command line arguments ignored"
fi

# Check command line arguments.
if ! [[ "$request_type" =~ ^(author|collection|source)$ ]]; then
    echo "Request type must be 'author', 'collection', or 'source'"
    exit 1
fi
if [ -z "$REQUESTS_HOME" ]; then
    echo "REQUESTS_HOME environment variable is not exported"
    exit 1
fi
if [ ! -d "$REQUESTS_HOME/$request_type" ]; then
    echo "$REQUESTS_HOME/$request_type is not a directory"
    exit 1
fi
if [ $USE_DID_POP == 0 ]; then
    if [ ! -d "$REQUESTS_HOME/$request_type/do-push" ]; then
        echo "$REQUESTS_HOME/$request_type/do-push is not a directory"
        exit 1
    fi
else
    if [ ! -d "$REQUESTS_HOME/$request_type/did-pop" ]; then
        echo "$REQUESTS_HOME/$request_type/did-pop is not a directory"
        exit 1
    fi
fi    
if [ ! -d "$REQUESTS_HOME/$request_type/queue" ]; then
    echo "$REQUESTS_HOME/$request_type/queue is not a directory"
    exit 1
fi

# Copy, but do not overwrite, JSON files from 'do-push', or optionally
# 'did-pop', to 'queue'
pushd $REQUESTS_HOME &> /dev/null
if [ $USE_DID_POP == 0 ]; then
    files=`ls $request_type/do-push/*.json`
else
    files=`ls $request_type/did-pop/*.json`
fi    
for file in $files; do
    cmd="cp -n $file $request_type/queue"
    echo `date "+%Y-%m-%d-%H:%M:%S"`": $cmd"
    $cmd
done
popd &> /dev/null
