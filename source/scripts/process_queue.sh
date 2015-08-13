#!/bin/bash

# Print usage.
usage() {
    cat << EOF

NAME
     process_queue -- processes a Blu Pen queue

SYNOPSIS
     process_queue author|collection|source

DESCRIPTION
     Processes author, collection, or source request files from
     'queue'. All relevant actions are logged. Note that the
     CONTENT_HOME environment variable must be exported.

OPTIONS
     None

EOF
}

# Parse command line options
while getopts ":h" opt; do
    case $opt in
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

# Check requests home directory
REQUESTS_HOME="$CONTENT_HOME/requests"
if [ ! -d "$REQUESTS_HOME/$REQUEST_TYPE" ]; then
    echo "$REQUESTS_HOME/$REQUEST_TYPE is not a directory"
    exit 1
fi
if [ ! -d "$REQUESTS_HOME/$REQUEST_TYPE/queue" ]; then
    echo "$REQUESTS_HOME/$REQUEST_TYPE/queue is not a directory"
    exit 1
fi

# Check scripts home
SCRIPTS_HOME="$CONTENT_HOME/source/scripts"
if [ ! -d "$SCRIPTS_HOME" ]; then
    echo "$SCRIPTS_HOME is not a directory"
    exit 1
fi

# Work in the scripts home directory
pushd $SCRIPTS_HOME

# Process request queue
cmd="./fill_queue.sh $REQUEST_TYPE"
echo `date "+%Y-%m-%d-%H:%M:%S"`": $cmd"; $cmd
cmd="crontab empty_${REQUEST_TYPE}_queue.cron"
echo `date "+%Y-%m-%d-%H:%M:%S"`": $cmd"; $cmd

# Notify admin
msg=`date "+%Y-%m-%d-%H:%M:%S"`": Scheduled $REQUEST_TYPE process"
cat "empty_${REQUEST_TYPE}_queue.cron" | mail -s "$msg" raymond@blue-peninsula.com

# Minimize side effects
pushd $SCRIPTS_HOME
