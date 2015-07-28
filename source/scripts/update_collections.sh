#!/bin/bash

# Print usage.
usage() {
    cat << EOF

NAME
     update_collections -- schedule update of Blu Pen collections

SYNOPSIS
     update_collections

DESCRIPTION
     Schedule update of Blu Pen collections using cron. All relevant
     actions are logged. Note that the CONTENT_HOME environment
     variable must be exported.

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
if [ $# -gt 0 ]; then
    echo "Additional command line arguments ignored"
fi

# Check environment variables.
if [ -z "$CONTENT_HOME" ]; then
    echo "CONTENT_HOME environment variable is not exported"
    exit 1
fi

# Check scripts directory
SOURCE_HOME="$CONTENT_HOME/source"
if [ ! -d "$SOURCE_HOME/scripts" ]; then
    echo "$SOURCE_HOME/scripts is not a directory"
    exit 1
fi

# Schedule update of collections
pushd "$SOURCE_HOME/scripts"
cmd="crontab update_collections.cron"
echo `date "+%Y-%m-%d-%H:%M:%S"`": $cmd"; $cmd
popd

# Notify admin
msg=`date "+%Y-%m-%d-%H:%M:%S"`": Scheduled collection update"
cat "update_collections.cron" | mail -s "$msg" raymond.leclair@blue-peninsula.com
