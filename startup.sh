#!/bin/sh

# Exit immediately if a command exits with a non-zero status
set -e

# Echo commands
set -x

# Navigate to app directory (just in case)
cd /app

# Run the command passed as arguments
exec "$@"
