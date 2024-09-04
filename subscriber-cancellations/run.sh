#!/bin/bash

# Directories
DEV_DIR="dev"
PROD_DIR="prod"
CHANGELOG_FILE="$DEV_DIR/changelog.log"
ERROR_LOG_FILE="$DEV_DIR/error.log"
TEST_SCRIPT="tests.py"
PYTHON_SCRIPT="main.py"

# Check the latest version in the changelog
if [ ! -f "$CHANGELOG_FILE" ]; then
    echo "Changelog file does not exist. Exiting..."
    exit 1
fi

# Run the tests
echo "Running tests..."
python "$TEST_SCRIPT"

# Run the Python script
echo "Running the Python script..."
python "$PYTHON_SCRIPT"

# Check if the script ran successfully by checking if there are new entries in the log
if grep -q "Database updated successfully" "$CHANGELOG_FILE"; then
    echo "Update successful, moving files to production..."

    # Move files from /dev to /prod
    mv "$DEV_DIR/cademycode_updated.db" "$PROD_DIR/"
    mv "$DEV_DIR/cademycode_updated.csv" "$PROD_DIR/"

    echo "Files moved to production."

else
    echo "Update failed. Check the error log."
    grep "Error" "$ERROR_LOG_FILE"
    exit 1
fi

echo "Process completed."
