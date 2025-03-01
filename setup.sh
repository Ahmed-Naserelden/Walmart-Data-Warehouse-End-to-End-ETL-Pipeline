#!/bin/bash

# Install dependencies
sudo apt-get update
sudo apt-get install -y python3-pip python3-venv

# Create virtual environment if not exists
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
    echo "Virtual environment created."
fi

# Activate virtual environment
source .venv/bin/activate

# Upgrade pip and install dependencies
pip install --upgrade pip
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    echo "requirements.txt not found. Skipping dependency installation."
fi

# Deactivate virtual environment
deactivate


# Prompt user for environment variable values
read -p "Enter MySQL username: " MYSQL_USERNAME
read -sp "Enter MySQL password: " MYSQL_PASSWORD
echo
read -p "Enter MySQL database: " MYSQL_DATABASE
read -p "Enter MySQL host: " MYSQL_HOST

read -p "Enter PostgreSQL username: " PG_USERNAME
read -sp "Enter PostgreSQL password: " PG_PASSWORD
echo
read -p "Enter PostgreSQL database: " PG_DATABASE
read -p "Enter PostgreSQL host: " PG_HOST

# Save credentials in a .env file
cat <<EOT > .env
MYSQL_USERNAME="$MYSQL_USERNAME"
MYSQL_PASSWORD="$MYSQL_PASSWORD"
MYSQL_DATABASE="$MYSQL_DATABASE"
MYSQL_HOST="$MYSQL_HOST"

PG_USERNAME="$PG_USERNAME"
PG_PASSWORD="$PG_PASSWORD"
PG_DATABASE="$PG_DATABASE"
PG_HOST="$PG_HOST"
EOT
echo "Environment variables saved in .env file."

# Provide instructions for loading the environment variables
echo "Run 'source .env' to apply environment variables in your session."
