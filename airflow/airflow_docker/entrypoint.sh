#!/bin/bash
set -e

echo "🚨 Checking airflow.cfg:"
ls -l /opt/airflow/airflow.cfg && cat /opt/airflow/airflow.cfg | grep sql_alchemy_conn || echo " Not found"


echo "🚨 Environment variable:"
echo "$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"

echo "🚨 airflow config:"
airflow config get-value database sql_alchemy_conn || echo "⚠️ Airflow failed to read config!"



# Source environment variables (if airflow_env.sh adds anything extra)
source /opt/airflow/airflow_env.sh || echo "Warning: airflow_env.sh not found or failed"

# Debug: Print key environment variables to confirm they’re set
echo "DEBUG: AIRFLOW_HOME=$AIRFLOW_HOME"
echo "DEBUG: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
echo "DEBUG: AIRFLOW__CORE__EXECUTOR=$AIRFLOW__CORE__EXECUTOR"


# Initialize Airflow DB if needed
if [ ! -f /opt/airflow/airflow.db ]; then
    echo "📦 Initializing Airflow metadata DB "
    airflow db init
    airflow db upgrade
else
    echo "✅ SQLite database already exists: skipping db init"
fi


# Check if the admin user already exists
ADMIN_USER_EXISTS=$(airflow users list | grep "admin" | wc -l)

# Create the admin user if it does not exist
if [ "$ADMIN_USER_EXISTS" == "0" ]; then
    airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname Admin \
        --role Admin \
        --email admin@example.com
fi



# Start supervisord to run both webserver and scheduler
exec supervisord -c /etc/supervisor/conf.d/supervisord.conf
