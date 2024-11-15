echo "Starting the Web Server and Scheduler..."
nohup airflow webserver --port 8080 > airflow_webserver.log 2>&1 &
nohup airflow scheduler > airflow_scheduler.log 2>&1 &
echo "Web server and scheduler started."

echo "Access the Airflow Web UI by opening a web browser and navigating to http://localhost:8080"

# Trap Ctrl+C to stop the services and clean up
trap "deactivate; exit 0" INT
wait