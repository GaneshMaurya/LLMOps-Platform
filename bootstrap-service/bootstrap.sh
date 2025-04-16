#!/bin/bash

# Define Variables
ENV_FILE="/exports/applications/.env"
set -a
source "$ENV_FILE"
set +a

# Use environment variables from .env
NFS_SERVER=${nfs_server_ip}  # Fallback if not defined
MOUNT_ROOT=${mount_root:-"/exports"}
SERVICES_DIR=${services_dir:-"$MOUNT_ROOT/applications"}

# Log file
LOG_FILE="/var/log/bootstrap.log"
echo "Bootstrap started at $(date)" | tee -a $LOG_FILE

# Function to run boot_nfs_client.sh on a remote machine
run_boot_nfs_client() {
    machine_ip=$1
    machine_user=${ssh_users[$machine_ip]}
    machine_pass=${ssh_passwords[$machine_ip]}

    echo "Transferring boot_nfs_client.sh to $machine_ip..." | tee -a $LOG_FILE
    # Transfer the script to the remote machine
    sshpass -p "$machine_pass" scp -o StrictHostKeyChecking=no ./boot_nfs_client.sh $machine_user@$machine_ip:/tmp/boot_nfs_client.sh
    
    # Execute the script remotely
    echo "Running boot_nfs_client.sh on $machine_ip..." | tee -a $LOG_FILE
    sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "bash /tmp/boot_nfs_client.sh"
    
    if [ $? -eq 0 ]; then
        echo "Successfully ran boot_nfs_client.sh on $machine_ip." | tee -a $LOG_FILE
    else
        echo "Failed to run boot_nfs_client.sh on $machine_ip." | tee -a $LOG_FILE
        exit 1
    fi
}

# Install NFS Client
echo "Installing nfs-common..." | tee -a $LOG_FILE
sudo apt update && sudo apt -y install nfs-common

# Define service-machine mappings using values from .env
declare -A services
services["service_registry"]="${service_registry_ip}"
services["deployer_service"]="${deployer_registry_ip}"
services["model_registry"]="${model_registry_ip}"
services["frontend_service"]="${frontend_service_ip}"

# Define start commands (Ensure `start.sh` exists in each service directory)
declare -A start_commands
start_commands["service_registry"]="nohup $SERVICES_DIR/service-registry/start.sh &"
start_commands["deployer_service"]="nohup $SERVICES_DIR/deployer-service/start.sh &"
start_commands["model_registry"]="nohup $SERVICES_DIR/model-registry/start.sh &"
start_commands["frontend_service"]="nohup $SERVICES_DIR/frontend-service/start.sh &"

# Define SSH credentials for each machine using .env values
declare -A ssh_users
declare -A ssh_passwords

ssh_users["${service_registry_ip}"]="${service_registry_user}"
ssh_passwords["${service_registry_ip}"]="${service_registry_pass}"

ssh_users["${deployer_registry_ip}"]="${deployer_service_user}"
ssh_passwords["${deployer_registry_ip}"]="${deployer_service_pass}"

ssh_users["${model_registry_ip}"]="${model_registry_user}"
ssh_passwords["${model_registry_ip}"]="${model_registry_pass}"

ssh_users["${frontend_service_ip}"]="${frontend_service_user}"
ssh_passwords["${frontend_service_ip}"]="${frontend_service_pass}"

# Function to check if NFS is mounted on a remote machine
check_nfs_mount() {
    machine_ip=$1
    machine_user=${ssh_users[$machine_ip]}
    machine_pass=${ssh_passwords[$machine_ip]}

    echo "Checking NFS mount on $machine_ip..." | tee -a $LOG_FILE
    sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "mount | grep '$MOUNT_ROOT' > /dev/null"
    
    if [ $? -ne 0 ]; then
        echo "Error: NFS not mounted on $machine_ip. Exiting..." | tee -a $LOG_FILE
        exit 1
    fi
}

# Function to start a service on a remote machine
start_service() {
    service_name=$1
    machine_ip=$2
    command=$3
    machine_user=${ssh_users[$machine_ip]}
    machine_pass=${ssh_passwords[$machine_ip]}

    echo "Starting $service_name on $machine_ip..." | tee -a $LOG_FILE
    sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "bash -c '$command'"
    
    if [ $? -eq 0 ]; then
        echo "$service_name started successfully on $machine_ip." | tee -a $LOG_FILE
        return 0
    else
        echo "Failed to start $service_name on $machine_ip." | tee -a $LOG_FILE
        return 1
    fi
}

# Function to check if a service is ready
check_service_ready() {
    service_name=$1
    machine_ip=$2
    machine_user=${ssh_users[$machine_ip]}
    machine_pass=${ssh_passwords[$machine_ip]}
    
    # Maximum wait time in seconds
    max_wait=120
    wait_time=0
    interval=5
    
    echo "Waiting for $service_name to be ready..." | tee -a $LOG_FILE
    
    while [ $wait_time -lt $max_wait ]; do
        # This is a generic check - modify based on your service readiness check method
        # For example, you might check a specific port or endpoint
        case $service_name in
            "service_registry")
                sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "pgrep -f service-registry" > /dev/null
                ;;
            "deployer_service")
                sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "pgrep -f deployer-service" > /dev/null
                ;;
            "model_registry")
                sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "pgrep -f model-registry" > /dev/null
                ;;
            "frontend_service")
                sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "pgrep -f frontend-service" > /dev/null
                ;;
        esac
        
        if [ $? -eq 0 ]; then
            echo "$service_name is ready." | tee -a $LOG_FILE
            return 0
        fi
        
        echo "Waiting for $service_name... ($wait_time/$max_wait seconds)" | tee -a $LOG_FILE
        sleep $interval
        wait_time=$((wait_time + interval))
    done
    
    echo "Timeout waiting for $service_name to be ready." | tee -a $LOG_FILE
    return 1
}

# Define the sequence of services
service_sequence=("service_registry" "deployer_service" "model_registry" "frontend_service")

# Start services in sequence
for service in "${service_sequence[@]}"; do
    machine_ip="${services[$service]}"
    
    # Run boot_nfs_client.sh on each machine
    # Uncomment if needed
    # run_boot_nfs_client "$machine_ip"
    
    # Ensure NFS is mounted
    # Uncomment if needed
    # check_nfs_mount "$machine_ip"
    
    # Start the service
    start_service "$service" "$machine_ip" "${start_commands[$service]}"
    
    # Wait for the service to be ready before starting the next one
    if ! check_service_ready "$service" "$machine_ip"; then
        echo "ERROR: $service failed to start properly. Stopping deployment." | tee -a $LOG_FILE
        exit 1
    fi
    
    echo "$service is running successfully. Proceeding to next service." | tee -a $LOG_FILE
done

echo "All services started successfully in sequence." | tee -a $LOG_FILE
echo "Bootstrap completed at $(date)" | tee -a $LOG_FILE