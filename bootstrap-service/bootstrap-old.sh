#!/bin/bash

# Define Variables
ENV_FILE="/exports/applications/.env"
LOG_FILE="/exports/applications/bootstrap.log"
BOOT_NFS_CLIENT_SCRIPT="/exports/applications/bootstrap-service/boot_nfs_client.sh"

# Load environment variables from the .env file
set -a
source "$ENV_FILE"
set +a

echo "Bootstrap started at $(date)" | tee -a $LOG_FILE

# Define service-machine mappings using values from .env
declare -A services
services["service_registry"]="${service_registry_ip}"
services["life_cycle_manager"]="${life_cycle_manager_ip}"
services["model_registry"]="${model_registry_ip}"
services["agent_1"]="${agent_1_ip}"
services["agent_2"]="${agent_2_ip}"

# Define start commands (absolute paths to bootstrap scripts)
declare -A start_commands
start_commands["service_registry"]="/exports/applications/service-registry/bootstrap-service-registry.sh"
start_commands["life_cycle_manager"]="/exports/applications/controller-Service/bootstrap-controller.sh"
start_commands["model_registry"]="/exports/applications/model-registry/bootstrap-model-registry.sh"
start_commands["agent_1"]="/exports/applications/agent-Service/bootstrap-agent.sh"
start_commands["agent_2"]="/exports/applications/agent-Service/bootstrap-agent.sh"

# Ensure necessary permissions for scripts
chmod +x /exports/applications/service-registry/bootstrap-service-registry.sh
chmod +x /exports/applications/controller-Service/bootstrap-controller.sh
chmod +x /exports/applications/model-registry/bootstrap-model-registry.sh
chmod +x /exports/applications/agent-Service/bootstrap-agent.sh

# Define SSH credentials for each machine using .env values
declare -A ssh_users
declare -A ssh_passwords

ssh_users["${service_registry_ip}"]="${service_registry_user}"
ssh_passwords["${service_registry_ip}"]="${service_registry_pass}"

ssh_users["${life_cycle_manager_ip}"]="${life_cycle_manager_user}"
ssh_passwords["${life_cycle_manager_ip}"]="${life_cycle_manager_pass}"

ssh_users["${model_registry_ip}"]="${model_registry_user}"
ssh_passwords["${model_registry_ip}"]="${model_registry_pass}"

ssh_users["${agent_1_ip}"]="${agent_1_user}"
ssh_passwords["${agent_1_ip}"]="${agent_1_pass}"

ssh_users["${agent_2_ip}"]="${agent_2_user}"
ssh_passwords["${agent_2_ip}"]="${agent_2_pass}"

# Function to clean up running services on a remote machine
cleanup_service() {
    service_name=$1
    machine_ip=$2
    machine_user=${ssh_users[$machine_ip]}
    machine_pass=${ssh_passwords[$machine_ip]}

    echo "Checking if $service_name is running on $machine_ip..." | tee -a $LOG_FILE
    sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "pgrep -f ${start_commands[$service_name]}" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "$service_name is running on $machine_ip. Cleaning up..." | tee -a $LOG_FILE
        sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "pkill -f ${start_commands[$service_name]}"
        echo "$service_name cleaned up on $machine_ip." | tee -a $LOG_FILE
    else
        echo "$service_name is not running on $machine_ip." | tee -a $LOG_FILE
    fi
}

# Function to start a service on a remote machine
start_service() {
    service_name=$1
    machine_ip=$2
    command=$3
    machine_user=${ssh_users[$machine_ip]}
    machine_pass=${ssh_passwords[$machine_ip]}

    # Get the hostname of the remote machine
    hostname=$(sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "hostname")
    echo "Hostname of $service_name on $machine_ip: $hostname" | tee -a $LOG_FILE

    # Pass the hostname to the agent service and create a log file with the hostname
    echo "Starting $service_name on $machine_ip with hostname $hostname..." | tee -a $LOG_FILE
    sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "bash -c 'export AGENT_HOSTNAME=$hostname && export LOG_FILE=/exports/applications/agent-Service/logs/agent-$hostname.log && $command'"

    if [ $? -eq 0 ]; then
        echo "$service_name started successfully on $machine_ip with hostname $hostname." | tee -a $LOG_FILE
        return 0
    else
        echo "Failed to start $service_name on $machine_ip with hostname $hostname." | tee -a $LOG_FILE
        return 1
    fi
}

# Define the sequence of services
service_start_sequence=("service_registry" "life_cycle_manager" "model_registry" "agent_1" "agent_2")

# Run boot_nfs_client.sh only for agents
for service in "${service_sequence[@]}"; do
    machine_ip="${services[$service]}"
    machine_user=${ssh_users[$machine_ip]}
    machine_pass=${ssh_passwords[$machine_ip]}

    # cleanup_service "$service" "$machine_ip"

    echo "Copying boot_nfs_client.sh to home directory on $machine_ip..." | tee -a $LOG_FILE
    sshpass -p "$machine_pass" scp -o StrictHostKeyChecking=no "$BOOT_NFS_CLIENT_SCRIPT" $machine_user@$machine_ip:~/boot_nfs_client.sh

    echo "Ensuring boot_nfs_client.sh is executable on $machine_ip..." | tee -a $LOG_FILE
    sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "chmod +x ~/boot_nfs_client.sh"

    echo "Running boot_nfs_client.sh on $machine_ip..." | tee -a $LOG_FILE
    sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "echo $machine_pass | sudo -S bash -c '/home/$machine_user/boot_nfs_client.sh $nfs_server_ip'"

    if [ $? -eq 0 ]; then
        echo "boot_nfs_client.sh ran successfully on $machine_ip." | tee -a $LOG_FILE
    else
        echo "Failed to run boot_nfs_client.sh on $machine_ip." | tee -a $LOG_FILE
        exit 1
    fi
done

# Start services in sequence
for service in "${!start_commands[@]}"; do
    machine_ip="${services[$service]}"

    # cleanup_service "$service" "$machine_ip"
    start_service "$service" "$machine_ip" "${start_commands[$service]}"
done

echo "All services started successfully." | tee -a $LOG_FILE
echo "Bootstrap completed at $(date)" | tee -a $LOG_FILE