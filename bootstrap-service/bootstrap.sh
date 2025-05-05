# #!/bin/bash

# # Define Variables
# ENV_FILE="/exports/applications/.env"
# LOG_FILE="/exports/applications/bootstrap-service/bootstrap.log"
# BOOT_NFS_CLIENT_SCRIPT="/exports/applications/bootstrap-service/boot_nfs_client.sh"

# # Load environment variables from the .env file
# set -a
# source "$ENV_FILE"
# set +a

# echo "Bootstrap started at $(date)" | tee -a $LOG_FILE
# chmod 777 $LOG_FILE

# # Define local IP (should match the IP of the bootstrap machine)
# local_ip="${bootstrap_machine_ip}"

# # Define service-machine mappings using values from .env
# declare -A services
# services["service_registry"]="${service_registry_ip}"
# services["life_cycle_manager"]="${life_cycle_manager_ip}"
# services["model_registry"]="${model_registry_ip}"
# services["agent_1"]="${agent_1_ip}"
# services["agent_2"]="${agent_2_ip}"
# services["frontend"]="${frontend_ip}"

# # Define start commands
# declare -A start_commands
# start_commands["service_registry"]="/exports/applications/service-registry/bootstrap-service-registry.sh"
# start_commands["life_cycle_manager"]="/exports/applications/controller-Service/bootstrap-controller.sh"
# start_commands["model_registry"]="/exports/applications/model-registry/bootstrap-model-registry.sh"
# start_commands["agent_1"]="/exports/applications/agent-Service/bootstrap-agent.sh"
# start_commands["agent_2"]="/exports/applications/agent-Service/bootstrap-agent.sh"
# start_commands["frontend"]="/exports/applications/frontend/bootstrap-frontend.sh"
# start_commands["gateway"]="sudo systemctl start deployment-gateway"

# # Ensure necessary permissions for scripts
# chmod +x /exports/applications/service-registry/bootstrap-service-registry.sh
# chmod +x /exports/applications/controller-Service/bootstrap-controller.sh
# chmod +x /exports/applications/model-registry/bootstrap-model-registry.sh
# chmod +x /exports/applications/agent-Service/bootstrap-agent.sh
# chmod +x /exports/applications/frontend/bootstrap-frontend.sh

# # Define SSH credentials using .env values
# declare -A ssh_users
# declare -A ssh_passwords

# ssh_users["${service_registry_ip}"]="${service_registry_user}"
# ssh_passwords["${service_registry_ip}"]="${service_registry_pass}"
# ssh_users["${life_cycle_manager_ip}"]="${life_cycle_manager_user}"
# ssh_passwords["${life_cycle_manager_ip}"]="${life_cycle_manager_pass}"
# ssh_users["${model_registry_ip}"]="${model_registry_user}"
# ssh_passwords["${model_registry_ip}"]="${model_registry_pass}"
# ssh_users["${agent_1_ip}"]="${agent_1_user}"
# ssh_passwords["${agent_1_ip}"]="${agent_1_pass}"
# ssh_users["${agent_2_ip}"]="${agent_2_user}"
# ssh_passwords["${agent_2_ip}"]="${agent_2_pass}"
# ssh_users["${frontend_ip}"]="${frontend_user}"
# ssh_passwords["${frontend_ip}"]="${frontend_pass}"

# # Cleanup function
# cleanup_service() {
#     service_name=$1
#     machine_ip=$2
#     command=${start_commands[$service_name]}

#     if [ "$machine_ip" == "$local_ip" ]; then
#         echo "Cleaning up $service_name locally..." | tee -a $LOG_FILE
#         pkill -f "$command"
#     else
#         machine_user=${ssh_users[$machine_ip]}
#         machine_pass=${ssh_passwords[$machine_ip]}
#         echo "Checking if $service_name is running on $machine_ip..." | tee -a $LOG_FILE
#         sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "pgrep -f $command" > /dev/null 2>&1
#         if [ $? -eq 0 ]; then
#             echo "$service_name is running on $machine_ip. Cleaning up..." | tee -a $LOG_FILE
#             sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "pkill -f $command"
#         else
#             echo "$service_name is not running on $machine_ip." | tee -a $LOG_FILE
#         fi
#     fi
# }

# # Start service function
# start_service() {
#     service_name=$1
#     machine_ip=$2
#     command=${start_commands[$service_name]}

#     if [ "$machine_ip" == "$local_ip" ]; then
#         hostname=$(hostname)
#         echo "Starting $service_name locally with hostname $hostname..." | tee -a $LOG_FILE
#         AGENT_HOSTNAME="$hostname" LOG_FILE="/exports/applications/agent-Service/logs/agent-$hostname.log" bash -c "$command"
#     else
#         machine_user=${ssh_users[$machine_ip]}
#         machine_pass=${ssh_passwords[$machine_ip]}
#         hostname=$(sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "hostname")
#         echo "Starting $service_name remotely on $machine_ip with hostname $hostname..." | tee -a $LOG_FILE
#         sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "export AGENT_HOSTNAME=$hostname && export LOG_FILE=/exports/applications/agent-Service/logs/agent-$hostname.log && bash -c '$command'"
#     fi

#     if [ $? -eq 0 ]; then
#         echo "$service_name started successfully on $machine_ip." | tee -a $LOG_FILE
#     else
#         echo "Failed to start $service_name on $machine_ip." | tee -a $LOG_FILE
#         exit 1
#     fi
# }

# # Define a start sequence
# service_start_sequence=("service_registry" "life_cycle_manager" "model_registry" "agent_1" "agent_2" "frontend")

# # Only run NFS client where necessary (not on bootstrap or NFS server)
# for service in "${service_start_sequence[@]}"; do
#     machine_ip="${services[$service]}"
#     if [ "$machine_ip" != "$local_ip" ] && [ "$machine_ip" != "$nfs_server_ip" ]; then
#         machine_user=${ssh_users[$machine_ip]}
#         machine_pass=${ssh_passwords[$machine_ip]}

#         echo "Copying boot_nfs_client.sh to $machine_ip..." | tee -a $LOG_FILE
#         sshpass -p "$machine_pass" scp -o StrictHostKeyChecking=no "$BOOT_NFS_CLIENT_SCRIPT" $machine_user@$machine_ip:~/boot_nfs_client.sh

#         echo "Ensuring boot_nfs_client.sh is executable on $machine_ip..." | tee -a $LOG_FILE
#         sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "chmod +x ~/boot_nfs_client.sh"

#         echo "Running boot_nfs_client.sh on $machine_ip..." | tee -a $LOG_FILE
#         sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "echo $machine_pass | sudo -S bash -c '/home/$machine_user/boot_nfs_client.sh $nfs_server_ip'"

#         if [ $? -eq 0 ]; then
#             echo "boot_nfs_client.sh ran successfully on $machine_ip." | tee -a $LOG_FILE
#         else
#             echo "Failed to run boot_nfs_client.sh on $machine_ip." | tee -a $LOG_FILE
#             exit 1
#         fi
#     fi
# done

# # Start Gateway Service
# # echo "Starting Gateway Service on $gateway_ip..." | tee -a $LOG_FILE
# # sshpass -p "$gateway_pass" ssh -o StrictHostKeyChecking=no $gateway_user@$gateway_ip \
# # "echo $gateway_pass | sudo -S systemctl start deployment-gateway"

# # if [ $? -eq 0 ]; then
# #     echo "Gateway started successfully on $gateway_ip." | tee -a $LOG_FILE
# # else
# #     echo "Failed to start Gateway on $gateway_ip." | tee -a $LOG_FILE
# #     exit 1
# # fi

# # Start services
# for service in "${service_start_sequence[@]}"; do
#     machine_ip="${services[$service]}"
#     # cleanup_service "$service" "$machine_ip"
#     start_service "$service" "$machine_ip"
# done

# echo "All services started successfully." | tee -a $LOG_FILE
# echo "Bootstrap completed at $(date)" | tee -a $LOG_FILE

#!/bin/bash

# Define Variables
ENV_FILE="/exports/applications/.env"
LOG_FILE="/exports/applications/bootstrap-service/bootstrap.log"
BOOT_NFS_CLIENT_SCRIPT="/exports/applications/bootstrap-service/boot_nfs_client.sh"

# Load environment variables from the .env file
set -a
source "$ENV_FILE"
set +a

echo "Bootstrap started at $(date)" | tee -a $LOG_FILE
chmod 777 $LOG_FILE

# Define local IP (should match the IP of the bootstrap machine)
local_ip="${bootstrap_machine_ip}"

# Define service-machine mappings using values from .env
declare -A services
services["service_registry"]="${service_registry_ip}"
services["life_cycle_manager"]="${life_cycle_manager_ip}"
services["model_registry"]="${model_registry_ip}"
services["agent_1"]="${agent_1_ip}"
services["agent_2"]="${agent_2_ip}"
services["frontend"]="${frontend_ip}"

# Define start commands
declare -A start_commands
start_commands["service_registry"]="/exports/applications/service-registry/bootstrap-service-registry.sh"
start_commands["life_cycle_manager"]="/exports/applications/controller-Service/bootstrap-controller.sh"
start_commands["model_registry"]="/exports/applications/model-registry/bootstrap-model-registry.sh"
start_commands["agent_1"]="/exports/applications/agent-Service/bootstrap-agent.sh"
start_commands["agent_2"]="/exports/applications/agent-Service/bootstrap-agent.sh"
start_commands["frontend"]="/exports/applications/frontend/bootstrap-frontend.sh"
start_commands["gateway"]="sudo systemctl start deployment-gateway"

# Ensure necessary permissions for scripts
chmod +x /exports/applications/service-registry/bootstrap-service-registry.sh
chmod +x /exports/applications/controller-Service/bootstrap-controller.sh
chmod +x /exports/applications/model-registry/bootstrap-model-registry.sh
chmod +x /exports/applications/agent-Service/bootstrap-agent.sh
chmod +x /exports/applications/frontend/bootstrap-frontend.sh

# Define SSH credentials using .env values
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
ssh_users["${frontend_ip}"]="${frontend_user}"
ssh_passwords["${frontend_ip}"]="${frontend_pass}"

# Check if machine is local
is_local() {
    machine_ip=$1
    if [ "$machine_ip" == "$local_ip" ]; then
        return 0  # true in bash
    else
        return 1  # false in bash
    fi
}

# Cleanup function
cleanup_service() {
    service_name=$1
    machine_ip=$2
    command=${start_commands[$service_name]}

    if is_local "$machine_ip"; then
        echo "Cleaning up $service_name locally..." | tee -a $LOG_FILE
        pkill -f "$command"
    else
        machine_user=${ssh_users[$machine_ip]}
        machine_pass=${ssh_passwords[$machine_ip]}
        echo "Checking if $service_name is running on $machine_ip..." | tee -a $LOG_FILE
        sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "pgrep -f $command" > /dev/null 2>&1
        if [ $? -eq 0 ]; then
            echo "$service_name is running on $machine_ip. Cleaning up..." | tee -a $LOG_FILE
            sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "pkill -f $command"
        else
            echo "$service_name is not running on $machine_ip." | tee -a $LOG_FILE
        fi
    fi
}

# Start service function
start_service() {
    service_name=$1
    machine_ip=$2
    command=${start_commands[$service_name]}

    if is_local "$machine_ip"; then
        hostname=$(hostname)
        echo "Starting $service_name locally with hostname $hostname..." | tee -a $LOG_FILE
        AGENT_HOSTNAME="$hostname" LOG_FILE="/exports/applications/agent-Service/logs/agent-$hostname.log" bash -c "$command"
    else
        machine_user=${ssh_users[$machine_ip]}
        machine_pass=${ssh_passwords[$machine_ip]}
        hostname=$(sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "hostname")
        echo "Starting $service_name remotely on $machine_ip with hostname $hostname..." | tee -a $LOG_FILE
        sshpass -p "$machine_pass" ssh -o StrictHostKeyChecking=no $machine_user@$machine_ip "export AGENT_HOSTNAME=$hostname && export LOG_FILE=/exports/applications/agent-Service/logs/agent-$hostname.log && bash -c '$command'"
    fi

    if [ $? -eq 0 ]; then
        echo "$service_name started successfully on $machine_ip." | tee -a $LOG_FILE
    else
        echo "Failed to start $service_name on $machine_ip." | tee -a $LOG_FILE
        exit 1
    fi
}

# Define a start sequence
service_start_sequence=("service_registry" "life_cycle_manager" "agent_1" "agent_2" "frontend")

# Run boot_nfs_client.sh only for non-bootstrap and non-NFS server hosts
for service in "${service_start_sequence[@]}"; do
    machine_ip="${services[$service]}"
    
    # Skip SSH and NFS client setup for local machine
    if ! is_local "$machine_ip" && [ "$machine_ip" != "$nfs_server_ip" ]; then
        machine_user=${ssh_users[$machine_ip]}
        machine_pass=${ssh_passwords[$machine_ip]}

        echo "Copying boot_nfs_client.sh to $machine_ip..." | tee -a $LOG_FILE
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
    else
        echo "Skipping boot_nfs_client.sh for $service on $machine_ip (local/NFS server)" | tee -a $LOG_FILE
    fi
done

# Start services
for service in "${service_start_sequence[@]}"; do
    machine_ip="${services[$service]}"
    start_service "$service" "$machine_ip"
done

echo "All services started successfully." | tee -a $LOG_FILE
echo "Bootstrap completed at $(date)" | tee -a $LOG_FILE