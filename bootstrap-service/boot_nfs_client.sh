# #!/bin/bash
# # Updated NFS client script that takes server IP as input

# # Check if script is run as root
# if [ "$(id -u)" -ne 0 ]; then
#     echo "This script must be run as root. Please use sudo."
#     exit 1
# fi

# # Get NFS server IP from user input
# if [ -n "$1" ]; then
#     NFS_SERVER="$1"
# else
#     read -p "Enter NFS server IP address: " NFS_SERVER
# fi

# # Validate IP address format
# if ! [[ $NFS_SERVER =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
#     echo "Invalid IP address format. Please enter a valid IPv4 address."
#     exit 1
# fi

# # Define other variables
# MOUNT_ROOT="/exports"
# ENV_FILE="${MOUNT_ROOT}/applications/.env"

# echo "NFS Server IP: ${NFS_SERVER}"

# # Install NFS Common Client
# echo "Installing nfs-common..."
# apt update && apt -y install nfs-common || {
#     echo "Failed to install nfs-common. Please check your internet connection."
#     exit 1
# }

# # Create mount points if they don't exist
# echo "Creating mount points..."
# mkdir -p ${MOUNT_ROOT}/models
# mkdir -p ${MOUNT_ROOT}/applications
# mkdir -p ${MOUNT_ROOT}/datasets

# # Check if server is reachable
# echo "Checking connection to NFS server..."
# ping -c 1 ${NFS_SERVER} > /dev/null 2>&1
# if [ $? -ne 0 ]; then
#     echo "Cannot reach NFS server at ${NFS_SERVER}. Please verify the IP address and network connection."
#     exit 1
# fi

# # Try to mount each export
# echo "Mounting NFS exports from ${NFS_SERVER}..."

# # Function to mount a directory with proper error handling
# mount_nfs_dir() {
#     local export_dir="$1"
#     local mount_point="$2"
    
#     echo "Mounting ${NFS_SERVER}:${export_dir} to ${mount_point}..."
    
#     # Check if already mounted
#     if mount | grep -q "${mount_point}"; then
#         echo "${mount_point} is already mounted. Unmounting first..."
#         umount ${mount_point} 2>/dev/null
#     fi
    
#     # Attempt to mount
#     mount -t nfs ${NFS_SERVER}:${export_dir} ${mount_point}
    
#     if [ $? -eq 0 ]; then
#         echo "Successfully mounted ${export_dir} to ${mount_point}"
#         return 0
#     else
#         echo "Failed to mount ${export_dir} to ${mount_point}"
#         return 1
#     fi
# }

# # Mount each directory
# mount_nfs_dir "/exports/models" "${MOUNT_ROOT}/models"
# mount_nfs_dir "/exports/applications" "${MOUNT_ROOT}/applications" 
# mount_nfs_dir "/exports/datasets" "${MOUNT_ROOT}/datasets"

# # Check if mounts are created
# echo "Checking mounted filesystems:"
# df -h | grep "${NFS_SERVER}" || {
#     echo "No NFS mounts were successful. Please check server configuration."
#     exit 1
# }

# # Source .env file if it exists (after mounting)
# if [ -f "$ENV_FILE" ]; then
#     echo "Found .env file, sourcing environment variables..."
#     set -a
#     source "$ENV_FILE"
#     set +a
# else
#     echo "No .env file found at ${ENV_FILE}"
# fi

# echo "NFS client setup complete!"

#!/bin/bash
# Updated NFS client script that takes server IP as input

# Check if script is run as root
if [ "$(id -u)" -ne 0 ]; then
    echo "This script must be run as root. Please use sudo."
    exit 1
fi

# Get NFS server IP from user input
if [ -n "$1" ]; then
    NFS_SERVER="$1"
else
    read -p "Enter NFS server IP address: " NFS_SERVER
fi

# Validate IP address format
if ! [[ $NFS_SERVER =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Invalid IP address format. Please enter a valid IPv4 address."
    exit 1
fi

# Define other variables
MOUNT_ROOT="/exports"
ENV_FILE="${MOUNT_ROOT}/applications/.env"

echo "NFS Server IP: ${NFS_SERVER}"

# Install NFS Common Client
echo "Installing nfs-common..."
apt update || {
    echo "Failed to update package lists. Continuing..."
}
# apt -y install nfs-common || {
#     echo "Failed to install nfs-common. Please check your internet connection or package manager configuration."
#     exit 1
# }

# Create mount points if they don't exist
echo "Creating mount points..."
mkdir -p ${MOUNT_ROOT}/models
mkdir -p ${MOUNT_ROOT}/applications
mkdir -p ${MOUNT_ROOT}/datasets

# Check if server is reachable
echo "Checking connection to NFS server..."
ping -c 1 ${NFS_SERVER} > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo "Cannot reach NFS server at ${NFS_SERVER}. Please verify the IP address and network connection."
    exit 1
fi

# Try to mount each export
echo "Mounting NFS exports from ${NFS_SERVER}..."

# Function to mount a directory with proper error handling
mount_nfs_dir() {
    local export_dir="$1"
    local mount_point="$2"
    
    echo "Mounting ${NFS_SERVER}:${export_dir} to ${mount_point}..."
    
    # Check if already mounted
    if mount | grep -q "${mount_point}"; then
        echo "${mount_point} is already mounted. Unmounting first..."
        umount ${mount_point} 2>/dev/null
    fi
    
    # Attempt to mount
    mount -t nfs ${NFS_SERVER}:${export_dir} ${mount_point}
    
    if [ $? -eq 0 ]; then
        echo "Successfully mounted ${export_dir} to ${mount_point}"
        return 0
    else
        echo "Failed to mount ${export_dir} to ${mount_point}"
        return 1
    fi
}

# Mount each directory
mount_nfs_dir "/exports/models" "${MOUNT_ROOT}/models"
mount_nfs_dir "/exports/applications" "${MOUNT_ROOT}/applications" 
mount_nfs_dir "/exports/datasets" "${MOUNT_ROOT}/datasets"

# Check if mounts are created
echo "Checking mounted filesystems:"
df -h | grep "${NFS_SERVER}" || {
    echo "No NFS mounts were successful. Please check server configuration."
    exit 1
}

# Source .env file if it exists (after mounting)
if [ -f "$ENV_FILE" ]; then
    echo "Found .env file, sourcing environment variables..."
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "No .env file found at ${ENV_FILE}"
fi

echo "NFS client setup complete!"