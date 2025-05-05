#!/bin/bash
# Enhanced NFS server setup script with better error handling and configuration

# Check if running as root
if [ "$(id -u)" -ne 0 ]; then
    echo "This script must be run as root"
    exit 1
fi

# Install NFS server if not already installed
echo "Checking and installing NFS server packages..."
apt update && apt -y install nfs-kernel-server || {
    echo "Failed to install NFS server packages"
    exit 1
}

# Define export directories
EXPORT_DIRS=("/exports/models" "/exports/applications" "/exports/datasets")

# Create export directories if they don't exist
echo "Creating export directories..."
for dir in "${EXPORT_DIRS[@]}"; do
    mkdir -p "$dir"
    chmod 777 "$dir"
    chown nobody:nogroup "$dir"
done

# Detect network interfaces and get IP address
echo "Detecting network interfaces..."
# INTERFACES=$(ip -o link show | awk -F': ' '{print $2}' | grep -v "lo\|virbr\|docker")
INTERFACES=$(ip -o -4 addr show | awk '{print $2}' | sort -u)

# Print available interfaces for selection
echo "Available network interfaces:"
i=1
declare -A interface_map
for iface in $INTERFACES; do
    ip_addr=$(ip addr show $iface | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -1)
    if [ -n "$ip_addr" ]; then
        echo "$i) $iface ($ip_addr)"
        interface_map[$i]=$iface
        ((i++))
    fi
done

# Let user select interface or use default
if [ ${#interface_map[@]} -eq 0 ]; then
    echo "No network interfaces with IPv4 addresses found"
    exit 1
elif [ ${#interface_map[@]} -eq 1 ]; then
    SELECTED_IFACE=${interface_map[1]}
    echo "Using only available interface: $SELECTED_IFACE"
else
    read -p "Select interface number [1]: " selection
    selection=${selection:-1}
    SELECTED_IFACE=${interface_map[$selection]}
    
    if [ -z "$SELECTED_IFACE" ]; then
        echo "Invalid selection"
        exit 1
    fi
fi

# Get IP address from selected interface
IP_ADDRESS=$(ip addr show $SELECTED_IFACE | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -1)
if [ -z "$IP_ADDRESS" ]; then
    echo "Failed to obtain IP address from interface $SELECTED_IFACE"
    exit 1
fi

# Generate subnet in CIDR notation
SUBNET="${IP_ADDRESS%.*}.0/24"
echo "Using IP address: $IP_ADDRESS with subnet: $SUBNET"

# Backup original exports file
BACKUP_FILE="/etc/exports.bak.$(date +%Y%m%d%H%M%S)"
cp /etc/exports "$BACKUP_FILE"
echo "Original exports file backed up to $BACKUP_FILE"

# Create new exports file
echo "Creating exports configuration..."
cat > /etc/exports <<EOF
# NFS exports created by setup script - $(date)
# Exports for subnet: $SUBNET (generated from $SELECTED_IFACE)
/exports/models $SUBNET(rw,sync,no_subtree_check,no_root_squash)
/exports/applications $SUBNET(rw,sync,no_subtree_check,no_root_squash)
/exports/datasets $SUBNET(rw,sync,no_subtree_check,no_root_squash)
EOF

echo "Updated /etc/exports file:"
cat /etc/exports

# Configure and restart NFS services
echo "Configuring and restarting NFS services..."
systemctl enable rpcbind
systemctl enable nfs-server
systemctl start rpcbind
systemctl restart nfs-server

# Re-export the filesystems
exportfs -ra

# Verify NFS is running
if systemctl is-active --quiet nfs-server; then
    echo "NFS service started successfully with updated exports"
else
    echo "Failed to start NFS service"
    exit 1
fi

# Check if portmap/rpcbind is running
if systemctl is-active --quiet rpcbind; then
    echo "RPC bind service is running"
else
    echo "Warning: RPC bind service is not running"
    echo "Attempting to start rpcbind..."
    systemctl start rpcbind
fi

# Check firewall status and provide instructions
if command -v ufw &> /dev/null && ufw status | grep -q "active"; then
    echo "Firewall is active. Opening required ports..."
    ufw allow from $SUBNET to any port 111
    ufw allow from $SUBNET to any port 2049
    echo "Firewall rules added for NFS"
fi

# Show export list
echo "Currently exported filesystems:"
exportfs -v

echo "NFS server setup complete!"
echo "Server IP address: $IP_ADDRESS"
echo "Clients in subnet $SUBNET can now mount the following exports:"
for dir in "${EXPORT_DIRS[@]}"; do
    echo "  - $IP_ADDRESS:$dir"
done
echo
echo "On client machines, use:"
echo "sudo mount -t nfs $IP_ADDRESS:/exports/models /path/to/local/mount"