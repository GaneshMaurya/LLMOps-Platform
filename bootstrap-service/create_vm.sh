#!/bin/bash

# VM Configuration
VM_NAME="AlpineVM"
OS_TYPE="Linux_64"
RAM_SIZE=512
CPU_COUNT=1
HDD_SIZE=5000
ISO_PATH="/path/to/alpine-virt-3.21.3-x86_64.iso"
HDD_PATH="$HOME/VirtualBox VMs/$VM_NAME/$VM_NAME.vdi"
ANSWER_DISK_PATH="$HOME/VirtualBox VMs/$VM_NAME/answers.vdi"
ANSWER_FILE_CONTENT="KEYMAPOPTS=\"us\"
HOSTNAMEOPTS=\"-n alpine-vm\"
INTERFACESOPTS=\"auto\"
DNSOPTS=\"8.8.8.8\"
TIMEZONEOPTS=\"UTC\"
PROXYOPTS=\"none\"
APKREPOSOPTS=\"http://dl-cdn.alpinelinux.org/alpine/latest-stable/main\"
SSHDOPTS=\"-c openssh\"
USEROPTS=\"-a -u myuser -f -i\"
ROOT_PASSWORD=\"password123\"
USER_PASSWORD=\"password123\"
BOOTOPTS=\"grub\""

# Function to check errors
handle_error() {
    if [ $? -ne 0 ]; then
        echo "Error: $1"
        exit 1
    fi
}

# Step 1: Create VM
VBoxManage createvm --name "$VM_NAME" --ostype "$OS_TYPE" --register
handle_error "Failed to create VM."

# Step 2: Configure Memory & CPU
VBoxManage modifyvm "$VM_NAME" --memory $RAM_SIZE --cpus $CPU_COUNT --uart1 0x3F8 4 --uartmode1 server /tmp/$VM_NAME-serial
handle_error "Failed to configure VM settings."

# Step 3: Create Hard Disk
VBoxManage createhd --filename "$HDD_PATH" --size $HDD_SIZE --format VDI
handle_error "Failed to create hard disk."

# Step 4: Attach Storage Controllers
VBoxManage storagectl "$VM_NAME" --name "SATA Controller" --add sata --controller IntelAhci
VBoxManage storageattach "$VM_NAME" --storagectl "SATA Controller" --port 0 --device 0 --type hdd --medium "$HDD_PATH"
handle_error "Failed to attach hard disk."

VBoxManage storagectl "$VM_NAME" --name "IDE Controller" --add ide
VBoxManage storageattach "$VM_NAME" --storagectl "IDE Controller" --port 0 --device 0 --type dvddrive --medium "$ISO_PATH"
handle_error "Failed to attach ISO."

# Step 5: Create an extra virtual disk for the answer file
VBoxManage createhd --filename "$ANSWER_DISK_PATH" --size 10 --format VDI
handle_error "Failed to create answer disk."

# Step 6: Attach the extra disk
VBoxManage storageattach "$VM_NAME" --storagectl "SATA Controller" --port 1 --device 0 --type hdd --medium "$ANSWER_DISK_PATH"
handle_error "Failed to attach answer disk."

# Step 7: Start VM in GUI mode (so you can interact with it)
VBoxManage startvm "$VM_NAME" --type gui
handle_error "Failed to start VM."

# Step 8: Write answer file to the extra disk
echo "$ANSWER_FILE_CONTENT" > "$HOME/VirtualBox VMs/$VM_NAME/setup-alpine.answers"
handle_error "Failed to create answer file."

echo "Virtual machine '$VM_NAME' has been created. You need to manually mount the answer disk inside the VM."
