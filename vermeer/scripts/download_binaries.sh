#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TOOLS_DIR="$PROJECT_ROOT/tools"

# Versions
SUPERVISORD_VERSION="0.6.9"
PROTOC_VERSION="21.12"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Download and verify file with MD5 checksum
download_and_verify() {
    local url=$1
    local filepath=$2
    local expected_md5=$3
    local md5_cmd

    # Detect md5 command (md5sum on Linux, md5 on macOS)
    if command -v md5sum &> /dev/null; then
        md5_cmd="md5sum"
    elif command -v md5 &> /dev/null; then
        md5_cmd="md5 -r"
    else
        log_warn "MD5 verification tool not found, skipping checksum verification"
        expected_md5=""
    fi

    if [[ -f $filepath ]]; then
        if [[ -n $expected_md5 ]]; then
            log_info "File $filepath exists. Verifying MD5 checksum..."
            actual_md5=$($md5_cmd "$filepath" | awk '{ print $1 }')
            if [[ $actual_md5 != $expected_md5 ]]; then
                log_warn "MD5 checksum mismatch for $filepath. Expected: $expected_md5, got: $actual_md5"
                log_info "Deleting and re-downloading $filepath..."
                rm -f "$filepath"
            else
                log_info "MD5 checksum verified for $filepath"
                return 0
            fi
        else
            log_info "File $filepath already exists, skipping download"
            return 0
        fi
    fi

    log_info "Downloading $filepath..."
    if ! curl -L -f "$url" -o "$filepath"; then
        log_error "Failed to download from $url"
        return 1
    fi

    if [[ -n $expected_md5 ]]; then
        actual_md5=$($md5_cmd "$filepath" | awk '{ print $1 }')
        if [[ $actual_md5 != $expected_md5 ]]; then
            log_error "MD5 checksum verification failed after download. Expected: $expected_md5, got: $actual_md5"
            rm -f "$filepath"
            return 1
        fi
        log_info "MD5 checksum verified successfully"
    fi

    return 0
}

# Download supervisord
download_supervisord() {
    local platform=$1
    local arch=$2
    local md5=$3
    
    SUPERVISORD_DIR="$TOOLS_DIR/supervisord/${platform}"
    mkdir -p "$SUPERVISORD_DIR"
    
    local download_url="https://github.com/ochinchina/supervisord/releases/download/v${SUPERVISORD_VERSION}/supervisord_${SUPERVISORD_VERSION}_Linux_${arch}.tar.gz"
    local temp_file="/tmp/supervisord_${platform}.tar.gz"
    
    log_info "Downloading supervisord for ${platform}..."
    
    if ! download_and_verify "$download_url" "$temp_file" "$md5"; then
        return 1
    fi
    
    if [ ! -f "$SUPERVISORD_DIR/supervisord" ]; then
        tar -xzf "$temp_file" -C "$SUPERVISORD_DIR" --strip-components=1
        chmod +x "$SUPERVISORD_DIR/supervisord"
        log_info "Successfully extracted supervisord for ${platform}"
    fi
    
    rm -f "$temp_file"
    return 0
}

# Download protoc
download_protoc() {
    local platform=$1
    local protoc_platform=$2
    local md5=$3
    
    PROTOC_DIR="$TOOLS_DIR/protoc/${platform}"
    mkdir -p "$PROTOC_DIR"
    
    local download_url="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-${protoc_platform}.zip"
    local temp_file="/tmp/protoc_${platform}.zip"
    
    log_info "Downloading protoc for ${platform}..."
    
    if ! download_and_verify "$download_url" "$temp_file" "$md5"; then
        return 1
    fi
    
    if [ ! -f "$PROTOC_DIR/protoc" ]; then
        unzip -q "$temp_file" -d "$PROTOC_DIR"
        chmod +x "$PROTOC_DIR/bin/protoc"
        
        # Move protoc binary to the root of protoc directory for compatibility
        if [ -f "$PROTOC_DIR/bin/protoc" ]; then
            cp "$PROTOC_DIR/bin/protoc" "$PROTOC_DIR/protoc"
        fi
        
        log_info "Successfully extracted protoc for ${platform}"
    fi
    
    rm -f "$temp_file"
    return 0
}

# Main function
main() {
    log_info "Starting to download binary dependencies..."
    log_info "Tools directory: $TOOLS_DIR"
    
    # Download supervisord for different platforms
    # MD5 checksums for supervisord v4.2.5
    download_supervisord "linux_amd64" "64-bit" "" # Add MD5 if available
    download_supervisord "linux_arm64" "ARM64" "" # Add MD5 if available
    
    # Download protoc for different platforms
    # MD5 checksums for protoc v21.12
    download_protoc "linux64" "linux-x86_64" "" # Add MD5 if available
    download_protoc "osxm1" "osx-aarch_64" "" # Add MD5 if available
    
    log_info "All binary dependencies downloaded successfully!"
    log_info ""
    log_info "Downloaded binaries:"
    log_info "  - supervisord (linux_amd64, linux_arm64)"
    log_info "  - protoc (linux64, osxm1, win64)"
    log_info ""
    log_info "Note: These binaries are excluded from source releases."
    log_info "Users should run 'make download-binaries' before building."
}

# Run main function
main "$@"
