#!/bin/bash
# =============================================================================
# Crypto Big Data - Environment Setup for Linux / WSL / macOS
# =============================================================================

# Lấy thư mục gốc project linh hoạt
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Tự động tìm Java 17 nếu chưa có JAVA_HOME
if [ -z "$JAVA_HOME" ] || [ ! -d "$JAVA_HOME" ]; then
    if [ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
    elif [ -d "/usr/lib/jvm/default-java" ]; then
        export JAVA_HOME="/usr/lib/jvm/default-java"
    fi
fi

# Tự động tìm Virtual Environment
if [ -d "$PROJECT_ROOT/.venv" ]; then
    VENV_PATH="$PROJECT_ROOT/.venv"
elif [ -d "$HOME/venvs/crypto-bigdata" ]; then
    VENV_PATH="$HOME/venvs/crypto-bigdata"
fi

if [ -n "$VENV_PATH" ]; then
    export PYSPARK_PYTHON="$VENV_PATH/bin/python"
    export PYSPARK_DRIVER_PYTHON="$VENV_PATH/bin/python"
fi

echo "=========================================================="
echo " [OK] Crypto Big Data Environment Configured (Linux/WSL)"
echo "=========================================================="
echo "PROJECT_ROOT : $PROJECT_ROOT"
echo "JAVA_HOME    : $JAVA_HOME"
echo "PYTHON       : $PYSPARK_PYTHON"
echo "=========================================================="
