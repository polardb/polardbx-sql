#!/bin/bash

# ==========================
# PolarDB-X Systemd Service Installer
# ==========================

# 定义变量
SERVICE_NAME="polardbx"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
POLARDBX_USER="polarx"
POLARDBX_HOME="/home/${POLARDBX_USER}/polardbx-engine"
MYSQLD_SAFE_PATH="/opt/polardbx_engine/bin/mysqld_safe"
MY_CNF_PATH="${POLARDBX_HOME}/my.cnf"
PID_FILE="${POLARDBX_HOME}/run/mysql.pid"
LOG_FILE="/var/log/${SERVICE_NAME}_install.log"
BACKUP_DIR="/etc/systemd/system/backup"
TIMEOUT_SEC=60

# 记录日志函数
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "${LOG_FILE}"
}

# 退出并打印错误信息
error_exit() {
    log "错误: $1"
    exit 1
}

# 检查命令是否存在
check_command() {
    command -v "$1" &>/dev/null || error_exit "未找到命令：$1，请安装后再试。"
}

# 确保以 root 用户运行
if [ "$(id -u)" -ne 0 ]; then
    error_exit "请以 root 用户运行此脚本。"
fi

log "检查系统环境..."

# 检查所需命令
REQUIRED_COMMANDS=("systemctl" "tee" "chmod" "mv" "mkdir" "sleep")
for cmd in "${REQUIRED_COMMANDS[@]}"; do
    check_command "$cmd"
done

# 确保 mysqld_safe 存在
if [ ! -f "${MYSQLD_SAFE_PATH}" ]; then
    error_exit "未找到 mysqld_safe，路径可能不正确：${MYSQLD_SAFE_PATH}"
fi

# 确保 my.cnf 存在
if [ ! -f "${MY_CNF_PATH}" ]; then
    error_exit "未找到 my.cnf，路径可能不正确：${MY_CNF_PATH}"
fi

# 创建备份目录（如果不存在）
if [ ! -d "${BACKUP_DIR}" ]; then
    mkdir -p "${BACKUP_DIR}" || error_exit "无法创建备份目录：${BACKUP_DIR}"
    log "已创建备份目录：${BACKUP_DIR}"
fi

# 备份旧的 service 文件
if [ -f "${SERVICE_FILE}" ]; then
    backup_file="${BACKUP_DIR}/${SERVICE_NAME}_$(date '+%Y%m%d%H%M%S').service"
    mv "${SERVICE_FILE}" "${backup_file}" || error_exit "无法备份旧的服务文件：${SERVICE_FILE}"
    log "已备份旧的 service 文件到：${backup_file}"
fi

# 创建 Systemd 服务文件
log "创建 Systemd 服务文件：${SERVICE_FILE}"
cat <<EOF | tee "${SERVICE_FILE}" >/dev/null
[Unit]
Description=PolarDB-X Service
After=network.target

[Service]
Type=forking
User=${POLARDBX_USER}
ExecStart=${MYSQLD_SAFE_PATH} --defaults-file=${MY_CNF_PATH}
ExecStop=/bin/kill -TERM \$MAINPID
PIDFile=${PID_FILE}
Restart=on-failure
LimitNOFILE=65535
TimeoutSec=${TIMEOUT_SEC}

[Install]
WantedBy=multi-user.target
EOF

# 确保服务文件创建成功
if [ ! -f "${SERVICE_FILE}" ]; then
    error_exit "创建 Systemd 服务文件失败！"
fi

# 设置服务文件权限
chmod 644 "${SERVICE_FILE}" || error_exit "无法设置服务文件权限：${SERVICE_FILE}"

# 重新加载 Systemd 配置
log "重新加载 Systemd 配置..."
systemctl daemon-reload || error_exit "Systemd 配置重载失败！"

# 启用并启动 PolarDB-X 服务
log "启用 PolarDB-X 服务..."
systemctl enable "${SERVICE_NAME}" || error_exit "无法启用服务 ${SERVICE_NAME}！"

log "启动 PolarDB-X 服务..."
systemctl start "${SERVICE_NAME}" || {
    log "服务 ${SERVICE_NAME} 启动失败，尝试回滚..."
    systemctl stop "${SERVICE_NAME}"
    [ -n "${backup_file}" ] && mv "${backup_file}" "${SERVICE_FILE}" && systemctl daemon-reload
    error_exit "服务启动失败，已回滚到旧版本。"
}

# 等待服务启动
log "等待服务启动..."
sleep 5

# 检查服务状态
log "检查服务状态..."
systemctl status "${SERVICE_NAME}" --no-pager | tee -a "${LOG_FILE}"

# 检查是否运行成功
if systemctl is-active --quiet "${SERVICE_NAME}"; then
    log "PolarDB-X 服务已成功启动并配置为开机自启。"
else
    log "服务 ${SERVICE_NAME} 启动失败，尝试回滚..."
    systemctl stop "${SERVICE_NAME}"
    [ -n "${backup_file}" ] && mv "${backup_file}" "${SERVICE_FILE}" && systemctl daemon-reload
    error_exit "服务启动失败，已回滚到旧版本。"
fi

log "安装完成！"
exit 0
