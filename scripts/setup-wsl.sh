#!/usr/bin/env bash
# WSL2 Ubuntu 안에서 실행한다. 여러 번 실행해도 된다(이미 된 단계는 건너뜀).
#   - Redis, MySQL 설치 + DB ranking, 계정 bench/bench
#   - --with-kafka: Kafka 4.3.1 (KRaft 단일 노드, systemd 서비스 kafka)
#
# Windows에서: wsl -d Ubuntu -u root -- bash scripts/setup-wsl.sh --with-kafka
# WSL 안에서:  sudo bash scripts/setup-wsl.sh --with-kafka
set -euo pipefail

KAFKA_VERSION=4.3.1
SCALA_VERSION=2.13
WITH_KAFKA=false
[[ "${1:-}" == "--with-kafka" ]] && WITH_KAFKA=true

if [[ $EUID -ne 0 ]]; then
    echo "root로 실행해야 한다: sudo bash $0 $*" >&2
    exit 1
fi

if [[ "$(ps -p 1 -o comm=)" != "systemd" ]]; then
    echo "systemd가 꺼져 있다. /etc/wsl.conf에 아래를 넣고 Windows에서 'wsl --shutdown' 후 다시 실행:" >&2
    printf '[boot]\nsystemd=true\n' >&2
    exit 1
fi

export DEBIAN_FRONTEND=noninteractive
echo "==> apt 패키지"
apt-get update -qq
# --no-upgrade: 이미 설치돼 있으면 버전을 바꾸지 않는다
apt-get install -y -qq --no-upgrade redis-server mysql-server curl >/dev/null
systemctl enable --now redis-server mysql

echo "==> MySQL DB ranking, 계정 bench/bench"
mysql <<'SQL'
CREATE DATABASE IF NOT EXISTS ranking;
CREATE USER IF NOT EXISTS 'bench'@'localhost' IDENTIFIED BY 'bench';
GRANT ALL PRIVILEGES ON ranking.* TO 'bench'@'localhost';
FLUSH PRIVILEGES;
SQL

if $WITH_KAFKA; then
    echo "==> Kafka ${KAFKA_VERSION}"
    apt-get install -y -qq --no-upgrade openjdk-21-jre-headless >/dev/null

    if [[ ! -x /opt/kafka/bin/kafka-server-start.sh ]]; then
        tgz="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
        # 최신이 아니게 되면 downloads에서 빠지고 archive로 옮겨진다
        curl -fsSL "https://downloads.apache.org/kafka/${KAFKA_VERSION}/${tgz}" -o "/tmp/${tgz}" \
            || curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${tgz}" -o "/tmp/${tgz}"
        mkdir -p /opt/kafka
        tar -xzf "/tmp/${tgz}" -C /opt/kafka --strip-components=1
        rm -f "/tmp/${tgz}"
    fi

    props=/opt/kafka/config/server.properties
    set_prop() {
        if grep -qE "^$1=" "$props"; then
            sed -i "s|^$1=.*|$1=$2|" "$props"
        else
            echo "$1=$2" >> "$props"
        fi
    }
    # localhost로 광고하면 Windows 클라이언트가 ::1로 붙으려다 실패한다
    set_prop advertised.listeners "PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093"
    set_prop log.dirs /var/lib/kafka
    set_prop num.partitions 4

    if [[ ! -f /var/lib/kafka/meta.properties ]]; then
        mkdir -p /var/lib/kafka
        /opt/kafka/bin/kafka-storage.sh format --standalone \
            -t "$(/opt/kafka/bin/kafka-storage.sh random-uuid)" -c "$props"
    fi

    cat > /etc/systemd/system/kafka.service <<'UNIT'
[Unit]
Description=Apache Kafka (KRaft single node)
After=network.target

[Service]
Environment="KAFKA_HEAP_OPTS=-Xmx1G -Xms512M"
Environment="KAFKA_OPTS=-Djava.net.preferIPv4Stack=true"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-failure

[Install]
WantedBy=multi-user.target
UNIT
    systemctl daemon-reload
    systemctl enable --now kafka
fi

echo "==> 확인"
redis-cli ping
mysql -ubench -pbench ranking -e "SELECT 'mysql ok' AS status" 2>/dev/null
if $WITH_KAFKA; then
    for _ in $(seq 30); do
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list >/dev/null 2>&1 && break
        sleep 1
    done
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --list >/dev/null && echo "kafka ok"
fi
echo "완료"
