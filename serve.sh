#!/usr/bin/env bash

self="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$self")" || exit 1
port=7000
pidfile=target/serve.pid
guardfile=target/serve.guard
log=target/serve.log
manual="sudo ufw route delete allow proto tcp from any to any port $port"

close_port() {
    sudo ufw route delete allow proto tcp from any to any port "$port" >/dev/null 2>&1
}

port_open() {
    sudo ufw status | grep -Eq "^$port/tcp[[:space:]].*FWD"
}

ufw_active() {
    sudo ufw status | grep -q "Status: active"
}

alive() {
    [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null
}

say() {
    echo "$2" >>"$log"
    if [ -w "$1" ]; then echo "$2" >"$1"; fi
}

close_and_check() {
    close_port
    if port_open; then close_port; fi
    ! port_open
}

if [ "$1" = guard ]; then
    main=$2
    out=$3
    firewall=$4
    echo $$ >"$guardfile"
    while st=$(ps -o stat= -p "$main") && [ "${st#Z}" = "$st" ]; do
        sleep 1
    done
    say "$out" "Останавливаю сайт…"
    closed=1
    if [ "$firewall" = 1 ]; then
        close_and_check || closed=0
    fi
    docker compose stop >>"$log" 2>&1
    rm -f "$pidfile" "$guardfile"
    if [ "$firewall" != 1 ]; then
        say "$out" "Сайт остановлен."
        exit 0
    fi
    if [ $closed = 1 ]; then
        say "$out" "Сайт остановлен, порт $port закрыт (проверено по ufw status)."
        exit 0
    fi
    say "$out" "!!! ВНИМАНИЕ: порт $port НЕ закрылся. Закройте вручную: $manual"
    exit 1
fi

if [ "$1" = stop ]; then
    if alive "$pidfile"; then
        kill "$(cat "$pidfile")"
        for _ in $(seq 60); do
            [ -f "$pidfile" ] || break
            sleep 1
        done
        if [ -f "$pidfile" ]; then
            echo "Сайт не остановился за минуту, подробности в $log"
            exit 1
        fi
    else
        docker compose stop >>"$log" 2>&1
        rm -f "$pidfile" "$guardfile"
    fi
    if ! command -v ufw >/dev/null; then
        echo "Сайт остановлен."
        exit 0
    fi
    sudo -v || exit 1
    if close_and_check; then
        echo "Сайт остановлен, порт $port закрыт (проверено по ufw status)."
        exit 0
    fi
    echo "!!! ВНИМАНИЕ: порт $port НЕ закрылся. Закройте вручную: $manual"
    exit 1
fi

if alive "$pidfile"; then
    echo "Сайт уже запущен. Остановить: Ctrl+C в его терминале или ./serve.sh stop"
    exit 1
fi

mkdir -p target
: >"$log"
firewall=1
if ! command -v ufw >/dev/null; then
    firewall=0
else
    echo "Для открытия порта $port нужен пароль sudo."
    sudo -v || exit 1
    if ! ufw_active; then
        firewall=0
        echo "Файрвол ufw выключен, порт открывать не нужно."
    elif port_open; then
        echo "Порт $port остался открытым с прошлого запуска, закрываю."
        if ! close_and_check; then
            echo "!!! Не удалось закрыть порт $port. Закройте вручную: $manual"
            exit 1
        fi
    fi
fi

rm -f "$guardfile"
out=$(tty 2>/dev/null) || out=/dev/null
if [ $firewall = 1 ]; then
    sudo setsid -f bash "$self" guard $$ "$out" 1 </dev/null >>"$log" 2>&1
elif command -v setsid >/dev/null; then
    setsid -f bash "$self" guard $$ "$out" 0 </dev/null >>"$log" 2>&1
else
    nohup bash "$self" guard $$ "$out" 0 </dev/null >>"$log" 2>&1 &
fi
for _ in $(seq 50); do
    alive "$guardfile" && break
    sleep 0.1
done
if ! alive "$guardfile"; then
    echo "Не удалось запустить сторожа, который останавливает сайт и закрывает порт. Порт не открываю."
    exit 1
fi
echo $$ >"$pidfile"

echo "Запуск…"
if ! docker compose up -d --build >>"$log" 2>&1; then
    tail -20 "$log"
    exit 1
fi
state=
for _ in $(seq 120); do
    state=$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q coordinator)" 2>/dev/null)
    [ "$state" = healthy ] && break
    sleep 1
done
if [ "$state" != healthy ]; then
    echo "Сайт не запустился, подробности в $log"
    exit 1
fi

if [ $firewall = 1 ]; then
    if ! alive "$guardfile"; then
        echo "Сторож остановился, порт не открываю. Подробности в $log"
        exit 1
    fi
    sudo ufw route allow proto tcp from any to any port "$port" >/dev/null || exit 1
    if ! port_open; then
        echo "Правило добавлено, но ufw status его не показывает: проверка закрытия не сработает. Останавливаю."
        sudo ufw status >>"$log"
        exit 1
    fi
fi

addresses() {
    if [ -d /sys/class/net ]; then
        for dev in /sys/class/net/*; do
            [ -e "$dev/device" ] || continue
            ip -4 -o addr show dev "$(basename "$dev")" scope global | sed -nE 's/.*inet ([0-9.]+)\/.*/\1/p'
        done
    else
        for dev in $(ifconfig -l 2>/dev/null); do
            case $dev in en*) ipconfig getifaddr "$dev" 2>/dev/null ;; esac
        done
    fi
}

echo
echo "Сайт работает:"
echo "  на этом компьютере:   http://127.0.0.1:$port"
found=0
for addr in $(addresses); do
    echo "  с других устройств:  http://$addr:$port"
    found=1
done
[ $found = 1 ] || echo "  с других устройств:  нет подключения к сети"
echo
echo "Ctrl+C — остановить сайт и закрыть порт"
exec sleep 2147483647
