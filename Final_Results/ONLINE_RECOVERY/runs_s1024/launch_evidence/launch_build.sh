set -e
B='@RBASE@'
case "$(hostname -I)" in *10.129.148.246*) ABI=u22;; *10.129.27.111*) ABI=u24;; *) exit 2;; esac
nohup bash "$B/build.sh" "$B" "$ABI" > "$B/build_$ABI.log" 2>&1 < /dev/null &
p=$!
echo "$p" > "$B/build.pid"
printf 'build_pid=%s abi=%s\n' "$p" "$ABI"
