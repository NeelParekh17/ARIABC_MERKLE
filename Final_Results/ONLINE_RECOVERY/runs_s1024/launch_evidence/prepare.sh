set -e
B='@RBASE@'
python3 "$B/repo/scripts/distributed/recovery_s1024/prepare_runner.py" --repo "$B/repo" --tag '@RTAG@'
"$B/venv/bin/pip" install 'psycopg[binary]'
"$B/venv/bin/pip" freeze > "$B/python_dependencies.txt"
