for file in /home/david/airflow/raw_data/log_error/*.log;do cat "$file">>\
result.txt;echo "" >>result.txt;done