#nohup uvicorn app:app  --host 0.0.0.0 --port 60001 1>log_net 2>err_net &
uvicorn app:app  --host 0.0.0.0 --port 60001
