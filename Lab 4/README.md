# How to run Kafka
To run the application, navigate to `src/` directory and call:
```bash
./run-application.sh
```
Kafka can be accessed via `localhost:9094`. Extract server can be accessed via `localhost:5001`.

To shut down the application:
```bash
docker compose down
```
If you use Ctrl+C or `docker compose stop` to stop, it may cause errors. You may try to use the above command.
