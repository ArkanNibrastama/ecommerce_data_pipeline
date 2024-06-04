# E-Commerce Data Pipeline

[install docker and git before]

```bash
sudo git clone https://github.com/ArkanNibrastama/ecommerce_data_pipeline.git
```

[go to project path]

```bash
sudo docker build -f Dockerfile.Spark . -t spark
sudo docker build -f Dockerfile.Airflow . -t airflow-spark
```

```bash
mkdir logs
chmod -R 777 logs/
```

```bash
sudo docker-compose up -d
```