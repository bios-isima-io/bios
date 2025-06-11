# Deli Framework

Deli is a data integration platform written in Python. The purpose of the platform is to push source (Kafka, S3 etc) data into biOS. It mainly consists of three parts: source or importer, processor, and destination or deliverer. Currently, only one destination type, biOS, is supported.

### Build

To build deli, you have to execute `bash.sh` script. If you are building this for first time it can take a couple of minutes. The script take bios-apps-version as argument. you can check bios-apps-version in `biosapps/app/pom.xml`.

```
cd biosapps/app/deli
./build.sh <bios-app-version>
```

### Install dependency

Deli framework has dependency on some external library that you can either install from `setup.py` or from `requirnment.txt`.

```
pip3 install requirnments.txt
```

### Configuration

You will have to update some parameter of `biosapps/apps/deli/integrations.ini` depends on your needs.


- endpoint: change endpoint to your BiOS server endpoin
- requestDumpEnabled: set it true to enable dumping requests to the log
- recordDumpEnabled: set it true to enable dumping delivering records to the log
- threads: if running on local set thread to true

### Run Deli

To run the webhook, execute `deli_webhook.py` script.

```
./deli_webhook.py
```

To run the kafka, execute `deli_kafka.py` script.

```
./deli_kafka.py
```

To run the s3 execute, `deli_s3.py` script.

```
./deli_s3.py
```

To run the file execute, `deli_file.py` script.
```
./deli_file.py
```
