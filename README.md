# GOOGLE FILE SYSTEM (GFS)
## PROJECT SUBMITTED AS A PART OF OPERATING SYSTEMS(PG) COURSE

# setup project
```bash
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

# create some directories for chunkserver path
```bash
rm -r temp/*
mkdir -p temp/ck9010
mkdir -p temp/ck9011
mkdir -p temp/ck9012
mkdir -p temp/ck9013
```

# Running the project
```bash
python master.py

python chunkserver.py 9010
python chunkserver.py 9011
python chunkserver.py 9012
python chunkserver.py 9013

python client.py 
```