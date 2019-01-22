# basic python3 image as base
FROM python:3

# copuy all local files to the image
COPY . /

# for testing locally only. 
# Comment this line when deploying
# COPY ./local /app
# ENV DATABASE_URI /app/database.csv

# install external dependancies into enviroment
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# execute algorithm in the container
CMD ["python", "./main.py"]