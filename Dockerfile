# basic python3 image as base
FROM python:3

# add main file to the docker image
ADD main.py /

# add local dependencies
ADD fileIO.py /
ADD algorithm.py /

# install external dependancies
RUN pip install pandas

# execute algorithm in the container
CMD ["python", "./main.py"]