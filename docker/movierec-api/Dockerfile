FROM python:3.8

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm -f /tmp/requirements.txt

COPY app.py fetch_data.py /
RUN python /fetch_data.py
EXPOSE 5000

ENTRYPOINT ["python"]
CMD ["/app.py"]
