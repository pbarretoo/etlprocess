FROM python:3.9

ADD sales_diesel.py .
ADD vendas_diesel.xls .


RUN pip install --upgrade pip
RUN pip install psycopg2-binary
RUN pip install xlrd
RUN pip install pandas
RUN pip install datetime

CMD [ "python", "./sales_diesel.py"]




