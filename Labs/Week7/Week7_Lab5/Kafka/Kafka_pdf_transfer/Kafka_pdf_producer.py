from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
filename = r"C:\Users\ganas\Desktop\Data Engineering\Week7\Week7_Lab4.pdf"
with open(filename, 'rb') as file:
    data = file.read()
producer.send('pdf_files', value=data)
producer.flush()
print('PDF sent successfully')
