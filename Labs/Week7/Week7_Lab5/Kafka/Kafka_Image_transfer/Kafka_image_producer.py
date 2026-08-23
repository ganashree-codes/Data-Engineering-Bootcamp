from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
filename = r"C:\Users\ganas\Pictures\Screenshots\Screenshot 2026-08-18 142258.png"
with open(filename, 'rb') as file:
    image_data = file.read()
producer.send('images', value=image_data)
producer.flush()
print('Image sent successfully')

