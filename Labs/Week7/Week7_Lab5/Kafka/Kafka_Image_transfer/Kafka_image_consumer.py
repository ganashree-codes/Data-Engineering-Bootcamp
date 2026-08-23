import os
import os

from kafka import KafkaConsumer

output_folder = r"C:\Kafka\Output\received_images"
os.makedirs(output_folder, exist_ok=True)

consumer = KafkaConsumer(
    'images',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)
for message in consumer:
    output_path = os.path.join(output_folder, 'received_image.jpg')
    with open(output_path, 'wb') as file:
        file.write(message.value)
    print(f'Image received successfully and saved in {output_path}')
    break
