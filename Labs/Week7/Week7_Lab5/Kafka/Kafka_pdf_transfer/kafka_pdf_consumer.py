import os
from kafka import KafkaConsumer

output_folder = r"C:\Kafka\Output\received_pdfs"
os.makedirs(output_folder, exist_ok=True)

consumer = KafkaConsumer(
    'pdf_files',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)
for message in consumer:
    output_path = os.path.join(output_folder, 'received_pdfs_2.pdf')
    with open(output_path, 'wb') as file:
        file.write(message.value)
    print(f'PDF received successfully and saved in {output_path}')
    break
