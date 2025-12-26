from tendo import singleton
import sys

try:
    me = singleton.SingleInstance()
except singleton.SingleInstanceException:
    print("Another instance is already running. Exiting...")
    sys.exit(1)

import datetime
import re
import socket
import configparser
import time
import json
import threading
import queue
import logging
from _thread import *
import pytz
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from logger_setup import init_logger_with_monitor
from datetime import timezone, timedelta, datetime
from email.message import EmailMessage
import smtplib
from market_data_pb2 import MarketData, MarketDataBatch, MarketDataUpdate, MarketDataUpdateType, NewsItem
from concurrent.futures import ThreadPoolExecutor

# Configuration setup
config = configparser.ConfigParser()
config.read('config.ini')

listen_ip = config['tcp_server_2']['listen_ip']
listen_port = int(config['tcp_server_2']['listen_port'])
kafka_ip = config['kafka_server']['kafka_ip']
kafka_port = int(config['kafka_server']['kafka_port'])
message_format = config['message_format']['format']

logger = init_logger_with_monitor()

ThreadCount = 0


def parse_fix_message_to_proto(message: str) -> MarketDataUpdate:
    message = message.replace("CPR ", "5051=")
    umd = MarketDataUpdate()
    umd.type = MarketDataUpdateType.UPDATE
    md = MarketData()
    fields_str = ""
    try:
        for pair in message.strip().split(';'):
            if '=' not in pair:
                continue

            tag, value = pair.split('=', 1)
            field_name = f"f_{tag}"
            fields_str = field_name + "=" + value

            if not hasattr(md, field_name):
                continue

            field_descriptor = md.DESCRIPTOR.fields_by_name[field_name]

            try:
                if field_descriptor.type == field_descriptor.TYPE_DOUBLE:
                    if '#CLEAR*' in value:
                        continue
                    if "+" in value:
                        value = value.replace("+", "")
                    setattr(md, field_name, float(value))
                elif field_descriptor.type in (field_descriptor.TYPE_INT32, field_descriptor.TYPE_INT64):
                    try:
                        if '#CLEAR*' in value:
                            continue
                        setattr(md, field_name, int(value))
                    except ValueError:
                        continue
                else:
                    setattr(md, field_name, value)

            except ValueError:
                print(f"Warning: Cannot convert value '{value}' for field '{field_name}' {md.f_5051}")
                logger.error(f"Warning: Cannot convert value '{value}' for field '{field_name}'")
                continue
    except Exception as err:
        print(fields_str, err)
        logger.error(f"{fields_str} =>{err}")

    if hasattr(md, 'f_18') and md.HasField('f_18'):
        md.f_18 = md.f_18
    if hasattr(md, 'f_10012') and md.HasField('f_10012'):
        md.f_18 = md.f_10012

    ist_timezone = pytz.timezone('Asia/Kolkata')
    # Get the current UTC time
    utc_now = datetime.now(ist_timezone)
    now_epoch = int(utc_now.timestamp())

    # if hasattr(md, 'f_2') and md.HasField('f_2') and not md.HasField('f_10'):
    #     md.f_10 = now_epoch
    if hasattr(md, 'f_5') and md.HasField('f_5'):
        md.f_12 = now_epoch
    if hasattr(md, 'f_4') and md.HasField('f_4'):
        md.f_11 = now_epoch
    if hasattr(md, 'f_10') and not md.HasField('f_10'):
        md.f_10 = now_epoch
    if md.HasField('f_10'):
        md.f_10 = now_epoch

    umd.market_data.CopyFrom(md)
    return umd


def convert_string_to_json(data_str):
    data_str = data_str.replace("CPR ", "5051=")

    json_obj = {}
    for item in data_str.split(";"):
        item = item.replace('\r', '')
        if "=" not in item:
            continue
        k, v = item.split("=", 1)
        f_key = f"f_{k}"
        try:
            json_obj[f_key] = float(v) if "." in v else int(v)
        except ValueError:
            json_obj[f_key] = v

    if 'f_10012' in json_obj:
        json_obj['f_18'] = json_obj['f_10012']
    if 'f_18' in json_obj:
        json_obj['f_18'] = json_obj['f_18']

    ist_timezone = pytz.timezone('Asia/Kolkata')
    # Get the current UTC time
    utc_now = datetime.now(ist_timezone)
    now_epoch = int(utc_now.timestamp())

    if 'f_4' in json_obj:
        json_obj['f_11'] = now_epoch
    if 'f_5' in json_obj:
        json_obj['f_12'] = now_epoch
    # if 'f_2' in json_obj and 'f_10' not in json_obj:
    #     json_obj['f_10'] = now_epoch
    if 'f_10' not in json_obj:
        json_obj['f_10'] = now_epoch
    if 'f_10' in json_obj:
        json_obj['f_10'] = now_epoch

    return json_obj


class ProducerWorker(threading.Thread):
    def __init__(self, topic, hostname, worker_queue, worker_id, counter):
        super().__init__(daemon=True)
        self.topic = topic
        self.hostname = hostname
        self.queue = worker_queue
        self.worker_id = worker_id
        self.counter = counter
        self.running = True

        self.producer = Producer({
            'bootstrap.servers': f'{kafka_ip}:{kafka_port}',
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 524288,
            'queue.buffering.max.ms': 1,
            'batch.num.messages': 10000,
            'batch.size': 1048576,
            'linger.ms': 5,
            'compression.type': 'lz4',
            'acks': 1,
            'socket.timeout.ms': 3000,
            'enable.idempotence': False
        })

    def run(self):
        TARGET_BATCH_COUNT = 500
        batch = []
        last_flush = time.time()

        try:
            while self.running or not self.queue.empty():
                try:
                    msg_str = self.queue.get(timeout=0.1)
                except queue.Empty:
                    # flush partial batch on idle
                    if batch:
                        self.send_batch(batch)
                        batch.clear()
                        last_flush = time.time()
                    continue

                try:
                    if message_format.lower() == 'protobuf':
                        message = parse_fix_message_to_proto(msg_str).SerializeToString()
                    else:
                        message = json.dumps(convert_string_to_json(msg_str))

                    batch.append(message)

                    if len(batch) >= TARGET_BATCH_COUNT or (time.time() - last_flush) > 0.05:
                        self.send_batch(batch)
                        batch.clear()
                        last_flush = time.time()

                finally:
                    self.queue.task_done()

        finally:
            # final flush
            if batch:
                self.send_batch(batch)

            self.producer.flush(10)
            logger.info(f"[Worker-{self.worker_id}] stopped cleanly")

    def send_batch(self, batch):
        for message in batch:
            try:
                self.producer.produce(
                    topic=self.topic,
                    value=message,
                    headers=[('hostname', self.hostname.encode('utf-8'))]
                )
                self.counter['sent'] += 1
            except Exception as e:
                logger.error(
                    f"[Worker-{self.worker_id}]- {self.topic} Kafka error: {e}"
                )
        self.producer.poll(0)

    def stop(self):
        self.running = False


# Manages one client connection and one Kafka topic.
class ClientHandler:
    def __init__(self, client_socket, address, topic, hostname, num_workers=24):
        self.client_socket = client_socket
        self.address = address
        self.topic = topic
        self.hostname = hostname
        self.running = True
        self.queue = queue.Queue(maxsize=100000)
        self.max_queue_size = 50000
        self.last_warning_time = 0
        self.warning_interval = 30

        self.counter = {'received': 0, 'sent': 0}

        self.num_workers = num_workers
        self.worker_queues = [queue.Queue(maxsize=250000) for _ in range(num_workers)]
        self.workers = []

        # Splits the load to multiple Kafka producers (ProducerWorker) for that topic.
        for i in range(num_workers):
            worker = ProducerWorker(self.topic, self.hostname, self.worker_queues[i], i, self.counter)
            worker.start()
            self.workers.append(worker)

        self.dispatcher_thread = threading.Thread(target=self.dispatcher, daemon=True)
        self.dispatcher_thread.start()

        self.reporter_thread = threading.Thread(target=self.reporter, daemon=True)
        self.reporter_thread.start()

    def enqueue(self, message):
        # self.queue.put_nowait(message)
        self.queue.put(message)
        self.counter['received'] += 1
        return True

    def dispatcher(self):
        idx = 0
        while self.running or not self.queue.empty():
            try:
                msg = self.queue.get(timeout=0.01)
                self.queue.task_done()

                try:
                    self.worker_queues[idx].put_nowait(msg)
                except queue.Full:
                    logger.warning(f"Worker-{idx} queue full for {self.topic}")

                idx = (idx + 1) % self.num_workers

            except queue.Empty:
                continue

    def reporter(self):
        while self.running:
            time.sleep(2)
            received = self.counter['received']
            sent_count = self.counter['sent']
            remaining_msg = received - sent_count
            logger.info(
                f"[STATS] Topic: {self.topic} | Received: {self.counter['received']} | Sent: {self.counter['sent']} | Remaining: {remaining_msg}")

    def stop(self):
        self.running = False
        self.queue.join()
        for wq in self.worker_queues:
            wq.join()
        for w in self.workers:
            w.stop()
        for w in self.workers:
            w.join()


MAX_BUFFER_SIZE = 65536
LOGIN_TIMEOUT = 10  # seconds
thread_lock = threading.Lock()


def multi_threaded_client(connection):
    global ThreadCount
    client_address = connection.getpeername()
    client_topic = None
    handler = None
    buffer = b""
    delimiter_pattern = re.compile(rb'[\r\n]+')

    try:
        connection.settimeout(60.0)
        login_deadline = time.time() + LOGIN_TIMEOUT
        login_received = False

        while True:
            try:
                data = connection.recv(4096)
            except socket.timeout:
                logger.warning(f"Timeout from client {client_address}")
                break

            if not data:
                logger.info(f"Client at {client_address} closed connection.")
                continue

            buffer += data

            if len(buffer) > MAX_BUFFER_SIZE:
                logger.warning(f"Buffer overflow from {client_address}, clearing.")
                buffer = b""

            while True:
                m = delimiter_pattern.search(buffer)
                if not m:
                    break

                line_bytes, buffer = buffer[:m.start()], buffer[m.end():]

                try:
                    line = line_bytes.decode('utf-8', errors='ignore').strip()
                except Exception as e:
                    logger.error(f"Error decoding line from {client_address}: {e}")
                    continue

                if not line or line.startswith("MONPR"):
                    continue

                if line.startswith("LOGIN "):
                    parts = line.split()
                    if len(parts) >= 2 and ";" in parts[1]:
                        client_topic = parts[1].split('\\')[0]
                        hostname = parts[1].split(";", 1)[1].split("\\")[-1]
                        logger.info(f"Kafka topic set to: {client_topic}")
                        handler = ClientHandler(connection, client_address, client_topic, hostname)
                        connection.sendall(b"1;Ok;1000025=5;\r\n")
                        login_received = True
                    continue

                if not login_received and time.time() > login_deadline:
                    logger.warning(f"Client {client_address} did not login in time.")
                    return

                if handler and not line.startswith("LOGIN "):
                    handler.enqueue(line)
                else:
                    logger.debug(f"Skipping LOGIN line for {client_address}: {line}")

    except Exception as e:
        logger.error(f"Error in client thread {client_address}: {e}")

    finally:
        if handler:
            handler.stop()
        connection.close()


def listener():
    global ThreadCount
    executor = ThreadPoolExecutor(max_workers=200)

    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        server_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        server_socket.bind((listen_ip, listen_port))
        server_socket.listen()
        logger.info(f"Listening on {listen_ip}:{listen_port}")
    except Exception as e:
        logger.error(f"Failed to initialize server socket: {str(e)}")
        return

    while True:
        try:
            client_socket, address = server_socket.accept()
            logger.info(f"Client connected: {address}")
            client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            client_socket.settimeout(30.0)

            with thread_lock:
                ThreadCount += 1
            logger.info(f"Clients connected: {ThreadCount}")

            executor.submit(multi_threaded_client, client_socket)

        except Exception as e:
            logger.error(f"Error accepting client: {e}")


def send_mail(subject, details):
    host = config['mail_sender']['host']
    port = int(config['mail_sender']['port'])
    from_addr = config['mail_sender']['from_email']
    to_addr = config['mail_sender']['to_email']

    msg = EmailMessage()
    disclaimer_message = "Disclaimer: Please do not reply to this email. This email was automatically generated."

    msg['From'] = from_addr
    # msg['To'] = to_addr
    msg['To'] = '; '.join(config.get('Recipients', r) for r in config.options('Recipients'))
    msg["subject"] = subject

    html_content = f"<p style='color: black;'>{details}</p>\n\n" \
                   f"<p style='color: blue;'>{disclaimer_message}</p>"

    msg.add_alternative(html_content, subtype='html')

    try:
        with smtplib.SMTP(host, port) as server:
            server.send_message(msg)
        print("Mail Sent Successfully")

    except Exception as e:
        print(f"Failed to send email: {e}")


def main():
    listener_thread = threading.Thread(target=listener, daemon=True)
    listener_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Application stopped")


if __name__ == "__main__":
    main()
