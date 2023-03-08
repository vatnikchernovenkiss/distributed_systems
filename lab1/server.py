import pandas as pd
import pika
import redis
import multiprocessing
import time
import errno

r1_nodes = []
r2_nodes = []
r1_nodes.append(redis.Redis(host='127.0.0.1', port=6379)) 
r1_nodes.append(redis.Redis(host='127.0.0.1', port=6378)) 
r1_nodes.append(redis.Redis(host='127.0.0.1', port=6377)) 

r2_nodes.append(redis.Redis(host='127.0.0.1', port=6376)) 
r2_nodes.append(redis.Redis(host='127.0.0.1', port=6375)) 
r2_nodes.append(redis.Redis(host='127.0.0.1', port=6374)) 

def get_values(keys, values, r):
    while True:
        try:
            vals = r.mget(keys)
            for val in vals:
                values.append(val)
        except redis.ConnectionError as e:
            continue
        break


def callback(ch, method, properties, body):
    body = body.decode('UTF-8')
    beg_date, end_date, step = body.split(' ')
    beg_date = (pd.to_datetime(beg_date, format='%Y-%m-%d')).strftime("%Y-%m-%dT%H:%M:%S")
    end_date = (pd.to_datetime(end_date, format='%Y-%m-%d')).strftime("%Y-%m-%dT%H:%M:%S")
    cur_date = beg_date
    need_dates = []
    step = int(step)
    while cur_date <= end_date:
        need_dates.append(cur_date)
        cur_date = (pd.to_datetime(cur_date, format='%Y-%m-%dT%H:%M:%S') + pd.DateOffset(minutes=step)).strftime("%Y-%m-%dT%H:%M:%S")
    
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    values_1 = manager.list()
    values_2 = manager.list()
    r1_pos = 0
    r2_pos = 0
    p1 = multiprocessing.Process(target=get_values, args=(need_dates, values_1, r1_nodes[r1_pos]))
    p1.start()
    p2 = multiprocessing.Process(target=get_values, args=(need_dates, values_2, r2_nodes[r2_pos]))
    p2.start()
    is_done = False

    while is_done == False:
        p1.join(timeout=3)
        if p1.is_alive() == True:
            print('cur r1 pos =', r1_pos + 1)
            p1.terminate()
            r1_pos = r1_pos + 1
            if r1_pos == 3:
                print('ERROR: UNABLE TO ACCESS ANY OF THE FIRST DATABASE NODES')
                break
            p1 = multiprocessing.Process(target=get_values, args=(need_dates, values_1, r1_nodes[r1_pos]))
            p1.start()
        else:
            is_done = True
            
    is_done = False
    while is_done == False:
        p2.join(timeout=3)
        if p2.is_alive() == True:
            print('cur r2 pos =', r2_pos + 1)
            p2.terminate()
            r2_pos = r2_pos + 1
            if r2_pos == 3:
                print('ERROR: UNABLE TO ACCESS ANY OF THE SECOND DATABASE NODES')
                break
            p2 = multiprocessing.Process(target=get_values, args=(need_dates, values_2, r2_nodes[r2_pos]))
            p2.start()
        else:
            is_done = True
       
    list_1 = list(map(lambda x:  x.decode('utf-8'), list(filter(lambda x: x is not None, values_1))))
    list_2 = list(map(lambda x:  x.decode('utf-8'), list(filter(lambda x: x is not None, values_2))))

    res_l = []
    for date, stat in zip(need_dates, list_1 + list_2):
        res_l.append(date +': ' + stat)
    response = '\n'.join(res_l)
    ch.basic_publish(exchange='', routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)


connection = pika.BlockingConnection(pika.ConnectionParameters('127.0.0.1'))
channel = connection.channel()
queue = 'rcc_queue'
channel.queue_declare(queue=queue)
channel.basic_qos(prefetch_count=1) 
channel.basic_consume(queue=queue, on_message_callback=callback)
channel.start_consuming()
