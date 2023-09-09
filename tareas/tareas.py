import csv
import os
from celery import Celery
from celery.signals import task_postrun

celery_app = Celery('tasks', broker='redis://127.0.0.1:6379/0')


@celery_app.task(name='log.registrar')
def enviar_log(datos_log):
    file_exists = os.path.exists('data_log.csv')

    with open('data_log.csv', 'a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=datos_log.keys())

        if not file_exists:
            writer.writeheader()

        writer.writerow(datos_log)
