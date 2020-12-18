import logging
import os
import random
import uuid

import locust


WORKER_URL = os.getenv('WORKER_URL')
if WORKER_URL is None:
    raise RuntimeError('Undefined WORKER_URL')


logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


CAR_MODELS = (
    ('bmw', ('m1', 'm2', 'x4', 'm8')),
    ('audi', ('a3', 'q5', 'tt', 'r8')),
    ('tesla', ('3', 's', 'x', 'y')),
    ('porsche', ('911', 'macan', 'panamera'))
)
CAR_YEARS = tuple(range(1989, 2021))
WHEEL_AIR_PRESSURES = tuple(range(25, 40))
WHEEL_COMPANIES = ('goodyear', 'bridgestone', 'michelin', 'pirelli')


class WorkerMonkey(locust.HttpUser):
    wait_time = locust.between(0, 1)
    host = WORKER_URL

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prepare_schema()

    @locust.task(1)
    def insert_car(self) -> None:
        wheels = list()
        for _ in range(0, 4):
            wheel = self.create_wheel()
            query = 'insert into wheel (id, air_pressure, company) ' \
                'values ("{id}", {air_pressure}, "{company}")'.format(**wheel)
            r = self.client.post('/exec', json=dict(sql=query), name='insert')
            if r.status_code == 200:
                wheels.append(wheel)
            else:
                logger.error('Failed to persist wheel %s: %s', wheel['id'], r.text)
        car = self.create_car()
        query = 'insert into car (id, make, model, year) ' \
            'values ("{id}", "{make}", "{model}", {year})'.format(**car)
        r = self.client.post('/exec', json=dict(sql=query), name='insert')
        if r.status_code == 200:
            for wheel in wheels:
                query = 'insert into car_wheel (car_id, wheel_id) ' \
                    'values ("{0}", "{1}")'.format(car['id'], wheel['id'])
                r = self.client.post('/exec', json=dict(sql=query), name='insert')
                if r.status_code != 200:
                    logger.error('Failed to persist car wheel relationship')
        else:
            logger.error('Failed to persist car %s: %s', car['id'], r.text)

    @locust.task(5)
    def query_cars(self) -> None:
        query = 'select * from car_wheel cw ' \
            'inner join car c on cw.car_id = c.id ' \
            'inner join wheel w on cw.wheel_id = w.id'
        r = self.client.post('/query', json=dict(sql=query), name='query')
        if r.status_code != 200:
            logger.error('Failed to query cars: %s', r.text)
            return
        logger.info('Query returned %s cars', r.json().get('rows_affected'))

    def prepare_schema(self) -> None:
        queries = [
            'create table if not exists car ('
            '    id text primary key,'
            '    make text,'
            '    model text,'
            '    year integer'
            ')',
            'create index if not exists user_idx_car_1 on car (make)',
            'create index if not exists user_idx_car_2 on car (model)',
            'create table if not exists wheel ('
            '    id text primary key,'
            '    air_pressure integer,'
            '    company text'
            ')',
            'create index if not exists user_idx_wheel_1 on wheel(air_pressure)',
            'create table if not exists car_wheel ('
            '  car_id text,'
            '  wheel_id text,'
            '  primary key (car_id, wheel_id),'
            '  foreign key (car_id) references car (id),'
            '  foreign key (wheel_id) references wheel (id)'
            ')'
        ]
        for query in queries:
            logger.info('Executing SQL: %s', query)
            r = self.client.post('/exec', json={'sql': query}, name='schema')
            if r.status_code != 200:
                logger.info('Failed to execute schema query: %s', r.text)

    @staticmethod
    def create_car() -> dict:
        make, models = random.choice(CAR_MODELS)
        return {
            'id': str(uuid.uuid4()),
            'make': make,
            'model': random.choice(models),
            'year': random.choice(CAR_YEARS)
        }

    @staticmethod
    def create_wheel() -> dict:
        return {
            'id': str(uuid.uuid4()),
            'air_pressure': random.choice(WHEEL_AIR_PRESSURES),
            'company': random.choice(WHEEL_COMPANIES)
        }
