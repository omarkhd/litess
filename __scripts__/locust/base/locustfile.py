import random
import uuid

import locust

import settings


CAR_MODELS = (
    ('bmw', ('m1', 'm2', 'x4', 'm8')),
    ('audi', ('a3', 'q5', 'tt', 'r8')),
    ('tesla', ('3', 's', 'x', 'y')),
    ('porsche', ('911', 'macan', 'panamera'))
)
CAR_YEARS = tuple(range(1989, 2021))
WHEEL_AIR_PRESSURES = tuple(range(25, 40))
WHEEL_COMPANIES = ('goodyear', 'bridgestone', 'michelin', 'pirelli')


logger = settings.get_logger()


class WorkerUser(locust.HttpUser):
    wait_time = locust.between(0, 0.5)
    host = settings.WORKER_URL

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cfg = settings.Manager.get_instance()
        self.prepare_schema()
        self.created_cars = list()

    @locust.task(6)
    def insert_car(self) -> None:
        if not self.is_enabled():
            return

        # Stop inserting if full capacity.
        if self.is_full():
            return

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
            self.created_cars.append(car['id'])
            for wheel in wheels:
                query = 'insert into car_wheel (car_id, wheel_id) ' \
                    'values ("{}", "{}")'.format(car['id'], wheel['id'])
                r = self.client.post('/exec', json=dict(sql=query), name='insert')
                if r.status_code != 200:
                    logger.error('Failed to persist car wheel relationship')
        else:
            logger.error('Failed to persist car %s: %s', car['id'], r.text)

    @locust.task(1)
    def delete_car(self) -> None:
        if len(self.created_cars) < 1:
            return
        pick = random.choice(self.created_cars)
        # Wheel rows deletion.
        query = 'delete from wheel where id in (' \
            'select cw.wheel_id from car_wheel cw ' \
            'where cw.car_id = "{}")'.format(pick)
        r = self.client.post('/exec', json=dict(sql=query), name='delete')
        if r.status_code != 200:
            logger.error('Failed to delete wheels: %s', r.text)
        # Relationship rows deletion.
        query = 'delete from car_wheel where car_id = "{}"'.format(pick)
        r = self.client.post('/exec', json=dict(sql=query), name='delete')
        if r.status_code != 200:
            logger.error('Failed to delete car wheel relationship: %s', r.text)
        # Car row deletion.
        query = 'delete from car where id = "{}"'.format(pick)
        r = self.client.post('/exec', json=dict(sql=query), name='delete')
        if r.status_code != 200:
            logger.error('Failed to delete car: %s', r.text)
        self.created_cars.remove(pick)

    @locust.task(1)
    def query_cars(self) -> None:
        if not self.is_enabled():
            return

        query = 'select c.id, count(w.id) from car_wheel cw ' \
            'inner join car c on cw.car_id = c.id ' \
            'inner join wheel w on cw.wheel_id = w.id ' \
            'group by c.id'
        r = self.client.post('/query', json=dict(sql=query), name='query')
        if r.status_code != 200:
            logger.error('Failed to query cars: %s', r.text)
            return

        # This task also saves count to state.
        count = r.json().get('rows_affected')
        self.cfg.state.set('count', count)
        logger.info('Query returned %s cars', count)

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
            '  foreign key (car_id) references car (id) on delete restrict,'
            '  foreign key (wheel_id) references wheel (id) on delete restrict'
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

    def is_enabled(self) -> bool:
        cfg, _ = self.cfg.get()
        if cfg.get('enabled') is False:
            return False
        return True

    def is_full(self) -> bool:
        cfg, _ = self.cfg.get()
        count = self.cfg.state.get('count')
        capacity = cfg.get('capacity')
        if None in (count, capacity):
            return False
        return count >= capacity
