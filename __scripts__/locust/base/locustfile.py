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

    @locust.task(4)
    def insert_car(self) -> None:
        if not self.is_enabled():
            return

        # Stop inserting if full capacity.
        if self.is_full():
            return

        car = self.create_car()
        query = "insert into car (id, make, model, year) " \
            "values ('{id}', '{make}', '{model}', {year})".format(**car)
        r = self.client.post('/exec', json=dict(sql=query), name='insert_car')
        if r.status_code != 200:
            logger.error('Failed to insert car: %s', r.text)
            return
        self.created_cars.append(car['id'])

        wheels = list()
        for _ in range(0, 4):
            wheel = self.create_wheel()
            query = "insert into wheel (id, air_pressure, company) " \
                "values ('{id}', {air_pressure}, '{company}')".format(**wheel)
            r = self.client.post('/exec', json=dict(sql=query), name='insert_wheel')
            if r.status_code != 200:
                logger.error('Failed to insert wheel %s: ', r.text)
                continue
            wheels.append(wheel)

        for wheel in wheels:
            query = "insert into car_wheel (car_id, wheel_id) " \
                "values ('{}', '{}')".format(car['id'], wheel['id'])
            r = self.client.post('/exec', json=dict(sql=query), name='insert_car_wheel')
            if r.status_code != 200:
                logger.error('Failed to insert car wheel relationship: %s', r.text)

    @locust.task(25)
    def update_car(self) -> None:
        if not self.is_enabled():
            return

        if len(self.created_cars) == 0:
            return
        pick = random.choice(self.created_cars)
        year = random.choice(CAR_YEARS)
        query = "update car set year = {} where id = '{}'".format(year, pick)
        r = self.client.post('/exec', json=dict(sql=query), name='update_car')
        if r.status_code != 200:
            logger.error('Failed to update car: %s', r.text)

    @locust.task(50)
    def update_wheels(self) -> None:
        if not self.is_enabled():
            return

        pressure = 35
        company = random.choice(WHEEL_COMPANIES)
        make, _ = random.choice(CAR_MODELS)
        query_up = "update wheel " \
            "set air_pressure = air_pressure + 1 " \
            "where air_pressure < {} and company = '{}' and id in (" \
            "    select cw.wheel_id from car_wheel cw " \
            "    inner join car c on cw.car_id = c.id " \
            "    where make = '{}')".format(pressure, company, make)
        query_down = "update wheel " \
            "set air_pressure = air_pressure - 3 " \
            "where air_pressure >= {} and company = '{}' and id in (" \
            "    select cw.wheel_id from car_wheel cw " \
            "    inner join car c on cw.car_id = c.id " \
            "    where make = '{}')".format(pressure, company, make)
        query = random.choice((query_up, query_down))
        r = self.client.post('/exec', json=dict(sql=query), name='update_wheels')
        if r.status_code != 200:
            logger.error('Failed to update wheels: %s', r.text)

    @locust.task(2)
    def delete_car(self) -> None:
        if not self.is_enabled():
            return

        if len(self.created_cars) == 0:
            return

        pick = random.choice(self.created_cars)
        # Wheel rows deletion.
        query = "delete from wheel " \
            "where id in (" \
            "    select cw.wheel_id from car_wheel cw " \
            "    where cw.car_id = '{}')".format(pick)
        r = self.client.post('/exec', json=dict(sql=query), name='delete_wheels')
        if r.status_code != 200:
            logger.error('Failed to delete wheels: %s', r.text)
            return
        # Car wheels relationship rows deletion.
        query = "delete from car_wheel where car_id = '{}'".format(pick)
        r = self.client.post('/exec', json=dict(sql=query), name='delete_car_wheels')
        if r.status_code != 200:
            logger.error('Failed to delete car wheels relationships: %s', r.text)
            return
        # Car row deletion.
        query = "delete from car where id = '{}'".format(pick)
        r = self.client.post('/exec', json=dict(sql=query), name='delete_car')
        if r.status_code != 200:
            logger.error('Failed to delete car: %s', r.text)
            return
        self.created_cars.remove(pick)

    @locust.task(1)
    def join_all_cars(self) -> None:
        if not self.is_enabled():
            return

        query = "select c.*, w.* from car_wheel cw " \
            "inner join car c on cw.car_id = c.id " \
            "inner join wheel w on cw.wheel_id = w.id"
        r = self.client.post('/query', json=dict(sql=query), name='join_all_cars')
        if r.status_code != 200:
            logger.error('Failed to join all cars: %s', r.text)

    @locust.task(10)
    def query_cars(self) -> None:
        if not self.is_enabled():
            return

        if len(self.created_cars) == 0:
            return
        values = ', '.join("'{}'".format(c) for c in self.created_cars)
        air_pressure = random.choice(WHEEL_AIR_PRESSURES)
        company = random.choice(WHEEL_COMPANIES)
        query = "select distinct c.* from car c " \
            "inner join car_wheel cw on c.id = cw.car_id " \
            "inner join wheel w on cw.wheel_id = w.id " \
            "where w.air_pressure > {} and w.company != '{}' " \
            "and c.id in ({})".format(air_pressure, company, values)
        r = self.client.post('/query', json=dict(sql=query), name='query_cars')
        if r.status_code != 200:
            logger.error('Failed to query cars: %s', r.text)
            return

    @locust.task(5)
    def query_car(self) -> None:
        if not self.is_enabled():
            return

        if len(self.created_cars) == 0:
            return
        pick = random.choice(self.created_cars)
        query = "select c.*, w.* from car c " \
            "inner join car_wheel cw on c.id = cw.car_id " \
            "inner join wheel w on cw.wheel_id = w.id " \
            "where c.id = '{}'".format(pick)
        r = self.client.post('/query', json=dict(sql=query), name='query_car')
        if r.status_code != 200:
            logger.error('Failed to query car: %s', r.text)

    @locust.task(1)
    def select_all_cars(self) -> None:
        self.select_all_and_set_count('car')
        self.log_counts()

    @locust.task(1)
    def select_all_wheels(self) -> None:
        self.select_all_and_set_count('wheel')

    @locust.task(1)
    def select_all_car_wheels(self) -> None:
        self.select_all_and_set_count('car_wheel')

    def select_all_and_set_count(self, table) -> None:
        if not self.is_enabled():
            return
        query = "select * from {}".format(table)
        r = self.client.post('/query', json=dict(sql=query), name='select_all_{}'.format(table))
        if r.status_code != 200:
            logger.error('Failed to select from table: %s', r.text)
            return
        # This task also keeps track of table counts.
        count = r.json().get('rows_affected')
        self.cfg.state.set('{}_count'.format(table), count)

    def prepare_schema(self) -> None:
        if self.cfg.state.get('schema_loaded'):
            logger.info('Schema previously loaded, skipping')
            return

        queries = [
            "create table if not exists car ("
            "    id text primary key,"
            "    make text,"
            "    model text,"
            "    year integer"
            ")",
            "create index if not exists user_idx_car_1 on car (make)",
            "create index if not exists user_idx_car_2 on car (model)",
            "create table if not exists wheel ("
            "    id text primary key,"
            "    air_pressure integer,"
            "    company text"
            ")",
            "create index if not exists user_idx_wheel_1 on wheel(air_pressure)",
            "create table if not exists car_wheel ("
            "    car_id text,"
            "    wheel_id text,"
            "    primary key (car_id, wheel_id),"
            "    foreign key (car_id) references car (id) on delete restrict,"
            "    foreign key (wheel_id) references wheel (id) on delete restrict"
            ")"
        ]
        for query in queries:
            r = self.client.post('/exec', json={'sql': query}, name='schema')
            if r.status_code != 200:
                logger.info('Failed to execute schema query: %s', r.text)
                return

        logger.info('Schema successfully loaded')
        self.cfg.state.set('schema_loaded', True)

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
        count = self.cfg.state.get('car_count')
        capacity = cfg.get('capacity')
        if None in (count, capacity):
            return False
        return count >= capacity

    def log_counts(self) -> None:
        logger.info('[car: {car}] [wheel: {wheel}] [car_wheel: {car_wheel}]'.format(**{
            'car': self.cfg.state.get('car_count'),
            'wheel': self.cfg.state.get('wheel_count'),
            'car_wheel': self.cfg.state.get('car_wheel_count')
        }))
