import pandas as pd
import sonoff
import requests
import json

from config import dbConfig, switchCloudControl
from db import db
from logger import create_logger

logger = create_logger('action')

devices_file = switchCloudControl['config_file']

with db(**dbConfig) as DB:
    DB.create_schema('action')

    DB.session.execute("""
        CREATE TABLE IF NOT EXISTS action.status (
            status SMALLINT PRIMARY KEY
            , description TEXT
        );

        INSERT INTO action.status (status, description)
        VALUES (-1, 'Cancelled')
            , (0, 'Failed')
            , (1, 'Success')
        ON CONFLICT (status) DO NOTHING;

        CREATE TABLE IF NOT EXISTS action.device_type (
            id SMALLINT PRIMARY KEY
            , name VARCHAR(100)
        );

        INSERT INTO action.device_type (id, name)
        VALUES (1, 'Shelly')
            , (2, 'Sonoff')
        ON CONFLICT (id) DO NOTHING;

        CREATE TABLE IF NOT EXISTS action.action (
            action_id SERIAL PRIMARY KEY
            , created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            , action_time TIMESTAMP WITH TIME ZONE NOT NULL
            , device_type SMALLINT
            , device_id VARCHAR(100) NOT NULL
            , action VARCHAR(100) NOT NULL
            , actioned_at TIMESTAMP WITH TIME ZONE
            , status SMALLINT
            , CONSTRAINT fk_status FOREIGN KEY(status)
                REFERENCES action.status(status)
                ON DELETE SET NULL
                ON UPDATE CASCADE
            , CONSTRAINT fk_device_type FOREIGN KEY(device_type)
                REFERENCES action.device_type(id)
                ON DELETE SET NULL
                ON UPDATE CASCADE
            );
        """)
    DB.session.commit()


# class action:
#     DB = db(**dbConfig)
#     sonoff_account = sonoff.Sonoff(switchCloudControl['username'],
#                                    switchCloudControl['password'],
#                                    switchCloudControl['api_region'])

#     def check_multi_action(self):
#         """
#         If multiple commands for single device at single time, cancel all but the most recent.
#         """

#         self.DB.session.execute("""
#             UPDATE action.action AS a
#             SET status = -1
#             FROM (
#                     SELECT *, ROW_NUMBER() OVER (PARTITION BY action_time, device_id ORDER BY created_at DESC) AS rn
#                     FROM action.action
#                     WHERE status IS NULL
#                 ) AS a2
#             WHERE a2.rn != 1
#                 AND a.action_id = a2.action_id
#             """)
#         self.DB.session.commit()

#     @property
#     def actions(self) -> pd.DataFrame:
#         return pd.read_sql("""
#             SELECT action_id, action_time, device_id, action, status
#             FROM action.action
#             WHERE actioned_at IS NULL
#                 AND action_time <= CURRENT_TIMESTAMP
#                 AND status IS NULL
#             ORDER BY action_time DESC
#         """, self.DB.connection)

#     def execute_todo(self):
#         logger.info('Running Action().excute_todo()')

#         self.check_multi_action()

#         for idx, item in self.actions.iterrows():
#             try:
#                 self.sonoff_account.switch(item.action, item.device_id)

#                 # check of switch is online and has required status
#                 device = self.sonoff_account.get_device(item.device_id)

#                 assert device['online']
#                 assert item.action == device['params']['switch']

#                 item.status = 1  # success
#             except:
#                 item.status = 0  # failed

#             self.DB.session.execute(f"""
#                 UPDATE action.action
#                 SET actioned_at = CURRENT_TIMESTAMP
#                     , status = {item.status}
#                 WHERE action_id = {item.action_id}
#             """)
#         self.DB.session.commit()

class Device_Base:
    device_type: str = None

    def __init__(self, device_id: str = None):
        self.device_id = device_id

        self.get_credentials()

    def get_credentials(self):
        with open(devices_file, 'r') as f:
            devices = json.load(f)
        device = devices[self.device_type][self.device_id]
        self.__dict__.update(device)

   # These methods need defining for each manufacturer
    def log_action(self, status):
        logger.info(
            f"Turning {self.device_type} device {self.device_id} {status}")

    def on(self):
        pass

    def off(self):
        pass

    def toggle(self):
        pass


class Sonoff(Device_Base):
    device_type = 'Sonoff'

    def __init__(self, device_id: str = None):
        super().__init__(device_id)
        self.get_credentials()

        self.sonoff_account = sonoff.Sonoff(self.username,
                                            self.password,
                                            self.api_region)

    @property
    def status(self):
        device = self.sonoff_account.get_device(item.device_id)
        return device['params']['switch']

    def turn(self, status: str):
        assert status in ['on', 'off']

        self.log_action(status)
        self.sonoff_account.switch(status, self.device_id)
        assert self.status == status

    def on(self):
        self.turn('on')

    def off(self):
        self.turn('off')

    def toggle(self):
        if self.status == 'on':
            self.off()
        else:
            self.on()


class Shelly(Device_Base):
    device_type = 'Shelly'

    @property
    def status(self):
        r = requests.get(self.endpoint + 'relay/0',
                         auth=(self.username, self.password))
        return r.json()['ison']

    def turn(self, status: str):
        assert status in ['on', 'off', 'toggle']

        statusStart = self.status

        self.log_action(status)
        r = requests.post(self.endpoint + 'relay/0', data={'turn': status},
                          auth=(self.username, self.password))

        if status == 'toggle':
            assert statusStart != self.status
        else:
            assert self.status == (status == 'on')

        return r.json()

    def on(self):
        return self.turn('on')

    def off(self):
        return self.turn('off')

    def toggle(self):
        return self.turn('toggle')


class action:
    DB = db(**dbConfig)

    Status = DB.lookup_table('status', 'action', index='status')

    def check_multi_action(self):
        """
        If multiple commands for single device at single time, cancel all but the most recent.
        """

        self.DB.session.execute(f"""
            UPDATE action.action AS a
            SET status = {self.Status.Cancelled.value}
            FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY action_time, device_id ORDER BY created_at DESC) AS rn
                    FROM action.action
                    WHERE status IS NULL
                ) AS a2
            WHERE a2.rn != 1
                AND a.action_id = a2.action_id
            """)
        self.DB.session.commit()

    @property
    def actions(self) -> pd.DataFrame:
        return pd.read_sql("""
            SELECT action_id
                , action_time
                , device_type
                , device_id
                , action
                , status
            FROM action.action
            WHERE actioned_at IS NULL
                AND action_time <= CURRENT_TIMESTAMP
                AND status IS NULL
            ORDER BY action_time DESC
        """, self.DB.connection)

    @staticmethod
    def create_device(device_type: str):
        if device_type == 'Sonoff':
            return Sonoff
        elif device_type == 'Shelly':
            return Shelly

    def execute_todo(self):
        logger.info('Running Action().excute_todo()')

        self.check_multi_action()

        for idx, item in self.actions.iterrows():
            try:
                device = self.create_device(item.device_type)(item.device_id)

                getattr(device, item.action)()

                item.status = self.Status.Success.value
            except:
                item.status = self.Status.Failed.value

            self.DB.session.execute(f"""
                    UPDATE action.action
                    SET actioned_at = CURRENT_TIMESTAMP
                        , status = {item.status}
                    WHERE action_id = {item.action_id}
                """)
            self.DB.session.commit()
