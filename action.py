import pandas as pd
import sonoff

from config import dbConfig, switchCloudControl
from db import db

with db(**dbConfig) as DB:
    DB.create_schema('action')

    DB.session.execute("""
        CREATE TABLE IF NOT EXISTS action.status (
            status SMALLINT PRIMARY KEY
            , description TEXT
        );

        INSERT INTO action.status (status, description)
        VALUES (-1, 'Cancelled')
            , (0, 'Execution failed')
            , (1, 'Successfully executed')
        ON CONFLICT (status) DO NOTHING;

        CREATE TABLE IF NOT EXISTS action.action (
            action_id SERIAL PRIMARY KEY
            , created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            , action_time TIMESTAMP WITH TIME ZONE NOT NULL
            , device_id VARCHAR(100) NOT NULL
            , action VARCHAR(100) NOT NULL
            , actioned_at TIMESTAMP WITH TIME ZONE
            , status SMALLINT
            , CONSTRAINT fk_status FOREIGN KEY(status)
                REFERENCES action.status(status)
                ON DELETE SET NULL
                ON UPDATE CASCADE
            );
        """)
    DB.session.commit()


class action:
    DB = db(**dbConfig)
    sonoff_account = sonoff.Sonoff(switchCloudControl['username'],
                                   switchCloudControl['password'],
                                   switchCloudControl['api_region'])

    def check_multi_action(self):
        """
        If multiple commands for single device at single time, cancel all but the most recent.
        """

        self.DB.session.execute("""
            UPDATE action.action AS a
            SET status = -1
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
            SELECT action_id, action_time, device_id, action, status
            FROM action.action
            WHERE actioned_at IS NULL
                AND action_time <= CURRENT_TIMESTAMP
                AND status IS NULL
            ORDER BY action_time DESC
        """, self.DB.connection)

    def execute_todo(self):
        self.check_multi_action()

        for idx, item in self.actions.iterrows():
            try:
                self.sonoff_account.switch(item.action, item.device_id)

                # check of switch is online and has required status
                device = self.sonoff_account.get_device(item.device_id)

                assert device['online']
                assert item.action == device['params']['switch']

                item.status = 1  # success
            except:
                item.status = 0  # failed

            self.DB.session.execute(f"""
                UPDATE action.action
                SET actioned_at = CURRENT_TIMESTAMP
                    , status = {item.status}
                WHERE action_id = {item.action_id}
            """)
        self.DB.session.commit()
