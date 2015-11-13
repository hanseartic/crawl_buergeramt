from sqlite3 import Connection


service_seed = [  # anliegen
    (120703, 'Personalausweis beantragen'),
    (120686, 'Anmelden einer Wohnung'),
    (121151, 'Reisepass beantragen'),
    (121469, 'Kinderreisepass beantragen'),
    (120926, 'Führungszeugnis beantragen'),
    (120702, 'Meldebescheinigung beantragen'),
    (121627, 'Ersterteilung Führerschein'),
    (121629, 'Erweiterung Führerschein'),
    (121637, 'Neuerteilung Führerschein nach Entzug'),
    (121593, 'Ersatzführerschein nach Verlust'),
]

location_seed = [  # dienstleister
    (122210, ''), (122217, ''), (122219, ''), (122227, ''), (122231, ''), (122238, ''), (122243, ''), (122252, ''),
    (122260, ''), (122262, ''), (122254, ''), (122271, ''), (122273, ''), (122277, ''), (122280, ''), (122282, ''),
    (122284, ''), (122291, ''), (122285, ''), (122286, ''), (122296, ''), (150230, ''), (122301, ''), (122297, ''),
    (122294, ''), (122312, ''), (122314, ''), (122304, ''), (122311, ''), (122309, ''), (317869, ''), (324433, ''),
    (325341, ''), (324434, ''), (324435, ''), (122281, ''), (324414, ''), (122283, ''), (122279, ''), (122276, ''),
    (122274, ''), (122267, ''), (122246, ''), (122251, ''), (122257, ''), (122208, ''), (122226, ''),
]


def seed(sqlite_connection: Connection):
    db_cursor = sqlite_connection.cursor()
    db_cursor.execute('''CREATE TABLE IF NOT EXISTS `customers` (
    id int primary key,
    name text,
    phone text,
    mail text,
    updated int,
    cancel_token text,
    appointment text,
    confirmation blob
)''')
    db_cursor.execute("SELECT `name` FROM 'sqlite_master' WHERE type='table' AND name='services'")
    if not db_cursor.fetchone():
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS `services` (
    id int(8) primary key,
    name text
)''')
        db_cursor.executemany("INSERT INTO `services` VALUES(?,?)", service_seed)
    db_cursor.execute("SELECT `name` FROM 'sqlite_master' WHERE type='table' AND name='locations'")
    if not db_cursor.fetchone():
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS `locations` (
    id int(8) primary key,
    name text
)''')
        db_cursor.executemany("INSERT INTO `locations` VALUES(?,?)", location_seed)
    db_cursor.connection.commit()

    db_cursor.close()
    pass
