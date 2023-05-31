from config import SCHEMA, TRIGGER


def payload(base: str) -> str:
    """Send updated table name as `base`,
    `id` and `modified` of the latest updated row"""
    return f"""
    json_build_object(
            '{base}',
            (SELECT json_build_object(
                    'id',
                    b.id,
                    'modified',
                    cast(b.{TRIGGER} AS TEXT)
                    )
            FROM {SCHEMA}.{base} as b
            ORDER BY {TRIGGER} DESC
            LIMIT 1
            )
        );
    """


def func_notify_trigger(func_name: str, base: str, event_name: str) -> str:
    """ Create postgres function to have notification on every
        change of `modified` field"""
    return f"""
    CREATE OR REPLACE FUNCTION {SCHEMA}.{func_name}() 
    RETURNS trigger AS $$
    DECLARE
        payload TEXT;
    BEGIN
        payload := {payload(base)}
        PERFORM pg_notify('{event_name}', payload);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    """


def trigger_insert_update(func_name: str, base: str) -> str:
    """ Trigger every time user updates a table"""
    return f"""
    CREATE OR REPLACE TRIGGER {base}_notify
    AFTER INSERT OR UPDATE ON {SCHEMA}.{base}
    FOR EACH ROW EXECUTE PROCEDURE {func_name}();
    """


def listen_event(event_name: str) -> str:
    """Listen to the event"""
    return f"LISTEN {event_name};"


def initial_notify(event_name: str, payload_: str) -> str:
    return f"""
    NOTIFY {event_name}, '{payload_}'
    """


def count_modified_rows(base: str, old_state: str) -> str:
    return f"""
    SELECT COUNT(*) 
    FROM {SCHEMA}.{base}
    WHERE 'modified' > '{old_state}';
    """


def person_producer(where_condition: str = '',
                    limit_condition: str = '') -> str:
    """ Select all persons works from database"""
    return f"""
    SELECT id, modified
    FROM {SCHEMA}.person
    {where_condition}
    ORDER BY modified
    {limit_condition};
    """


def person_enricher(where_condition: str = '') -> str:
    """ Select all film works with current persons"""
    return f"""
    SELECT fw.id, fw.modified
    FROM {SCHEMA}.film_work fw
    LEFT JOIN {SCHEMA}.person_film_work pfw ON pfw.film_work_id = fw.id
    {where_condition}
    GROUP BY fw.id
    ORDER BY fw.modified, fw.id;
    """


def genre_producer(where_condition: str = '') -> str:
    """ Select all genres works from database"""
    return f"""
    SELECT id, modified
    FROM {SCHEMA}.genre
    {where_condition}
    ORDER BY modified;
    """


def genre_enricher(where_condition: str = '') -> str:
    """ Select all film works with current genres"""
    return f"""
    SELECT fw.id, fw.modified
    FROM {SCHEMA}.film_work fw
    LEFT JOIN {SCHEMA}.genre_film_work gfw ON gfw.film_work_id = fw.id
    {where_condition}
    GROUP BY fw.id
    ORDER BY fw.modified, fw.id;
    """


def genre_merger(where_condition: str = '') -> str:
    """ Select all film works from database"""
    return f"""
    SELECT
    g.id as id_, 
    g.created, 
    g.modified, 
    g.name, 
    g.description
    FROM {SCHEMA}.genre g
    {where_condition};
    """
