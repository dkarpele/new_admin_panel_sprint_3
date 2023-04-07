from schemas import SCHEMA, TRIGGER, CHUNK_SIZE


def func_notify_trigger(func_name: str, base: str, event_name: str) -> str:
    """ Create postgres function to have notification on every
        change of `modified` field"""
    return f"""
    CREATE OR REPLACE FUNCTION {SCHEMA}.{func_name}() 
    RETURNS trigger AS $$
    DECLARE
        payload TEXT;
    BEGIN
        payload := 
        json_build_object(
            (SELECT tablename FROM pg_catalog.pg_tables
             WHERE tablename = '{base}' 
             AND schemaname='{SCHEMA}'
             ),
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


def filmwork_merger(old_modified: str, where_condition: str = '') -> str:
    """ Select all film works from database"""
    return f"""
    SELECT
    fw.id as fw_id, 
    fw.title, 
    fw.description, 
    fw.rating, 
    fw.type, 
    fw.created, 
    fw.modified, 
    pfw.role, 
    p.id, 
    p.full_name,
    g.name
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE {where_condition} 
    fw.modified > '{old_modified}'
    ORDER BY fw.modified DESC
    LIMIT {str(CHUNK_SIZE)};
    """
