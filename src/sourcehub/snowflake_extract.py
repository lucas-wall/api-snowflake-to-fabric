import re

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*){0,2}")


def incremental_query(table, watermark_col, columns):
    # Identifiers are whitelisted; values are always bound, never formatted in.
    for ident in (table, watermark_col, *columns):
        if not _IDENT.fullmatch(ident):
            raise ValueError(f"invalid identifier: {ident!r}")
    return (
        f"SELECT {', '.join(columns)} FROM {table} "
        f"WHERE {watermark_col} > %(since)s ORDER BY {watermark_col}"
    )


def extract_incremental(conn, table, watermark_col, columns, since, batch_size=10_000):
    cur = conn.cursor()
    try:
        cur.execute(incremental_query(table, watermark_col, columns), {"since": since})
        while True:
            batch = cur.fetchmany(batch_size)
            if not batch:
                return
            yield [dict(zip(columns, row)) for row in batch]
    finally:
        cur.close()


def connect_from_secrets(account, user, password, warehouse, database, schema):
    import snowflake.connector

    return snowflake.connector.connect(
        account=account, user=user, password=password,
        warehouse=warehouse, database=database, schema=schema,
    )
