import api.orm.queries as queries
import api.orm.sql as sql


async def create_tables():
    """
    Creating tables for application
    """
    text_create_citizens_table = queries.CREATE_CITIZENS
    text_create_relative_table = queries.CREATE_RELATIVES
    text_create_consts_table = queries.CREATE_CONSTS
    text_insert_base_import_id = queries.INSERT_IMPORT_ID_0
    text_create_index_citizens = queries.CREATE_INDEX_CITIZENS
    text_create_index_relatives = queries.CREATE_INDEX_RELATIVES

    queries_list = [text_create_citizens_table, text_create_relative_table,
                    text_create_consts_table, text_insert_base_import_id,
                    text_create_index_citizens,text_create_index_relatives]

    connection = await sql.connect()
    async with connection.transaction():
        for query in queries_list:
            await sql.execute_query(connection, query)
    await sql.close(connection)

