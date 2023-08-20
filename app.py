import httpx
import asyncio
from datetime import datetime
import asyncpg
import time
from parsing_schemas import matches_schema
import logging
import os

logging.basicConfig(level=logging.DEBUG, datefmt='%d.%m %H:%M:%S')
logging.getLogger('hpack').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)

DISCIPLINES = [("csgo", "15bfd86e-0b4b-4397-bffa-61c9c82d0b66"), ("football", 2)]


def get_tournaments_data(responses):
    tournaments_data = {
        tournament['id']: [tournament['token_international']]
        for tournament in responses['tournaments']
    }
    return tournaments_data


def get_start_date(match):
    return match['date']


def get_team_1(match):
    return match['participant1']['team']['token_international']


def get_team_2(match):
    return match['participant2']['team']['token_international']


async def fetch_url(url, client):
    response = await client.get(url)
    response = response.json()
    return response


async def esportsbattle():
    async with httpx.AsyncClient(http2=True) as client:
        results = []
        for discipline in DISCIPLINES:
            url = f"https://{discipline[0]}.esportsbattle.com/api/tournaments"

            params = {"dateFrom": datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z'),
                      "dateTo": "2099-12-31",
                      "page": "1"}
            response = await client.get(url, params=params)
            response = response.json()

            tournaments_data = get_tournaments_data(response)
            tasks = [asyncio.create_task(fetch_url(
                f"https://{discipline[0]}.esportsbattle.com/api/tournaments/{tournament_id}/matches", client)
            ) for tournament_id in tournaments_data.keys()]
            responses = await asyncio.gather(*tasks)

            for response, tournament_data in zip(responses, tournaments_data.values()):
                for match in response:
                    if discipline[1] != match["status_id"]:
                        continue
                    result = matches_schema.copy()
                    result["sport_name"] = discipline[0]
                    result["tournament_name"] = tournament_data[0]
                    result["match_start"] = get_start_date(match)
                    result["team_1"] = get_team_1(match)
                    result["team_2"] = get_team_2(match)
                    results.append(result)

        logging.info(f'Parsing end up with:{len(results)} matches')
        return results


async def insert_data(conn, data_list):
    query = """
    INSERT INTO matches (sport_name, tournament, match_start, team1, team2)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (sport_name, tournament, match_start, team1, team2) DO NOTHING
    """
    formatted_data = [(data['sport_name'], data['tournament_name'], data['match_start'], data['team_1'], data['team_2'])
                      for data in data_list]
    await conn.executemany(query, formatted_data)


async def create_table(conn):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS matches (
            id SERIAL PRIMARY KEY,
            sport_name TEXT,
            tournament TEXT,
            match_start TEXT,
            team1 TEXT,
            team2 TEXT,
            CONSTRAINT unique_combination UNIQUE (sport_name, tournament, match_start, team1, team2)
        )
        """
    await conn.execute(create_table_query)


async def pg_connection(data):
    connection_params = {
        "user": os.environ.get('DB_USER'),
        "password": os.environ.get('DB_PASSWORD'),
        "database": os.environ.get('DB_NAME'),
        "host": os.environ.get('DB_HOST'),
        "port": os.environ.get('DB_PORT')
    }

    async with asyncpg.create_pool(**connection_params) as pool:
        async with pool.acquire() as conn:
            await create_table(conn)
            await insert_data(conn, data)

while True:
    data = asyncio.run(esportsbattle())
    asyncio.run(pg_connection(data))
    logging.info(f'Data inserted in Postgres')
    logging.info(f'Go to sleep for 60 seconds')
    time.sleep(float(os.environ.get('TIME_RESTART')))
