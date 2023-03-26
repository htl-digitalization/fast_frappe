# import datetime
from collections import namedtuple
from http.client import HTTPException
# import frappe
from fastapi import APIRouter, Request
# , Response, Depends
# from fast_frappe.ctrl import init_frappe, destroy_frappe
import time
from fast_frappe.socketio import sio
from fast_frappe.replicache.db import pg_init, tx, cursor, default_space_id
from pydantic import BaseModel
import json
router = APIRouter()


def convert_dict_2_namedtuple(d):
    return namedtuple('X', d.keys())(*d.values())


class Mutation(BaseModel):
    id: int
    name: str
    args: dict


class PushRequestBody(BaseModel):
    clientID: str
    mutations: list[Mutation]


@router.post("/api/v1/reppush")
async def replicache_push(request: Request):
    body: PushRequestBody = await request.json()
    print(f"Processing push {json.dumps(body)}")
    t0 = time.time()
    Body = namedtuple('Person', body.keys())
    body = Body(*body.values())
    conn = await pg_init()
    try:
        for mutation in body.mutations:
            t1 = time.time()

            try:
                await process_mutation(conn, body.clientID, default_space_id, mutation)
            except Exception as e:
                print(f"Caught error from mutation {mutation} {e}")

                await process_mutation(conn, body.clientID, default_space_id, mutation, e)

            print(f"Processed mutation in {time.time() - t1}")

        # sio.connect("http://localhost:9000")
        print("Send ws response")
        sio.emit(default_space_id, "poke")

        return 'processed push 1'
    except Exception as e:
        print(f"Error: {e}")
        # return e
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        print(f"Processed push in {time.time() - t0}")
        return 'processed push'


async def process_mutation(conn, clientID, space_id, mutation, error=None):
    prev_version_row = await cursor(conn, f"""SELECT version FROM space WHERE key='{space_id}' FOR UPDATE""")
    if (prev_version_row):
        prev_version = prev_version_row["version"]
        next_version = prev_version + 1
    else:
        next_version = 1

    last_mutation_id = await get_last_mutation_id(conn, clientID, False)
    next_mutation_id = last_mutation_id + 1
    mutation = convert_dict_2_namedtuple(mutation)
    print(f"nextVersion: {next_version}, nextMutationID: {next_mutation_id}")

    if mutation.id < next_mutation_id:
        print(f"Mutation {mutation.id} has already been processed - skipping")
        return (f"Mutation {mutation.id} has already been processed - skipping")

    if mutation.id > next_mutation_id:
        raise Exception(f"Mutation {mutation.id} is from the future - aborting")

    if error is None:
        print(f"Processing mutation: {json.dumps(mutation)}")

        if mutation.name == "createMessage":
            await create_message(conn, mutation.args, space_id, next_version)
        else:
            raise Exception(f"Unknown mutation: {mutation.name}")
    else:
        print(f"Handling error from mutation {json.dumps(mutation)} {error}")

    print(f"Setting {clientID} last_mutation_id to {next_mutation_id}")
    await set_last_mutation_id(conn, clientID, next_mutation_id)

    await tx(conn, f"UPDATE space SET version = '{next_version}' WHERE key = '{space_id}'")


async def get_last_mutation_id(conn, clientID, required):
    client_row = await cursor(
        conn,
        f"SELECT last_mutation_id FROM replicache_client WHERE id = '{clientID}'"
    )

    if not client_row:
        if required:
            raise Exception(f"client not found: {clientID}")
        return 0

    return int(client_row["last_mutation_id"])


async def set_last_mutation_id(conn, clientID, mutation_id):
    # result = await tx(f"UPDATE replicache_client SET last_mutation_id = {mutation_id} WHERE id = '{clientID}'")
    # TODO: problem here of client
    client = await cursor(conn, f"SELECT * FROM replicache_client WHERE id = '{clientID}'")
    if client is None:
        await tx(
            conn,
            f"INSERT INTO replicache_client (id, last_mutation_id) VALUES ('{clientID}', {mutation_id})"
        )
    else:
        await tx(conn, f"UPDATE replicache_client SET last_mutation_id = {mutation_id} WHERE id = '{clientID}'")


async def create_message(conn, args, space_id, version):
    await tx(conn, f"""
        INSERT INTO message (id, space_id, sender, content, ord, deleted, version)
        VALUES ('{args["id"]}',
            '{space_id}',
            '{args["from"]}',
            '{args["content"]}',
            '{args["order"]}',
            false,
            '{version}')
        """)
