# import datetime
from collections import namedtuple
from http.client import HTTPException
# import frappe
from fastapi import APIRouter, Request
# , Response, Depends
# from fast_frappe.ctrl import init_frappe, destroy_frappe
import time
from fast_frappe.socketio import sio
from fast_frappe.replicache.db import tx, default_space_id
from pydantic import BaseModel
import json
router = APIRouter()


class Mutation(BaseModel):
    id: int
    name: str
    args: dict


class PushRequestBody(BaseModel):
    clientID: str
    mutations: list[Mutation]


@router.post("/api/v1/push")
async def replicache_push(request: Request):
    body: PushRequestBody = await request.json()
    print(f"Processing push {json.dumps(body)}")
    t0 = time.time()
    Body = namedtuple('Person', body.keys())
    body = Body(*body.values())

    try:
        for mutation in body.mutations:
            t1 = time.time()

            try:
                await process_mutation(tx, body.clientID, default_space_id, mutation)
            except Exception as e:
                print(f"Caught error from mutation {mutation} {e}")

                await process_mutation(tx, body.clientID, default_space_id, mutation, e)

            print(f"Processed mutation in {time.time() - t1}")

        sio.connect("http://localhost:9000")
        print("Send ws response")
        sio.emit(default_space_id, "poke")

        return {}
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        print(f"Processed push in {time.time() - t0}")


async def process_mutation(tx, clientID, space_id, mutation, error=None):
    prev_version_row = await tx(f"SELECT version FROM space WHERE key = {space_id} FOR UPDATE")
    prev_version = prev_version_row["version"]
    next_version = prev_version + 1

    last_mutation_id = await get_last_mutation_id(tx, clientID, False)
    next_mutation_id = last_mutation_id + 1

    print(f"nextVersion: {next_version}, nextMutationID: {next_mutation_id}")

    if mutation.id < next_mutation_id:
        print(f"Mutation {mutation.id} has already been processed - skipping")
        return

    if mutation.id > next_mutation_id:
        raise Exception(f"Mutation {mutation.id} is from the future - aborting")

    if error is None:
        print(f"Processing mutation: {json.dumps(mutation.dict())}")

        if mutation.name == "createMessage":
            await create_message(tx, mutation.args, space_id, next_version)
        else:
            raise Exception(f"Unknown mutation: {mutation.name}")
    else:
        print(f"Handling error from mutation {json.dumps(mutation.dict())} {error}")

    print(f"Setting {clientID} last_mutation_id to {next_mutation_id}")
    await set_last_mutation_id(tx, clientID, next_mutation_id)

    await tx("UPDATE space SET version = $1 WHERE key = $2", next_version, space_id)


async def get_last_mutation_id(tx, clientID, required):
    client_row = await tx.fetchrow(
        "SELECT last_mutation_id FROM replicache_client WHERE id = $1", clientID
    )

    if not client_row:
        if required:
            raise Exception(f"client not found: {clientID}")
        return 0

    return int(client_row["last_mutation_id"])


async def set_last_mutation_id(tx, clientID, mutation_id):
    result = await tx(
        "UPDATE replicache_client SET last_mutation_id = $2 WHERE id = $1",
        clientID,
        mutation_id,
    )

    if result == "UPDATE 0":
        await tx(
            "INSERT INTO replicache_client (id, last_mutation_id) VALUES ($1, $2)",
            clientID,
            mutation_id,
        )


async def create_message(tx, args, space_id, version):
    await tx(
        """
        INSERT INTO message (id, space_id, sender, content, ord, deleted, version)
        VALUES ($1, $2, $3, $4, $5, false, $6)
        """,
        args["id"],
        space_id,
        args["from"],
        args["content"],
        args["order"],
        version,
    )
