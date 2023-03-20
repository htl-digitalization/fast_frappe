# import datetime
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
    client_id: str
    mutations: list[Mutation]


@router.post("/api/v1/push")
async def replicache_push(request: Request):
    body: PushRequestBody = await request.json()
    print(f"Processing push {json.dumps(body.dict())}")
    t0 = time.time()

    try:
        for mutation in body.mutations:
            t1 = time.time()

            try:
                await process_mutation(tx, body.client_id, default_space_id, mutation)
            except Exception as e:
                print(f"Caught error from mutation {mutation} {e}")

                await process_mutation(tx, body.client_id, default_space_id, mutation, e)

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


async def process_mutation(tx, client_id, space_id, mutation, error=None):
    prev_version_row = await tx.exec("SELECT version FROM space WHERE key = $1 FOR UPDATE", space_id)
    prev_version = prev_version_row["version"]
    next_version = prev_version + 1

    last_mutation_id = await get_last_mutation_id(tx, client_id, False)
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

    print(f"Setting {client_id} last_mutation_id to {next_mutation_id}")
    await set_last_mutation_id(tx, client_id, next_mutation_id)

    await tx.execute("UPDATE space SET version = $1 WHERE key = $2", next_version, space_id)


async def get_last_mutation_id(tx, client_id, required):
    client_row = await tx.fetchrow(
        "SELECT last_mutation_id FROM replicache_client WHERE id = $1", client_id
    )

    if not client_row:
        if required:
            raise Exception(f"client not found: {client_id}")
        return 0

    return int(client_row["last_mutation_id"])


async def set_last_mutation_id(tx, client_id, mutation_id):
    result = await tx.execute(
        "UPDATE replicache_client SET last_mutation_id = $2 WHERE id = $1",
        client_id,
        mutation_id,
    )

    if result == "UPDATE 0":
        await tx.execute(
            "INSERT INTO replicache_client (id, last_mutation_id) VALUES ($1, $2)",
            client_id,
            mutation_id,
        )


async def create_message(tx, args, space_id, version):
    await tx.execute(
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
