import traceback
# import datetime
import time
# import frappe
# from fast_frappe.replicache.replicache_push import getLatestMutationID
from fastapi.responses import JSONResponse
from fastapi import HTTPException, APIRouter, Request
# FastAPI, Depends, Request, Response, Depends,
# from fast_frappe.socketio import sio
# from fast_frappe.ctrl import init_frappe, destroy_frappe
from fast_frappe.replicache.db import pg_init, tx, cursor, default_space_id
from pydantic import BaseModel
import json
from collections import namedtuple
from fast_frappe.replicache.replicache_push import convert_dict_2_namedtuple, get_last_mutation_id

router = APIRouter()


# class PullRequestBody(BaseModel):
#     clientID: str
#     lastMutationID: int
#     profileID: list
#     cookie: any
#     pullVersion: int
#     schemaVersion: str


@router.post('/api/v1/reppull')
async def replicache_pull(req: Request):
    body = await req.json()
    print("body")
    print(body)
    body = convert_dict_2_namedtuple(body)
    # return body
    # body: PullRequestBody = await req.json()
    print(f"Processing pull {json.dumps(body)}")
    t0 = time.time()
    conn = await pg_init()
    try:
        # Get current version for space.
        # await tx.execute("SELECT version FROM space WHERE key = %s", (default_space_id,))
        version = await cursor(conn, f"SELECT version FROM space WHERE key = '{default_space_id}'")
        print(version)

        # Get lastMutationID for the requesting client.
        is_existing_client = body.lastMutationID > 0
        lastMutationID = await get_last_mutation_id(conn, body.clientID, is_existing_client)

        # Get changed domain objects since the requested version.
        if body.cookie is None:
            from_version = 0
        else:
            from_version = body.cookie['version']
        # await tx.execute("SELECT id, sender,  content, ord, deleted FROM message WHERE version > %s", (from_version,))
        changed = await cursor(conn, f"SELECT id, sender, content, ord, deleted FROM message WHERE version > {from_version}", fetch_number=-1)

        # Build and return response.
        patch = []
        for row in changed:
            # id, sender, content, ord, deleted = row
            if row['deleted']:
                if from_version > 0:
                    patch.append({
                        "op": "del",
                        "key": f"message/{row['id']}",
                    })
            else:
                patch.append({
                    "op": "put",
                    "key": f"message/{id}",
                    "value": {
                        "from": row['sender'],
                        "content": row['content'],
                        "order": int(row['ord']),
                    },
                })

        return JSONResponse({
            "lastMutationID": lastMutationID,
            "cookie": version,
            "patch": patch,
        })
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        print(f"Processed pull in {time.time() - t0}")
