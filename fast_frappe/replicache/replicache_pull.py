import datetime
import time
import frappe
from fast_frappe.replicache.replicache_push import getLatestMutationID
from fastapi.responses import JSONResponse
from fastapi import FastAPI, Depends, Request, Response, HTTPException, Depends, APIRouter
from fast_frappe.socketio import sio
from fast_frappe.ctrl import init_frappe, destroy_frappe
from db import tx, default_space_id
from pydantic import BaseModel
import json
from replicache_push import get_last_mutation_id

router = APIRouter()


class PullRequestBody(BaseModel):
    client_id: str
    last_mutation_id: int
    mutations: list


@router.post('/api/v1/pull')
async def replicache_pull(body: PullRequestBody):
    print(f"Processing pull {json.dumps(body.dict())}")
    t0 = time.time()

    try:
        # Get current version for space.
        await tx.execute("SELECT version FROM space WHERE key = %s", (default_space_id,))
        version = (await tx.fetchone())[0]

        # Get last_mutation_id for the requesting client.
        is_existing_client = body.last_mutation_id > 0
        last_mutation_id = await get_last_mutation_id(tx, body.client_id, is_existing_client)

        # Get changed domain objects since the requested version.
        from_version = body.cookie or 0
        await tx.execute("SELECT id, sender, content, ord, deleted FROM message WHERE version > %s", (from_version,))
        changed = await tx.fetchall()

        # Build and return response.
        patch = []
        for row in changed:
            id, sender, content, ord, deleted = row
            if deleted:
                if from_version > 0:
                    patch.append({
                        "op": "del",
                        "key": f"message/{id}",
                    })
            else:
                patch.append({
                    "op": "put",
                    "key": f"message/{id}",
                    "value": {
                        "from": sender,
                        "content": content,
                        "order": int(ord),
                    },
                })

        return JSONResponse({
            "lastMutationID": last_mutation_id,
            "cookie": version,
            "patch": patch,
        })
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        print(f"Processed pull in {time.time() - t0}")
