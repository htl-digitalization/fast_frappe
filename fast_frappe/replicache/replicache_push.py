import datetime
from http.client import HTTPException
import frappe
from fastapi import Depends, APIRouter, Request, Response
from fast_frappe.ctrl import init_frappe, destroy_frappe
import time
from fast_frappe.socketio import sio
from db import tx, default_space_id
default_space_id = 'default'
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


def process_mutation(psg, clientID, spaceID, mutation, error=None):
    """
    Table space only has 2 col (key, version)
    Process a mutation from the client
    """
    # _dict = frappe.db.sql(f"""select version from tabSpace where key = '{spaceID}' for update""").as_dict()
    prev_version = frappe.db.get_value('RepSpace', filters={"key": spaceID}, fieldname='version')
    nextVersion = prev_version + 1
    lastMutationID = getLatestMutationID(psg, clientID=clientID, required=False)
    nextMutationID = int(lastMutationID) + 1

    if mutation['id'] < nextMutationID:
        raise Exception("Mutation ID has already been processed - skipping")

    if mutation['id'] > nextMutationID:
        raise Exception("Mutation ID is from the future - aborting")

    if (error is None):
        print(f'Processing mutation: {mutation}')
        if mutation['name'] == 'createMessage':
            print('processing mutation')
            createMessage('wtf is this', mutation['args'], spaceID, nextVersion)
        else:
            raise Exception(f"Unknown mutation {mutation['name']}")
    else:
        print(f'Error processing mutation: {mutation}')

    setLatestMutationID(psg, clientID, nextMutationID)
    # update version for space
#     await t.none('update space set version = $1 where key = $2', [
#     nextVersion,
#     spaceID,
#   ]);
    # frappe.db.set_value("Space", spaceID, "version", nextVersion)


async def get_last_mutation_id(t, client_id, required):
    client_row = await t.fetchrow(
        "SELECT last_mutation_id FROM replicache_client WHERE id = $1", client_id
    )

    if not client_row:
        if required:
            raise Exception(f"client not found: {client_id}")
        return 0

    return int(client_row["last_mutation_id"])


def setLatestMutationID(psg, clientID, mutationID):
    """_summary_

    Args:
        psg (_type_): _description_
        clientID (_type_): client id to mutate mutationID column in the table Replicache Client
        mutationID (_type_): new mutatation id
    """
    # TODO: direct copy of the javascript which is incorrect, this need to check if there is any row with the clientID, mutationID and update them
    # result = frappe.db.get_value("Replicache Client", client_id=clientID, latest_mutation_id=mutationID)

    result = frappe.db.get_value("RepClient", filters={'id': clientID}, fieldname=['name'])
    if result:
        result.db.set_value("RepClient", result, {"latest_mutation_id": mutationID})
        frappe.db.commit()
        # result.save()
    else:
        result = frappe.get_doc({
            "doctype": "RepClient",
            "id": clientID,
            "last_mutation_id": mutationID
        })
        print(result.as_dict())
        result.insert()
        frappe.db.commit()
        # frappe.db.insert({
        #     "doctype": "Replicache Client",
        #     "client_id": clientID,
        #     "latest_mutation_id": mutationID
        # })


def createMessage(t, _dict, spaceID, version):
    """_summary_

    Args:
        t (_type_): _description_
        _dict (_type_): _description_
        spaceID (_type_): _description_
        version (_type_): _description_
    """
    try:
        print('run create message')
        # using frappe query builder to insert into the database
        # _id, _from, content, order = _dict
        new_mess = frappe.get_doc({
            "doctype": "RepMessage",
            # "type": t,
            "id": _dict['id'],
            "sender": _dict['from'],
            "context": _dict['content'],
            "ord": _dict['order'],
            "space_id": spaceID,
            "version": version
        })
        new_mess.save()
        frappe.db.commit()
    except Exception as e:
        print(e)
    # frappe.db.insert({
    #     "doctype": "Replicache Message",
    #     "type": t,
    #     "id": _id,
    #     "from": _from,
    #     "content": content,
    #     "order": order,
    #     "space_id": spaceID,
    #     "version": version
    # })


# def sendPoke(sio):
#     '''Send a poke to the client to trigger a pull'''
#     sio.emit('poke', channel='replicache')
